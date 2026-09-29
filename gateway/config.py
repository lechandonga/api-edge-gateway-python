"""配置模型、规则冲突校验、原子热更新与变更历史（可回退）。

配置以不可变快照形式整体替换：校验通过后一次性生效，
进行中的请求仍持有旧快照引用，不受切换影响。

路由内灰度分流：
- 一条路由可声明多个具名后端组 ``backend_groups`` 与多条分流规则
  ``rules``；规则按请求头/查询参数精确匹配，命中则转发到指定组；
- 多条规则的优先级由匹配条件本身决定（条件更具体者优先），
  与声明顺序无关，因此任何声明顺序下分流结果都确定；
- 条件重复、或两组条件互相都不能判定更具体（同一维度各匹配不同值）
  的配置在提交时以 ``config_conflict`` 拒绝，并指出冲突的规则。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

from .errors import ErrorCode, GatewayError


class LBStrategy(str, Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"


@dataclass(frozen=True)
class Upstream:
    """单个上游实例。"""
    name: str
    base_url: str
    weight: int = 1


@dataclass(frozen=True)
class MatchCondition:
    """单个匹配条件：请求头或查询参数等于某个值（精确匹配）。

    多个 ``values`` 之间是“或”关系（命中任一值即可）；
    同一条规则内的多个条件之间是“与”关系（需同时满足）。
    """
    kind: str                 # "header" / "query"
    name: str                 # 头名（匹配时大小写不敏感）或查询参数名
    values: tuple[str, ...]

    @property
    def key(self) -> tuple[str, str]:
        return (self.kind,
                self.name.lower() if self.kind == "header" else self.name)

    def satisfied(self, headers: dict[str, str],
                  query: dict[str, list[str]]) -> bool:
        if self.kind == "header":
            lowered = {k.lower(): v for k, v in headers.items()}
            actual = lowered.get(self.name.lower())
            return actual is not None and actual in self.values
        return any(v in self.values for v in query.get(self.name, ()))


@dataclass(frozen=True)
class BackendGroup:
    """路由内的一个具名上游组，可独立配置负载均衡与重试。"""
    name: str
    upstreams: tuple[Upstream, ...]
    strategy: LBStrategy = LBStrategy.ROUND_ROBIN
    max_retries: int = 1


@dataclass(frozen=True)
class SplitRule:
    """一条灰度分流规则：满足全部条件时转发到 ``group``。"""
    name: str
    conditions: tuple[MatchCondition, ...]
    group: str

    def matches(self, headers: dict[str, str],
                query: dict[str, list[str]]) -> bool:
        return all(c.satisfied(headers, query) for c in self.conditions)


@dataclass(frozen=True)
class RateLimitRule:
    """按调用方维度的限流与配额规则。"""
    caller: str                  # 调用方标识（如 API key / 客户端 ID）
    rate_per_second: float       # 每秒允许的请求数（令牌桶速率）
    burst: int                   # 突发容量
    quota: int | None = None     # 周期内配额，None 表示不限
    quota_window_seconds: float | None = None  # 配额重置周期，None 不周期重置


@dataclass(frozen=True)
class BreakerConfig:
    """熔断器配置。"""
    failure_threshold: int = 3        # 连续失败多少次后打开
    reset_timeout: float = 5.0        # 打开后多少秒进入半开
    half: int = 1                     # 半开状态下允许的探测请求数
    timeout_seconds: float = 2.0      # 上游请求超时


@dataclass(frozen=True)
class Route:
    """路由：按路径前缀匹配，内部按规则灰度分流到后端组。"""
    name: str
    path_prefix: str                          # 按路径前缀匹配
    upstreams: tuple[Upstream, ...] = ()      # 默认组（兼容老配置）
    strategy: LBStrategy = LBStrategy.ROUND_ROBIN
    breaker: BreakerConfig = field(default_factory=BreakerConfig)
    max_retries: int = 1                      # 仅对幂等请求生效
    backend_groups: tuple[BackendGroup, ...] = ()
    rules: tuple[SplitRule, ...] = ()
    default_group: str = "default"

    def groups(self) -> dict[str, BackendGroup]:
        """路由内全部后端组（默认组 + 具名组）。"""
        groups = {self.default_group: BackendGroup(
            self.default_group, self.upstreams, self.strategy, self.max_retries)}
        for group in self.backend_groups:
            groups[group.name] = group
        return groups

    def ordered_rules(self) -> list[SplitRule]:
        """按条件具体度从高到低返回规则；具体度由条件本身决定。

        与声明顺序无关：维度更多、取值更窄的规则总是排在前面。
        """
        return sorted(self.rules, key=_rule_sort_key, reverse=True)

    def match_group(self, headers: dict[str, str],
                    query: dict[str, list[str]]) -> str:
        """返回请求命中的后端组名；无规则命中时返回默认组。"""
        for rule in self.ordered_rules():
            if rule.matches(headers, query):
                return rule.group
        return self.default_group


def _condition_sort_key(cond: MatchCondition) -> tuple:
    return (cond.kind, cond.key[1], tuple(sorted(cond.values)))


def _rule_sort_key(rule: SplitRule) -> tuple:
    """规则全序键：维度数多者优先，其次按条件具体内容排序。"""
    dims = tuple(sorted({c.key for c in rule.conditions}))
    conds = tuple(sorted(_condition_sort_key(c) for c in rule.conditions))
    return (len(dims), dims, conds, rule.name)


def _condition_covered(cond: MatchCondition, other: MatchCondition) -> bool:
    """other 是否覆盖 cond：同维度且 other 取值集合包含 cond 取值集合。"""
    if cond.key != other.key:
        return False
    return set(cond.values).issubset(set(other.values))


def _rule_relation(a: SplitRule, b: SplitRule) -> str:
    """两条规则的具体度关系。

    返回 ``equal`` / ``a_more_specific`` / ``b_more_specific`` /
    ``incomparable``：
    - 维度相同、每个条件取值集合一致：equal（条件重复）；
    - 一方每个条件都被另一方同维度条件覆盖，且至少一处更窄或维度更多：
      另一方更具体；
    - 同维度匹配互不包含的值（如 header=x 与 header=y），双方都
      不构成覆盖：incomparable（怎么排都分不出先后）。
    """
    a_conds = {c.key: c for c in a.conditions}
    b_conds = {c.key: c for c in b.conditions}
    a_covers_b = all(k in a_conds and _condition_covered(bc, a_conds[k])
                     for k, bc in b_conds.items())
    b_covers_a = all(k in b_conds and _condition_covered(ac, b_conds[k])
                     for k, ac in a_conds.items())
    if a_covers_b and b_covers_a:
        return "equal"
    a_narrower = (set(a_conds) > set(b_conds)
                  or any(set(a_conds[k].values) != set(b_conds[k].values)
                         for k in b_conds if k in a_conds))
    b_narrower = (set(b_conds) > set(a_conds)
                  or any(set(b_conds[k].values) != set(a_conds[k].values)
                         for k in a_conds if k in b_conds))
    if a_covers_b and a_narrower:
        return "a_more_specific"
    if b_covers_a and b_narrower:
        return "b_more_specific"
    return "incomparable"


def _rules_can_co_occur(a: SplitRule, b: SplitRule) -> tuple[bool, tuple]:
    """两条规则是否可能被同一请求同时命中。

    仅当它们在每个共享维度上的取值集合都有交集时才可能同时命中；
    返回 (是否可能, 共享但取值互不相交的维度)。
    """
    a_conds = {c.key: c for c in a.conditions}
    b_conds = {c.key: c for c in b.conditions}
    disjoint_dims = tuple(sorted(
        key for key in (set(a_conds) & set(b_conds))
        if set(a_conds[key].values).isdisjoint(set(b_conds[key].values))))
    return (len(disjoint_dims) == 0, disjoint_dims)


@dataclass(frozen=True)
class GatewayConfig:
    """完整网关配置快照（不可变）。"""
    routes: tuple[Route, ...]
    rate_limits: tuple[RateLimitRule, ...] = ()
    version: int = 0


def _validate_upstreams(scope: str,
                        upstreams: tuple[Upstream, ...]) -> None:
    if not upstreams:
        raise GatewayError(ErrorCode.CONFIG_INVALID,
                           f"{scope} 至少需要一个上游实例", 400)
    up_names: set[str] = set()
    for up in upstreams:
        if not up.name or not up.base_url.startswith(("http://", "https://")):
            raise GatewayError(
                ErrorCode.CONFIG_INVALID,
                f"{scope} 的上游 {up.name!r} 名称或 base_url 非法", 400)
        if up.name in up_names:
            raise GatewayError(ErrorCode.CONFIG_CONFLICT,
                               f"{scope} 内上游名重复: {up.name}", 409)
        up_names.add(up.name)
        if up.weight <= 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"上游 {up.name} 权重必须为正整数", 400)


def _validate_route_rules(route: Route) -> None:
    """校验后端组引用与分流规则的命名、条件及优先级冲突。"""
    groups = route.groups()

    group_names: set[str] = set()
    for group in route.backend_groups:
        if not group.name:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 存在未命名后端组", 400)
        if group.name == route.default_group:
            raise GatewayError(
                ErrorCode.CONFIG_CONFLICT,
                f"路由 {route.name} 的后端组名 {group.name!r} 是默认组保留名",
                409)
        if group.name in group_names:
            raise GatewayError(ErrorCode.CONFIG_CONFLICT,
                               f"路由 {route.name} 内后端组名重复: {group.name}",
                               409)
        group_names.add(group.name)
        _validate_upstreams(f"路由 {route.name} 后端组 {group.name}",
                            group.upstreams)
        if group.max_retries < 0:
            raise GatewayError(
                ErrorCode.CONFIG_INVALID,
                f"路由 {route.name} 后端组 {group.name} 的 max_retries 不能为负",
                400)

    rule_names: set[str] = set()
    for rule in route.rules:
        if not rule.name:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 存在未命名的分流规则", 400)
        if rule.name in rule_names:
            raise GatewayError(
                ErrorCode.CONFIG_CONFLICT,
                f"路由 {route.name} 内分流规则名重复: {rule.name}", 409)
        rule_names.add(rule.name)
        if not rule.conditions:
            raise GatewayError(
                ErrorCode.CONFIG_INVALID,
                f"路由 {route.name} 的规则 {rule.name} 至少需要一个匹配条件", 400)
        if rule.group not in groups:
            raise GatewayError(
                ErrorCode.CONFIG_INVALID,
                f"路由 {route.name} 的规则 {rule.name} 引用了不存在的后端组 "
                f"{rule.group!r}（可用组: {sorted(groups)}）", 400)
        dims: set[tuple[str, str]] = set()
        for cond in rule.conditions:
            if cond.kind not in ("header", "query"):
                raise GatewayError(
                    ErrorCode.CONFIG_INVALID,
                    f"路由 {route.name} 规则 {rule.name} 的匹配类型非法: "
                    f"{cond.kind!r}，仅支持 header/query", 400)
            if not cond.name:
                raise GatewayError(
                    ErrorCode.CONFIG_INVALID,
                    f"路由 {route.name} 规则 {rule.name} 的匹配名不能为空", 400)
            if not cond.values or any(v == "" for v in cond.values):
                raise GatewayError(
                    ErrorCode.CONFIG_INVALID,
                    f"路由 {route.name} 规则 {rule.name} 的匹配值不能为空", 400)
            if cond.key in dims:
                raise GatewayError(
                    ErrorCode.CONFIG_CONFLICT,
                    f"路由 {route.name} 规则 {rule.name} 内同一维度出现多个条件: "
                    f"{cond.kind} {cond.name!r}，请合并为一条条件的多值列表",
                    409)
            dims.add(cond.key)

    # 两两判定具体度：条件完全重复、或互不分先后（incomparable）都拒绝，
    # 错误信息指出具体是哪两条规则、哪个维度的问题。
    for i, first in enumerate(route.rules):
        for second in route.rules[i + 1:]:
            relation = _rule_relation(first, second)
            if relation == "equal":
                raise GatewayError(
                    ErrorCode.CONFIG_CONFLICT,
                    f"路由 {route.name} 的分流规则 {first.name!r} 与 "
                    f"{second.name!r} 匹配条件完全重复", 409)
            if relation == "incomparable":
                # 只有“同一请求能同时命中两者”且落向不同组时，先后顺序
                # 才会改变分流结果；同组或取值互斥（请求集不相交）都无歧义。
                can_co_occur, disjoint_dims = _rules_can_co_occur(first, second)
                if first.group == second.group or not can_co_occur:
                    continue
                overlap = sorted({c.key for c in first.conditions}
                                 & {c.key for c in second.conditions})
                if overlap:
                    dim_text = "、".join(f"{kind} {name!r}"
                                         for kind, name in overlap)
                    detail = (f"同时涉及维度 {dim_text} 且条件互不包含，"
                              f"存在请求可同时命中二者")
                else:
                    dims_a = "、".join(f"{c.kind} {c.name!r}"
                                       for c in first.conditions)
                    dims_b = "、".join(f"{c.kind} {c.name!r}"
                                       for c in second.conditions)
                    detail = (f"匹配维度互不相交（{first.name!r}: {dims_a}；"
                              f"{second.name!r}: {dims_b}），请求可同时命中二者")
                raise GatewayError(
                    ErrorCode.CONFIG_CONFLICT,
                    f"路由 {route.name} 的分流规则 {first.name!r} 与 "
                    f"{second.name!r} {detail} 且目标组不同（"
                    f"{first.group!r} vs {second.group!r}），分流结果取决于"
                    f"声明顺序；请收窄为包含关系、合并取值或增加区分条件",
                    409)


def validate_config(cfg: GatewayConfig) -> None:
    """校验配置，非法或冲突时抛出带可区分原因的 GatewayError。

    - CONFIG_INVALID：字段非法（空前缀、负速率、权重<=0、URL 缺失等）
    - CONFIG_CONFLICT：结构冲突（路由名/前缀重复、分流规则重复或
      优先级不可判定、限流调用方重复等）
    """
    if not cfg.routes:
        raise GatewayError(ErrorCode.CONFIG_INVALID, "至少需要一个路由", 400)

    seen_names: set[str] = set()
    seen_prefixes: list[str] = []
    for route in cfg.routes:
        if not route.name:
            raise GatewayError(ErrorCode.CONFIG_INVALID, "路由名不能为空", 400)
        if route.name in seen_names:
            raise GatewayError(ErrorCode.CONFIG_CONFLICT,
                               f"路由名重复: {route.name}", 409)
        seen_names.add(route.name)

        if not route.path_prefix.startswith("/"):
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 的 path_prefix 必须以 / 开头", 400)
        # 前缀互相包含会导致匹配歧义，视为冲突
        for p in seen_prefixes:
            if route.path_prefix.startswith(p) or p.startswith(route.path_prefix):
                raise GatewayError(
                    ErrorCode.CONFIG_CONFLICT,
                    f"路由前缀冲突: {route.path_prefix!r} 与 {p!r} 互相包含", 409)
        seen_prefixes.append(route.path_prefix)

        _validate_upstreams(f"路由 {route.name}", route.upstreams)
        _validate_route_rules(route)
        if route.max_retries < 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 的 max_retries 不能为负", 400)
        b = route.breaker
        if (b.failure_threshold <= 0 or b.reset_timeout <= 0
                or b.timeout_seconds <= 0 or b.half <= 0):
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 的熔断参数必须为正", 400)

    seen_callers: set[str] = set()
    for rule in cfg.rate_limits:
        if not rule.caller:
            raise GatewayError(ErrorCode.CONFIG_INVALID, "限流规则的 caller 不能为空", 400)
        if rule.caller in seen_callers:
            raise GatewayError(ErrorCode.CONFIG_CONFLICT,
                               f"限流调用方重复: {rule.caller}", 409)
        seen_callers.add(rule.caller)
        if rule.rate_per_second <= 0 or rule.burst <= 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"调用方 {rule.caller} 的速率与突发必须为正", 400)
        if rule.quota is not None and rule.quota < 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"调用方 {rule.caller} 的配额不能为负", 400)
        if rule.quota_window_seconds is not None and rule.quota_window_seconds <= 0:
            raise GatewayError(
                ErrorCode.CONFIG_INVALID,
                f"调用方 {rule.caller} 的 quota_window_seconds 必须为正", 400)


@dataclass(frozen=True)
class ConfigRevision:
    """一次成功的配置变更记录（提交或回退各产生一条）。"""
    revision: int
    config: GatewayConfig
    created_at: float                 # UNIX 时间戳（秒）
    source: str                       # "submit" / "rollback"
    note: str                         # 回退时记录来源 revision 等说明


class ConfigHistory:
    """线程安全的配置变更历史，支持查询与按 revision 取快照。"""

    def __init__(self, first: ConfigRevision, max_entries: int = 50) -> None:
        self._lock = threading.Lock()
        self._items: list[ConfigRevision] = [first]
        self._max = max_entries

    def append(self, item: ConfigRevision) -> None:
        with self._lock:
            self._items.append(item)
            if len(self._items) > self._max:
                # 保留最早一条（启动基线），裁剪其余最旧记录
                del self._items[1:1 + len(self._items) - self._max]

    def latest(self) -> ConfigRevision:
        with self._lock:
            return self._items[-1]

    def list(self, limit: int | None = None) -> list[ConfigRevision]:
        with self._lock:
            items = list(self._items)
        if limit is not None:
            items = items[-limit:]
        return list(reversed(items))   # 最近的在前

    def get(self, revision: int) -> ConfigRevision | None:
        with self._lock:
            for item in self._items:
                if item.revision == revision:
                    return item
        return None


class ConfigStore:
    """线程安全的配置持有者：读取返回当前快照，更新整体原子替换。"""

    def __init__(self, initial: GatewayConfig) -> None:
        validate_config(initial)
        self._lock = threading.Lock()
        self._current = initial

    def get(self) -> GatewayConfig:
        return self._current

    def update(self, cfg: GatewayConfig) -> GatewayConfig:
        """校验并原子替换；失败时旧配置保持不变。"""
        validate_config(cfg)  # 校验失败直接抛出，不触碰旧配置
        with self._lock:
            self._current = cfg
        return cfg


def _upstream_from_dict(u: dict) -> Upstream:
    return Upstream(name=u["name"], base_url=u["base_url"],
                    weight=int(u.get("weight", 1)))


def _upstream_to_dict(up: Upstream) -> dict:
    return {"name": up.name, "base_url": up.base_url, "weight": up.weight}


def _breaker_from_dict(b: dict) -> BreakerConfig:
    return BreakerConfig(
        failure_threshold=int(b.get("failure_threshold", 3)),
        reset_timeout=float(b.get("reset_timeout", 5.0)),
        half=int(b.get("half", 1)),
        timeout_seconds=float(b.get("timeout_seconds", 2.0)))


def _breaker_to_dict(b: BreakerConfig) -> dict:
    return {
        "failure_threshold": b.failure_threshold,
        "reset_timeout": b.reset_timeout,
        "half": b.half,
        "timeout_seconds": b.timeout_seconds,
    }


def _condition_from_dict(c: dict) -> MatchCondition:
    raw_values = c.get("values")
    if raw_values is None:
        if "value" not in c:
            raise KeyError("values")
        raw_values = [c["value"]]
    if not isinstance(raw_values, (list, tuple)):
        raw_values = [raw_values]
    return MatchCondition(kind=c["kind"], name=c["name"],
                          values=tuple(str(v) for v in raw_values))


def _condition_to_dict(c: MatchCondition) -> dict:
    return {"kind": c.kind, "name": c.name, "values": list(c.values)}


def _group_from_dict(g: dict) -> BackendGroup:
    return BackendGroup(
        name=g["name"],
        upstreams=tuple(_upstream_from_dict(u) for u in g["upstreams"]),
        strategy=LBStrategy(g.get("strategy", "round_robin")),
        max_retries=int(g.get("max_retries", 1)))


def _group_to_dict(g: BackendGroup) -> dict:
    return {
        "name": g.name,
        "strategy": g.strategy.value,
        "max_retries": g.max_retries,
        "upstreams": [_upstream_to_dict(u) for u in g.upstreams],
    }


def config_from_dict(data: dict) -> GatewayConfig:
    """从字典（如 JSON 请求体）构建配置；字段非法抛 CONFIG_INVALID。

    兼容老配置：不带 ``backend_groups`` / ``rules`` / ``default_group`` /
    ``quota_window_seconds`` 的配置按原语义解析（顶层 upstreams 即默认组）。
    """
    try:
        routes = []
        for r in data["routes"]:
            breaker = _breaker_from_dict(r.get("breaker", {}))
            default_upstreams = tuple(_upstream_from_dict(u)
                                      for u in r.get("upstreams", []))
            groups = tuple(_group_from_dict(g)
                           for g in r.get("backend_groups", []))
            rules = tuple(SplitRule(
                name=rl["name"],
                conditions=tuple(_condition_from_dict(c)
                                 for c in rl.get("conditions", [])),
                group=rl["group"],
            ) for rl in r.get("rules", []))
            routes.append(Route(
                name=r["name"],
                path_prefix=r["path_prefix"],
                upstreams=default_upstreams,
                strategy=LBStrategy(r.get("strategy", "round_robin")),
                breaker=breaker,
                max_retries=int(r.get("max_retries", 1)),
                backend_groups=groups,
                rules=rules,
                default_group=str(r.get("default_group", "default")),
            ))
        rules = tuple(RateLimitRule(
            caller=rl["caller"],
            rate_per_second=float(rl["rate_per_second"]),
            burst=int(rl["burst"]),
            quota=(None if rl.get("quota") is None else int(rl["quota"])),
            quota_window_seconds=(
                None if rl.get("quota_window_seconds") is None
                else float(rl["quota_window_seconds"])),
        ) for rl in data.get("rate_limits", []))
        return GatewayConfig(routes=tuple(routes), rate_limits=rules,
                             version=int(data.get("version", 0)))
    except (KeyError, TypeError, ValueError) as exc:
        raise GatewayError(ErrorCode.CONFIG_INVALID,
                           f"配置字段非法: {exc}", 400) from exc


def config_to_dict(cfg: GatewayConfig) -> dict:
    """把配置快照序列化为字典。"""
    return {
        "version": cfg.version,
        "routes": [{
            "name": r.name,
            "path_prefix": r.path_prefix,
            "strategy": r.strategy.value,
            "max_retries": r.max_retries,
            "default_group": r.default_group,
            "upstreams": [_upstream_to_dict(u) for u in r.upstreams],
            "backend_groups": [_group_to_dict(g) for g in r.backend_groups],
            "rules": [{
                "name": rl.name,
                "group": rl.group,
                "conditions": [_condition_to_dict(c) for c in rl.conditions],
            } for rl in r.rules],
            "breaker": _breaker_to_dict(r.breaker),
        } for r in cfg.routes],
        "rate_limits": [{
            "caller": rl.caller,
            "rate_per_second": rl.rate_per_second,
            "burst": rl.burst,
            "quota": rl.quota,
            "quota_window_seconds": rl.quota_window_seconds,
        } for rl in cfg.rate_limits],
    }


def config_fingerprint(cfg: GatewayConfig) -> str:
    """配置内容指纹（不含版本号）。

    引擎按内容指纹缓存：回退到历史版本时，若内容与曾经生效过的
    配置一致（如在两个版本间来回切），直接复用同一批引擎与熔断状态。
    """
    import hashlib
    import json
    data = config_to_dict(cfg)
    data.pop("version", None)
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False,
                     separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
