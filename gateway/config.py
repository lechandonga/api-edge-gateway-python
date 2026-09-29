"""配置模型与原子热更新。

配置以不可变快照形式整体替换：校验通过后一次性生效，
进行中的请求仍持有旧快照引用，不受切换影响。

每次成功提交（含回退）都会追加一条不可变的变更记录，
支持查询历史并回退到任意一次记录。
"""

from __future__ import annotations

import threading
import time
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
class RateLimitRule:
    """按调用方维度的限流与配额规则。"""
    caller: str                  # 调用方标识（如 API key / 客户端 ID）
    rate_per_second: float       # 每秒允许的请求数（令牌桶速率）
    burst: int                   # 突发容量
    quota: int | None = None     # 总配额，None 表示不限
    quota_period_seconds: float | None = None  # 配额周期；None 表示累计不重置


@dataclass(frozen=True)
class SplitRule:
    """路由下的分流规则：按请求头或查询参数把流量引到指定上游组。

    同一路由内规则按配置顺序求值，先命中先生效；都未命中走默认上游组。
    """
    name: str
    match_kind: str              # "header" 或 "query"
    match_key: str               # 请求头名或查询参数名
    match_value: str             # 期望的值（精确匹配）
    upstreams: tuple[Upstream, ...]


@dataclass(frozen=True)
class BreakerConfig:
    """熔断器配置。"""
    failure_threshold: int = 3        # 连续失败多少次后打开
    reset_timeout: float = 5.0        # 打开后多少秒进入半开
    half: int = 1                      # 半开状态下允许的探测请求数
    timeout_seconds: float = 2.0      # 上游请求超时


@dataclass(frozen=True)
class Route:
    """路由：按请求特征匹配并分发到一个上游组。"""
    name: str
    path_prefix: str                 # 按路径前缀匹配
    upstreams: tuple[Upstream, ...]
    strategy: LBStrategy = LBStrategy.ROUND_ROBIN
    breaker: BreakerConfig = field(default_factory=BreakerConfig)
    max_retries: int = 1             # 仅对可安全重放（幂等）的请求生效
    rules: tuple[SplitRule, ...] = ()  # 分流规则，按顺序求值


@dataclass(frozen=True)
class GatewayConfig:
    """完整网关配置快照（不可变）。"""
    routes: tuple[Route, ...]
    rate_limits: tuple[RateLimitRule, ...] = ()
    version: int = 0


def validate_config(cfg: GatewayConfig) -> None:
    """校验配置，非法或冲突时抛出带可区分原因的 GatewayError。

    - CONFIG_INVALID：字段非法（空前缀、负速率、权重<=0、URL 缺失等）
    - CONFIG_CONFLICT：结构冲突（路由名/前缀重复、限流调用方重复）
    """
    if not cfg.routes:
        raise GatewayError(ErrorCode.CONFIG_INVALID, "至少需要一个路由", 400)

    def check_upstreams(owner: str, upstreams: tuple[Upstream, ...]) -> None:
        if not upstreams:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"{owner} 至少需要一个上游实例", 400)
        up_names: set[str] = set()
        for up in upstreams:
            if not up.name or not up.base_url.startswith(("http://", "https://")):
                raise GatewayError(
                    ErrorCode.CONFIG_INVALID,
                    f"{owner} 的上游 {up.name!r} 名称或 base_url 非法", 400)
            if up.name in up_names:
                raise GatewayError(ErrorCode.CONFIG_CONFLICT,
                                   f"{owner} 内上游名重复: {up.name}", 409)
            up_names.add(up.name)
            if up.weight <= 0:
                raise GatewayError(ErrorCode.CONFIG_INVALID,
                                   f"上游 {up.name} 权重必须为正整数", 400)

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

        check_upstreams(f"路由 {route.name}", route.upstreams)
        if route.max_retries < 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 的 max_retries 不能为负", 400)
        b = route.breaker
        if b.failure_threshold <= 0 or b.reset_timeout <= 0 or b.timeout_seconds <= 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 的熔断参数必须为正", 400)

        # 分流规则：条件必须确定、互不重复，否则任何排序都无法消歧
        rule_names: set[str] = set()
        seen_conditions: set[tuple[str, str, str]] = set()
        for rule in route.rules:
            if not rule.name:
                raise GatewayError(ErrorCode.CONFIG_INVALID,
                                   f"路由 {route.name} 的分流规则名不能为空", 400)
            if rule.name in rule_names:
                raise GatewayError(
                    ErrorCode.CONFIG_CONFLICT,
                    f"路由 {route.name} 的分流规则名重复: {rule.name}", 409)
            rule_names.add(rule.name)
            if rule.match_kind not in ("header", "query"):
                raise GatewayError(
                    ErrorCode.CONFIG_INVALID,
                    f"分流规则 {rule.name} 的 match 类型必须是 header 或 query", 400)
            if not rule.match_key or not rule.match_value:
                raise GatewayError(
                    ErrorCode.CONFIG_INVALID,
                    f"分流规则 {rule.name} 的匹配键与值不能为空", 400)
            cond = (rule.match_kind, rule.match_key.lower(), rule.match_value)
            if cond in seen_conditions:
                raise GatewayError(
                    ErrorCode.CONFIG_CONFLICT,
                    f"路由 {route.name} 的分流匹配条件重复: "
                    f"{rule.match_kind} {rule.match_key}={rule.match_value}", 409)
            seen_conditions.add(cond)
            check_upstreams(f"分流规则 {rule.name}", rule.upstreams)

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
        if rule.quota_period_seconds is not None and rule.quota_period_seconds <= 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"调用方 {rule.caller} 的配额周期必须为正", 400)


@dataclass(frozen=True)
class ConfigRevision:
    """一次成功提交的变更记录（不可变）。"""
    revision: int                  # 单调递增的内部修订号
    timestamp: float               # 提交时间（time.time()）
    config: GatewayConfig
    rolled_back_to: int | None = None  # 若是回退产生，指向来源修订号


class ConfigStore:
    """线程安全的配置持有者：读取返回当前快照，更新整体原子替换。

    每次成功变更（提交或回退）追加一条 ConfigRevision 记录，
    历史只增不改，可回退到任意一条记录。
    """

    def __init__(self, initial: GatewayConfig) -> None:
        validate_config(initial)
        self._lock = threading.Lock()
        self._current = initial
        self._history: list[ConfigRevision] = [
            ConfigRevision(1, time.time(), initial)]
        self._next_revision = 2

    def get(self) -> GatewayConfig:
        return self._current

    def current_revision(self) -> int:
        return self._history[-1].revision

    def update(self, cfg: GatewayConfig) -> ConfigRevision:
        """校验并原子替换；失败时旧配置保持不变。"""
        validate_config(cfg)  # 校验失败直接抛出，不触碰旧配置
        with self._lock:
            rev = ConfigRevision(self._next_revision, time.time(), cfg)
            self._next_revision += 1
            self._current = cfg
            self._history.append(rev)
        return rev

    def history(self, limit: int = 20) -> list[ConfigRevision]:
        """最近的变更记录，新的在前。"""
        with self._lock:
            return list(reversed(self._history[-limit:]))

    def rollback(self, revision: int) -> ConfigRevision:
        """回退到指定修订号的配置；目标记录作为新的当前配置立即生效。

        历史不被修改：回退本身也追加一条记录（rolled_back_to 指向来源）。
        修订号不存在时抛错，当前配置保持不变。
        """
        with self._lock:
            target = next((r for r in self._history if r.revision == revision),
                          None)
            if target is None:
                raise GatewayError(ErrorCode.REVISION_NOT_FOUND,
                                   f"变更记录不存在: revision={revision}", 404)
            rev = ConfigRevision(self._next_revision, time.time(),
                                 target.config, rolled_back_to=revision)
            self._next_revision += 1
            self._current = target.config
            self._history.append(rev)
        return rev


def config_from_dict(data: dict) -> GatewayConfig:
    """从字典（如 JSON 请求体）构建配置；字段非法抛 CONFIG_INVALID。"""
    try:
        routes = []
        for r in data["routes"]:
            b = r.get("breaker", {})
            breaker = BreakerConfig(
                failure_threshold=int(b.get("failure_threshold", 3)),
                reset_timeout=float(b.get("reset_timeout", 5.0)),
                half=int(b.get("half", 1)),
                timeout_seconds=float(b.get("timeout_seconds", 2.0)),
            )
            ups = tuple(Upstream(name=u["name"], base_url=u["base_url"],
                                 weight=int(u.get("weight", 1)))
                        for u in r["upstreams"])
            rules = []
            for sr in r.get("rules", []):
                m = sr.get("match", {})
                kind = "header" if "header" in m else "query"
                key = m.get(kind)
                if key is None:
                    raise KeyError("match.header 或 match.query")
                rule_ups = tuple(
                    Upstream(name=u["name"], base_url=u["base_url"],
                             weight=int(u.get("weight", 1)))
                    for u in sr["upstreams"])
                rules.append(SplitRule(
                    name=sr["name"], match_kind=kind, match_key=str(key),
                    match_value=str(m.get("value", "")),
                    upstreams=rule_ups))
            routes.append(Route(
                name=r["name"], path_prefix=r["path_prefix"], upstreams=ups,
                strategy=LBStrategy(r.get("strategy", "round_robin")),
                breaker=breaker, max_retries=int(r.get("max_retries", 1)),
                rules=tuple(rules)))
        rules = tuple(RateLimitRule(
            caller=rl["caller"],
            rate_per_second=float(rl["rate_per_second"]),
            burst=int(rl["burst"]),
            quota=(None if rl.get("quota") is None else int(rl["quota"])),
            quota_period_seconds=(
                None if rl.get("quota_period_seconds") is None
                else float(rl["quota_period_seconds"])),
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
            "upstreams": [{"name": u.name, "base_url": u.base_url,
                           "weight": u.weight} for u in r.upstreams],
            "rules": [{
                "name": sr.name,
                "match": {sr.match_kind: sr.match_key, "value": sr.match_value},
                "upstreams": [{"name": u.name, "base_url": u.base_url,
                               "weight": u.weight} for u in sr.upstreams],
            } for sr in r.rules],
            "breaker": {
                "failure_threshold": r.breaker.failure_threshold,
                "reset_timeout": r.breaker.reset_timeout,
                "half": r.breaker.half,
                "timeout_seconds": r.breaker.timeout_seconds,
            },
        } for r in cfg.routes],
        "rate_limits": [{
            "caller": rl.caller,
            "rate_per_second": rl.rate_per_second,
            "burst": rl.burst,
            "quota": rl.quota,
            "quota_period_seconds": rl.quota_period_seconds,
        } for rl in cfg.rate_limits],
    }
