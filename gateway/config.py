"""配置模型与原子热更新。

配置以不可变快照形式整体替换：校验通过后一次性生效，
进行中的请求仍持有旧快照引用，不受切换影响。
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
class RateLimitRule:
    """按调用方维度的限流与配额规则。"""
    caller: str                  # 调用方标识（如 API key / 客户端 ID）
    rate_per_second: float       # 每秒允许的请求数（令牌桶速率）
    burst: int                   # 突发容量
    quota: int | None = None     # 总配额，None 表示不限


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

        if not route.upstreams:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 至少需要一个上游实例", 400)
        up_names: set[str] = set()
        for up in route.upstreams:
            if not up.name or not up.base_url.startswith(("http://", "https://")):
                raise GatewayError(
                    ErrorCode.CONFIG_INVALID,
                    f"路由 {route.name} 的上游 {up.name!r} 名称或 base_url 非法", 400)
            if up.name in up_names:
                raise GatewayError(ErrorCode.CONFIG_CONFLICT,
                                   f"路由 {route.name} 内上游名重复: {up.name}", 409)
            up_names.add(up.name)
            if up.weight <= 0:
                raise GatewayError(ErrorCode.CONFIG_INVALID,
                                   f"上游 {up.name} 权重必须为正整数", 400)
        if route.max_retries < 0:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"路由 {route.name} 的 max_retries 不能为负", 400)
        b = route.breaker
        if b.failure_threshold <= 0 or b.reset_timeout <= 0 or b.timeout_seconds <= 0:
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
