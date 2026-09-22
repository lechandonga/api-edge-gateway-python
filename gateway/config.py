"""配置模型与原子热更新的配置存储。"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class LBStrategy(str, Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"


class UpstreamConfig(BaseModel):
    name: str
    base_url: str
    weight: int = 1


class RouteMatch(BaseModel):
    path_prefix: str = "/"
    methods: list[str] = Field(default_factory=list)  # 空 = 全部
    headers: dict[str, str] = Field(default_factory=dict)


class RouteConfig(BaseModel):
    name: str
    match: RouteMatch
    upstreams: list[UpstreamConfig]
    strategy: LBStrategy = LBStrategy.ROUND_ROBIN
    timeout_ms: int = 3000
    max_retries: int = 1


class RateLimitConfig(BaseModel):
    caller: str
    rate_per_second: float
    burst: int
    quota: Optional[int] = None  # 配额窗口内允许的总次数，None = 不限
    quota_window_seconds: float = 60.0


class CircuitBreakerConfig(BaseModel):
    failure_threshold: int = 3
    open_seconds: float = 5.0
    half_open_max_probes: int = 1


class GatewayConfig(BaseModel):
    routes: list[RouteConfig] = Field(default_factory=list)
    rate_limits: list[RateLimitConfig] = Field(default_factory=list)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)


class ConfigError(Exception):
    """配置非法/冲突，reason 字段给出可区分原因。"""

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}")


class ConfigStore:
    """持有当前配置快照；更新时整体校验并原子替换，进行中的请求继续使用旧快照。"""

    def __init__(self, initial: GatewayConfig | None = None):
        self._lock = threading.Lock()
        self._snapshot: GatewayConfig = initial or GatewayConfig()

    def snapshot(self) -> GatewayConfig:
        # GatewayConfig 在运行期被当作不可变快照使用：
        # 更新只通过 update() 整体替换引用，进行中的请求持有的旧快照不受影响。
        with self._lock:
            return self._snapshot

    def update(self, new_config: GatewayConfig) -> None:
        self.validate(new_config)  # 先校验，非法配置绝不生效
        with self._lock:
            self._snapshot = new_config  # 原子替换引用

    @staticmethod
    def validate(cfg: GatewayConfig) -> None:
        route_names = [r.name for r in cfg.routes]
        if len(route_names) != len(set(route_names)):
            raise ConfigError("duplicate_route_name",
                              f"路由名重复: {sorted({n for n in route_names if route_names.count(n) > 1})}")
        for route in cfg.routes:
            if not route.match.path_prefix.startswith("/"):
                raise ConfigError("invalid_path_prefix",
                                  f"路由 {route.name} 的 path_prefix 必须以 / 开头")
            if not route.upstreams:
                raise ConfigError("empty_upstreams",
                                  f"路由 {route.name} 至少需要一个上游")
            upstream_names = [u.name for u in route.upstreams]
            if len(upstream_names) != len(set(upstream_names)):
                raise ConfigError("duplicate_upstream_name",
                                  f"路由 {route.name} 内上游名重复")
            for u in route.upstreams:
                if not u.base_url.startswith(("http://", "https://")):
                    raise ConfigError("invalid_upstream_url",
                                      f"上游 {u.name} 的 base_url 非法: {u.base_url}")
                if u.weight < 1:
                    raise ConfigError("invalid_weight",
                                      f"上游 {u.name} 的 weight 必须 >= 1")
            if route.timeout_ms <= 0:
                raise ConfigError("invalid_timeout",
                                  f"路由 {route.name} 的 timeout_ms 必须 > 0")
            if route.max_retries < 0:
                raise ConfigError("invalid_retries",
                                  f"路由 {route.name} 的 max_retries 必须 >= 0")
        # 前缀冲突：两个路由的 path_prefix 互为前缀时无法确定匹配优先级，拒绝
        prefixes = [(r.match.path_prefix, r.name) for r in cfg.routes]
        for i in range(len(prefixes)):
            for j in range(i + 1, len(prefixes)):
                a, b = prefixes[i][0], prefixes[j][0]
                if a.startswith(b) or b.startswith(a):
                    raise ConfigError("conflicting_path_prefix",
                                      f"路由 {prefixes[i][1]}({a}) 与 {prefixes[j][1]}({b}) 前缀冲突")
        callers = [r.caller for r in cfg.rate_limits]
        if len(callers) != len(set(callers)):
            raise ConfigError("duplicate_caller", "限流配置中 caller 重复")
        for rl in cfg.rate_limits:
            if rl.rate_per_second <= 0 or rl.burst < 1:
                raise ConfigError("invalid_rate_limit",
                                  f"caller {rl.caller} 的 rate_per_second 必须 > 0 且 burst >= 1")
            if rl.quota is not None and rl.quota < 1:
                raise ConfigError("invalid_quota",
                                  f"caller {rl.caller} 的 quota 必须 >= 1")
        cb = cfg.circuit_breaker
        if cb.failure_threshold < 1 or cb.open_seconds <= 0 or cb.half_open_max_probes < 1:
            raise ConfigError("invalid_circuit_breaker",
                              "failure_threshold/half_open_max_probes 必须 >= 1 且 open_seconds > 0")
