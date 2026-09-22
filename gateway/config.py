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
    """校验配置，非法或冲突时抛出带可区分原因的 GatewayError。"""
    raise NotImplementedError


class ConfigStore:
    """线程安全的配置持有者：读取返回当前快照，更新整体原子替换。"""

    def __init__(self, initial: GatewayConfig) -> None:
        self._lock = threading.Lock()
        self._current = initial

    def get(self) -> GatewayConfig:
        return self._current

    def update(self, cfg: GatewayConfig) -> GatewayConfig:
        """校验并原子替换；失败时旧配置保持不变。"""
        raise NotImplementedError
