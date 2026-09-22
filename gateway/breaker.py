"""熔断器：关闭 / 打开 / 半开三态转换。"""

from __future__ import annotations

import threading
from enum import Enum

from .config import BreakerConfig
from .errors import GatewayError


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """单个上游实例的熔断器，线程安全。"""

    def __init__(self, name: str, config: BreakerConfig) -> None:
        self.name = name
        self._config = config
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._half_open_inflight = 0

    @property
    def state(self) -> BreakerState:
        return self._state

    def before_request(self, now: float) -> None:
        """请求前检查；熔断打开且未到探测时间时抛 CIRCUIT_OPEN。"""
        raise NotImplementedError

    def on_success(self) -> None:
        """记录成功：关闭熔断器并清零失败计数。"""

    def on_failure(self, now: float) -> None:
        """记录失败：累计连续失败，达到阈值则打开。"""
