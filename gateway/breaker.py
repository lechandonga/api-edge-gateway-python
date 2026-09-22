"""熔断器：关闭 / 打开 / 半开三态转换。"""

from __future__ import annotations

import logging
import threading
from enum import Enum

from .config import BreakerConfig
from .errors import ErrorCode, GatewayError

logger = logging.getLogger("gateway.breaker")


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

    def _transition(self, new: BreakerState, now: float, reason: str) -> None:
        old = self._state
        if old is new:
            return
        self._state = new
        if new is BreakerState.OPEN:
            self._opened_at = now
        if new is BreakerState.HALF_OPEN:
            self._half_open_inflight = 0
        logger.info("breaker %s: %s -> %s (%s)", self.name, old.value, new.value, reason)

    def before_request(self, now: float) -> None:
        """请求前检查；熔断打开且未到探测时间时抛 CIRCUIT_OPEN。"""
        with self._lock:
            if self._state is BreakerState.OPEN:
                elapsed = now - self._opened_at
                if elapsed >= self._config.reset_timeout:
                    self._transition(BreakerState.HALF_OPEN, now,
                                     f"reset_timeout {self._config.reset_timeout}s 到期")
                else:
                    raise GatewayError(
                        ErrorCode.CIRCUIT_OPEN,
                        f"上游 {self.name} 熔断中，{self._config.reset_timeout - elapsed:.2f}s 后重试",
                        503, retry_after=self._config.reset_timeout - elapsed)
            if self._state is BreakerState.HALF_OPEN:
                if self._half_open_inflight >= self._config.half:
                    raise GatewayError(ErrorCode.CIRCUIT_OPEN,
                                       f"上游 {self.name} 半开探测中，拒绝并发探测", 503,
                                       retry_after=0.1)
                self._half_open_inflight += 1

    def on_success(self) -> None:
        """记录成功：关闭熔断器并清零失败计数。"""
        with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
            self._consecutive_failures = 0
            if self._state is not BreakerState.CLOSED:
                self._transition(BreakerState.CLOSED, 0.0, "探测/请求成功")

    def on_failure(self, now: float) -> None:
        """记录失败：累计连续失败，达到阈值则打开。"""
        with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
                # 半开探测失败：立即重新打开
                self._consecutive_failures = self._config.failure_threshold
                self._transition(BreakerState.OPEN, now, "半开探测失败")
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._config.failure_threshold:
                self._transition(
                    BreakerState.OPEN, now,
                    f"连续失败 {self._consecutive_failures} 次达到阈值 "
                    f"{self._config.failure_threshold}")
