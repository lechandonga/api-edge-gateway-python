"""上游熔断器：closed -> open -> half_open 状态机。"""
from __future__ import annotations

import logging
import threading
import time
from enum import Enum

from gateway.config import CircuitBreakerConfig

logger = logging.getLogger("gateway.circuitbreaker")


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    pass


class CircuitBreaker:
    """单个上游的熔断器。

    closed: 正常放行，连续失败达到 failure_threshold 后打开。
    open: 直接拒绝（CircuitOpenError），open_seconds 后进入 half_open。
    half_open: 放行至多 half_open_max_probes 个探测请求；
               探测成功则关闭并重置计数，失败则重新打开。
    """

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self._config = config
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._half_open_inflight = 0

    @property
    def state(self) -> BreakerState:
        with self._lock:
            self._maybe_transition_to_half_open()
            return self._state

    def _maybe_transition_to_half_open(self) -> None:
        if (self._state == BreakerState.OPEN
                and time.monotonic() - self._opened_at >= self._config.open_seconds):
            self._transition(BreakerState.HALF_OPEN, "open 超时，进入半开探测")

    def _transition(self, new_state: BreakerState, reason: str) -> None:
        old = self._state
        self._state = new_state
        if new_state == BreakerState.OPEN:
            self._opened_at = time.monotonic()
        if new_state == BreakerState.CLOSED:
            self._consecutive_failures = 0
            self._half_open_inflight = 0
        if new_state == BreakerState.HALF_OPEN:
            self._half_open_inflight = 0
        logger.warning("breaker[%s] %s -> %s (%s)", self.name, old.value, new_state.value, reason)

    def before_request(self) -> None:
        """请求前检查；熔断打开时抛 CircuitOpenError。"""
        with self._lock:
            self._maybe_transition_to_half_open()
            if self._state == BreakerState.OPEN:
                raise CircuitOpenError(f"circuit open for upstream {self.name}")
            if self._state == BreakerState.HALF_OPEN:
                if self._half_open_inflight >= self._config.half_open_max_probes:
                    raise CircuitOpenError(f"half-open probe limit reached for {self.name}")
                self._half_open_inflight += 1

    def on_success(self) -> None:
        with self._lock:
            if self._state == BreakerState.HALF_OPEN:
                self._transition(BreakerState.CLOSED, "半开探测成功，熔断关闭")
            else:
                self._consecutive_failures = 0

    def on_failure(self) -> None:
        with self._lock:
            if self._state == BreakerState.HALF_OPEN:
                self._transition(BreakerState.OPEN, "半开探测失败，重新打开")
                return
            self._consecutive_failures += 1
            if (self._state == BreakerState.CLOSED
                    and self._consecutive_failures >= self._config.failure_threshold):
                self._transition(BreakerState.OPEN,
                                 f"连续失败 {self._consecutive_failures} 次达到阈值")
