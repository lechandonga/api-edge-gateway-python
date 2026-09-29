"""按调用方维度的限流（令牌桶）与配额控制，并发安全。"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .config import RateLimitRule
from .errors import GatewayError


@dataclass(frozen=True)
class RateDecision:
    """限流判定结果，含可对外展示的状态提示。"""
    allowed: bool
    reason: str                       # "ok" / "rate_limited" / "quota_exhausted"
    remaining_burst: float            # 当前桶内剩余令牌
    remaining_quota: int | None       # 剩余配额
    retry_after: float | None         # 建议等待秒数


class RateLimiter:
    """单调用方令牌桶 + 配额计数器（支持按周期自动重置配额）。"""

    def __init__(self, rule: RateLimitRule) -> None:
        self._rule = rule
        self._lock = threading.Lock()
        self._tokens = float(rule.burst)
        self._last: float | None = None
        self._quota_used = 0
        self._window_start: float | None = None
        self._last_reject: str | None = None      # 最近一次拒绝原因

    def _reset_window_if_due(self, now: float) -> None:
        """配额周期到期则重置已用计数；调用方需持锁。"""
        period = self._rule.quota_period_seconds
        if period is None:
            return
        if self._window_start is None:
            self._window_start = now
        elif now - self._window_start >= period:
            self._quota_used = 0
            self._window_start = now

    def _quota_reset_in(self, now: float) -> float | None:
        """距配额周期重置的秒数；无周期时返回 None。"""
        period = self._rule.quota_period_seconds
        if period is None or self._window_start is None:
            return None
        return max(0.0, period - (now - self._window_start))

    def check(self, now: float) -> RateDecision:
        """判定一次请求是否放行；放行则消耗令牌与配额。

        令牌补充与消耗、配额周期重置与扣减都在同一把锁内完成，
        并发下不会超发或丢失计数，重置与请求并发也不会多放。
        """
        with self._lock:
            if self._last is None:
                self._last = now
            # 按流逝时间补充令牌，封顶 burst
            elapsed = max(0.0, now - self._last)
            self._tokens = min(float(self._rule.burst),
                               self._tokens + elapsed * self._rule.rate_per_second)
            self._last = now
            self._reset_window_if_due(now)

            remaining_quota = (None if self._rule.quota is None
                               else self._rule.quota - self._quota_used)

            if self._tokens < 1.0:
                # 速率超限：给出补满一个令牌所需的等待时间
                wait = (1.0 - self._tokens) / self._rule.rate_per_second
                self._last_reject = "rate_limited"
                return RateDecision(False, "rate_limited", self._tokens,
                                    remaining_quota, wait)
            if remaining_quota is not None and remaining_quota <= 0:
                self._last_reject = "quota_exhausted"
                return RateDecision(False, "quota_exhausted", self._tokens, 0,
                                    self._quota_reset_in(now))

            self._tokens -= 1.0
            if self._rule.quota is not None:
                self._quota_used += 1
                remaining_quota = self._rule.quota - self._quota_used
            return RateDecision(True, "ok", self._tokens, remaining_quota, None)

    def status(self, now: float) -> dict:
        """当前配额/限流状态快照，不消耗任何额度。"""
        with self._lock:
            if self._last is None:
                self._last = now
            elapsed = max(0.0, now - self._last)
            self._tokens = min(float(self._rule.burst),
                               self._tokens + elapsed * self._rule.rate_per_second)
            self._last = now
            self._reset_window_if_due(now)
            remaining_quota = (None if self._rule.quota is None
                               else self._rule.quota - self._quota_used)
            # 补满一个令牌还需多久（已够则为 0）
            if self._tokens >= 1.0:
                token_wait = 0.0
            else:
                token_wait = (1.0 - self._tokens) / self._rule.rate_per_second
            return {
                "caller": self._rule.caller,
                "remaining_burst": self._tokens,
                "remaining_quota": remaining_quota,
                "last_reject": self._last_reject,
                "retry_after": token_wait if self._tokens < 1.0 else None,
                "quota_period_seconds": self._rule.quota_period_seconds,
                "quota_reset_in": self._quota_reset_in(now),
            }


class RateLimitRegistry:
    """按调用方管理限流器；配置更新时保留已有计数状态。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._limiters: dict[str, RateLimiter] = {}

    def apply_rules(self, rules: tuple[RateLimitRule, ...]) -> None:
        """应用新规则集：为新调用方建限流器，移除已删除的调用方。

        已存在且规则未变的调用方保留计数状态（不重置已用配额）。
        """
        with self._lock:
            wanted = {r.caller: r for r in rules}
            for caller in list(self._limiters):
                if caller not in wanted:
                    del self._limiters[caller]
            for caller, rule in wanted.items():
                existing = self._limiters.get(caller)
                if existing is None or existing._rule != rule:
                    self._limiters[caller] = RateLimiter(rule)

    def check(self, caller: str, now: float) -> RateDecision:
        """对指定调用方判定；无规则时默认放行。"""
        with self._lock:
            limiter = self._limiters.get(caller)
        if limiter is None:
            return RateDecision(True, "ok", 0.0, None, None)
        return limiter.check(now)

    def status(self, caller: str, now: float) -> dict | None:
        """查询调用方当前状态；无规则时返回 None。"""
        with self._lock:
            limiter = self._limiters.get(caller)
        if limiter is None:
            return None
        return limiter.status(now)
