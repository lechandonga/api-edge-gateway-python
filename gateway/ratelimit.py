"""按调用方维度的限流（令牌桶）与周期配额控制，并发安全。

每个调用方暴露一份可查询状态：当前剩余令牌/配额、上一次被拒原因
与配额预计恢复时间。所有“补充—判定—扣减—周期重置”都在单把锁内
完成，重置与并发请求撞在一起也不会多放或把未恢复额度算作可用。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .config import RateLimitRule


@dataclass(frozen=True)
class RateDecision:
    """限流判定结果，含可对外展示的状态提示。"""
    allowed: bool
    reason: str                       # "ok" / "rate_limited" / "quota_exhausted"
    remaining_burst: float            # 当前桶内剩余令牌
    remaining_quota: int | None       # 当前周期剩余配额
    retry_after: float | None         # 建议等待秒数
    quota_reset_at: float | None      # 配额下次重置的单调时钟时刻


@dataclass(frozen=True)
class RateStatus:
    """调用方限流状态（管理接口/响应头使用）。"""
    caller: str
    rate_per_second: float
    burst: int
    remaining_burst: float
    quota: int | None
    remaining_quota: int | None
    quota_window_seconds: float | None
    quota_reset_at: float | None      # 单调时钟时刻，None 表示不周期重置
    quota_resets_in: float | None     # 距下次重置秒数
    last_rejected_reason: str | None
    last_rejected_at: float | None    # 单调时钟时刻
    last_rejected_retry_after: float | None


class RateLimiter:
    """单调用方令牌桶 + 可周期重置的配额计数器。"""

    def __init__(self, rule: RateLimitRule) -> None:
        self._rule = rule
        self._lock = threading.Lock()
        self._tokens = float(rule.burst)
        self._last: float | None = None
        # 配额窗口起点（单调时钟）；无周期配额时为 None，配额一次性扣减
        self._window_start: float | None = None
        self._quota_used = 0
        self._last_rejected_reason: str | None = None
        self._last_rejected_at: float | None = None
        self._last_rejected_retry_after: float | None = None

    def _quota_reset_at(self) -> float | None:
        if self._rule.quota is None or self._rule.quota_window_seconds is None:
            return None
        start = self._window_start
        if start is None:
            # 尚无请求占用配额：窗口将在首个请求时开启，暂无可报时刻
            return None
        return start + self._rule.quota_window_seconds

    def _refill_and_reset(self, now: float) -> None:
        """调用方需持锁。补充令牌，并在跨过窗口边界时重置配额。

        用 while 处理长时间无请求跨多个窗口的情况；重置与判定在同一
        临界区内，并发请求要么全在重置前、要么全在重置后，不会出现
        半个窗口的额度被提前放出。
        """
        if self._last is None:
            self._last = now
        elapsed = max(0.0, now - self._last)
        self._tokens = min(
            float(self._rule.burst),
            self._tokens + elapsed * self._rule.rate_per_second)
        self._last = now

        window = self._rule.quota_window_seconds
        if (self._rule.quota is not None and window is not None
                and self._window_start is not None
                and now >= self._window_start + window):
            # 跨窗口：已用配额清零，新窗口从此刻开始
            self._window_start = now
            self._quota_used = 0

    def _remaining_quota_locked(self) -> int | None:
        if self._rule.quota is None:
            return None
        return self._rule.quota - self._quota_used

    def check(self, now: float) -> RateDecision:
        """判定一次请求是否放行；放行则消耗令牌与配额。"""
        with self._lock:
            self._refill_and_reset(now)
            remaining_quota = self._remaining_quota_locked()
            reset_at = self._quota_reset_at()

            if remaining_quota is not None and remaining_quota <= 0:
                # 配额耗尽：等待到窗口结束即可恢复整个周期额度
                wait = (reset_at - now) if reset_at is not None else None
                self._record_rejection("quota_exhausted", now, wait)
                return RateDecision(False, "quota_exhausted", self._tokens, 0,
                                    wait, reset_at)
            if self._tokens < 1.0:
                # 速率超限：补满一个令牌所需等待时间
                wait = (1.0 - self._tokens) / self._rule.rate_per_second
                self._record_rejection("rate_limited", now, wait)
                return RateDecision(False, "rate_limited", self._tokens,
                                    remaining_quota, wait, reset_at)

            self._tokens -= 1.0
            if self._rule.quota is not None:
                if self._window_start is None:
                    if self._rule.quota_window_seconds is not None:
                        self._window_start = now
                self._quota_used += 1
                remaining_quota = self._rule.quota - self._quota_used
                reset_at = self._quota_reset_at()
            return RateDecision(True, "ok", self._tokens, remaining_quota,
                                None, reset_at)

    def _record_rejection(self, reason: str, now: float,
                          retry_after: float | None) -> None:
        self._last_rejected_reason = reason
        self._last_rejected_at = now
        self._last_rejected_retry_after = retry_after

    def status(self, now: float) -> RateStatus:
        """查询当前状态：只读地推进令牌补充与窗口重置，不消耗额度。"""
        with self._lock:
            self._refill_and_reset(now)
            reset_at = self._quota_reset_at()
            resets_in = (max(0.0, reset_at - now) if reset_at is not None
                         else None)
            rejected_ago = (None if self._last_rejected_at is None
                            else now - self._last_rejected_at)
            return RateStatus(
                caller=self._rule.caller,
                rate_per_second=self._rule.rate_per_second,
                burst=self._rule.burst,
                remaining_burst=round(self._tokens, 3),
                quota=self._rule.quota,
                remaining_quota=self._remaining_quota_locked(),
                quota_window_seconds=self._rule.quota_window_seconds,
                quota_reset_at=reset_at,
                quota_resets_in=round(resets_in, 3) if resets_in is not None
                else None,
                last_rejected_reason=self._last_rejected_reason,
                last_rejected_at=rejected_ago,
                last_rejected_retry_after=self._last_rejected_retry_after)


class RateLimitRegistry:
    """按调用方管理限流器；配置更新时保留已有计数状态。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._limiters: dict[str, RateLimiter] = {}

    def apply_rules(self, rules: tuple[RateLimitRule, ...]) -> None:
        """应用新规则集：为新调用方建限流器，移除已删除的调用方。

        已存在且规则未变的调用方保留计数状态（不重置已用配额）；
        规则变化（如仅调限流参数、配额或周期）时重建限流器。
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

    def _get(self, caller: str) -> RateLimiter | None:
        with self._lock:
            return self._limiters.get(caller)

    def check(self, caller: str, now: float) -> RateDecision:
        """对指定调用方判定；无规则时默认放行。"""
        limiter = self._get(caller)
        if limiter is None:
            return RateDecision(True, "ok", 0.0, None, None, None)
        return limiter.check(now)

    def status(self, caller: str, now: float) -> RateStatus | None:
        """查询调用方状态；调用方无规则时返回 None。"""
        limiter = self._get(caller)
        if limiter is None:
            return None
        return limiter.status(now)
