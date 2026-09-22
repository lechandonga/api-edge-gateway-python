"""按调用方维度的限流（令牌桶）与配额控制。"""
from __future__ import annotations

import threading
import time
from enum import Enum

from gateway.config import RateLimitConfig


class Decision(str, Enum):
    ALLOWED = "allowed"
    RATE_LIMITED = "rate_limited"   # 超出速率
    QUOTA_EXCEEDED = "quota_exceeded"  # 配额耗尽


class RateLimitResult:
    def __init__(self, decision: Decision, retry_after_seconds: float = 0.0,
                 remaining_quota: int | None = None):
        self.decision = decision
        self.retry_after_seconds = retry_after_seconds
        self.remaining_quota = remaining_quota


class _Bucket:
    """单个 caller 的令牌桶 + 配额窗口计数器（配置由 RateLimiter 持有，便于热更新）。"""

    def __init__(self, cfg: RateLimitConfig):
        self.tokens = float(cfg.burst)
        self.last_refill = time.monotonic()
        self.window_start = time.monotonic()
        self.used_in_window = 0


class RateLimiter:
    """每个 caller 一个令牌桶 + 配额窗口计数器；并发下计数准确。

    判定与扣减在同一把锁内完成（check-and-decrement 原子），
    因此并发请求不会超发；令牌按经过时间惰性补充，配额窗口到期后重置。
    可传入共享的 state 字典，使计数在配置热更新后保持连续。
    """

    def __init__(self, configs: list[RateLimitConfig],
                 state: dict[str, _Bucket] | None = None):
        self._configs = {c.caller: c for c in configs}
        self._buckets: dict[str, _Bucket] = state if state is not None else {}
        self._lock = threading.Lock()

    def check(self, caller: str) -> RateLimitResult:
        cfg = self._configs.get(caller)
        if cfg is None:
            return RateLimitResult(Decision.ALLOWED)
        with self._lock:
            bucket = self._buckets.get(caller)
            if bucket is None:
                bucket = self._buckets[caller] = _Bucket(cfg)
            now = time.monotonic()
            # 惰性补充令牌
            elapsed = now - bucket.last_refill
            bucket.tokens = min(float(cfg.burst),
                                bucket.tokens + elapsed * cfg.rate_per_second)
            bucket.last_refill = now
            # 配额窗口到期重置
            if cfg.quota is not None and now - bucket.window_start >= cfg.quota_window_seconds:
                bucket.window_start = now
                bucket.used_in_window = 0
            remaining_quota = (None if cfg.quota is None
                               else max(0, cfg.quota - bucket.used_in_window))
            # 先查配额（长期约束），再查速率（短期约束），原因可区分
            if cfg.quota is not None and bucket.used_in_window >= cfg.quota:
                retry = cfg.quota_window_seconds - (now - bucket.window_start)
                return RateLimitResult(Decision.QUOTA_EXCEEDED,
                                       retry_after_seconds=max(0.0, retry),
                                       remaining_quota=0)
            if bucket.tokens < 1.0:
                retry = (1.0 - bucket.tokens) / cfg.rate_per_second
                return RateLimitResult(Decision.RATE_LIMITED,
                                       retry_after_seconds=retry,
                                       remaining_quota=remaining_quota)
            # 原子扣减
            bucket.tokens -= 1.0
            if cfg.quota is not None:
                bucket.used_in_window += 1
                remaining_quota = cfg.quota - bucket.used_in_window
            return RateLimitResult(Decision.ALLOWED, remaining_quota=remaining_quota)
