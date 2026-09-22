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
    """单调用方令牌桶 + 配额计数器。"""

    def __init__(self, rule: RateLimitRule) -> None:
        self._rule = rule
        self._lock = threading.Lock()
        self._tokens = float(rule.burst)
        self._last = 0.0
        self._quota_used = 0

    def check(self, now: float) -> RateDecision:
        """判定一次请求是否放行；放行则消耗令牌与配额。"""
        raise NotImplementedError


class RateLimitRegistry:
    """按调用方管理限流器；配置更新时保留已有计数状态。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._limiters: dict[str, RateLimiter] = {}

    def apply_rules(self, rules: tuple[RateLimitRule, ...]) -> None:
        """应用新规则集：为新调用方建限流器，移除已删除的调用方。"""

    def check(self, caller: str, now: float) -> RateDecision:
        """对指定调用方判定；无规则时默认放行。"""
        raise NotImplementedError
