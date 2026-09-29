"""负载均衡：按路由配置的策略在上游实例间选择目标。"""

from __future__ import annotations

import random
import threading

from .config import LBStrategy, Upstream
from .errors import ErrorCode, GatewayError


class LoadBalancer:
    """单个上游组的负载均衡器，内部状态线程安全。"""

    def __init__(self, group: str, upstreams: tuple[Upstream, ...],
                 strategy: LBStrategy) -> None:
        self._group = group
        self._upstreams = upstreams
        self._strategy = strategy
        self._lock = threading.Lock()
        self._rr_index = 0
        self._inflight: dict[str, int] = {u.name: 0 for u in upstreams}
        # 按权重展开的轮询序列
        self._weighted: list[Upstream] = [
            u for u in upstreams for _ in range(u.weight)
        ]

    def pick(self, exclude: set[str] | None = None) -> Upstream:
        """按策略选择一个上游实例；exclude 中的实例本轮不参与。"""
        exclude = exclude or set()
        with self._lock:
            candidates = [u for u in self._upstreams if u.name not in exclude]
            if not candidates:
                raise GatewayError(ErrorCode.NO_HEALTHY_UPSTREAM,
                                   f"上游组 {self._group} 无可用上游实例", 503)
            strategy = self._strategy
            if strategy is LBStrategy.RANDOM:
                return random.choice(candidates)
            if strategy is LBStrategy.LEAST_CONNECTIONS:
                return min(candidates, key=lambda u: self._inflight[u.name])
            # round_robin：在加权序列上轮转，跳过被排除的实例
            for _ in range(len(self._weighted)):
                up = self._weighted[self._rr_index % len(self._weighted)]
                self._rr_index += 1
                if up.name not in exclude:
                    return up
            return candidates[0]

    def on_request_start(self, upstream: Upstream) -> None:
        """记录请求开始（用于 least_connections）。"""
        with self._lock:
            self._inflight[upstream.name] += 1

    def on_request_end(self, upstream: Upstream) -> None:
        """记录请求结束。"""
        with self._lock:
            self._inflight[upstream.name] = max(0, self._inflight[upstream.name] - 1)
