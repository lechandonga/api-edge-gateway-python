"""负载均衡：按路由配置的策略在上游实例间选择目标。"""

from __future__ import annotations

import threading

from .config import LBStrategy, Route, Upstream
from .errors import GatewayError


class LoadBalancer:
    """单个路由的负载均衡器，内部状态线程安全。"""

    def __init__(self, route: Route) -> None:
        self._route = route
        self._lock = threading.Lock()
        self._rr_index = 0
        self._inflight: dict[str, int] = {u.name: 0 for u in route.upstreams}

    def pick(self) -> Upstream:
        """按策略选择一个上游实例。"""
        raise NotImplementedError

    def on_request_start(self, upstream: Upstream) -> None:
        """记录请求开始（用于 least_connections）。"""

    def on_request_end(self, upstream: Upstream) -> None:
        """记录请求结束。"""
