"""请求转发：超时、熔断、有限重试（仅幂等请求）与错误归一化。"""

from __future__ import annotations

import httpx

from .breaker import CircuitBreaker
from .config import Route
from .errors import GatewayError
from .loadbalancer import LoadBalancer

# 可安全重放（幂等）的方法才允许重试，避免重复副作用
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


class ProxyEngine:
    """按路由转发请求，组合负载均衡、熔断与重试。"""

    def __init__(self, route: Route, client: httpx.AsyncClient) -> None:
        self._route = route
        self._client = client
        self._lb = LoadBalancer(route)
        self._breakers: dict[str, CircuitBreaker] = {
            u.name: CircuitBreaker(u.name, route.breaker) for u in route.upstreams
        }

    async def forward(self, method: str, path: str, headers: dict[str, str],
                      body: bytes) -> httpx.Response:
        """转发请求并返回上游响应；失败归一化为 GatewayError。"""
        raise NotImplementedError
