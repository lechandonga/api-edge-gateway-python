"""按请求特征的路由匹配与负载均衡。"""
from __future__ import annotations

import random
import threading

from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig


class NoRouteError(Exception):
    pass


class NoHealthyUpstreamError(Exception):
    pass


class Router:
    def __init__(self, config: GatewayConfig):
        self._config = config

    def match(self, path: str, method: str, headers: dict[str, str]) -> RouteConfig:
        for route in self._config.routes:
            m = route.match
            if not path.startswith(m.path_prefix):
                continue
            if m.methods and method.upper() not in {x.upper() for x in m.methods}:
                continue
            if any(headers.get(k.lower()) != v for k, v in m.headers.items()):
                continue
            return route
        raise NoRouteError(f"no route for {method} {path}")


class LoadBalancer:
    """对一条路由内的上游按策略选择目标；跟踪进行中的连接数。"""

    def __init__(self, route: RouteConfig):
        self._route = route
        self._lock = threading.Lock()
        self._rr_index = 0
        self._inflight: dict[str, int] = {u.name: 0 for u in route.upstreams}
        # 按权重展开的上游序列（round_robin/random 使用）
        self._weighted: list[UpstreamConfig] = [
            u for u in route.upstreams for _ in range(u.weight)
        ]

    def pick(self, healthy: list[str]) -> UpstreamConfig:
        candidates = [u for u in self._weighted if u.name in healthy]
        if not candidates:
            raise NoHealthyUpstreamError(f"route {self._route.name} has no healthy upstream")
        strategy = self._route.strategy
        with self._lock:
            if strategy == "round_robin":
                chosen = candidates[self._rr_index % len(candidates)]
                self._rr_index += 1
            elif strategy == "random":
                chosen = random.choice(candidates)
            elif strategy == "least_connections":
                chosen = min(candidates, key=lambda u: self._inflight[u.name])
            else:  # pragma: no cover - 配置校验已拦截
                raise ValueError(f"unknown strategy {strategy}")
        return chosen

    def acquire(self, name: str) -> None:
        with self._lock:
            self._inflight[name] += 1

    def release(self, name: str) -> None:
        with self._lock:
            self._inflight[name] -= 1
