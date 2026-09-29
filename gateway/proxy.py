"""请求转发：超时、熔断、有限重试（仅幂等请求）与错误归一化。"""

from __future__ import annotations

import logging
import time

import httpx

from .breaker import CircuitBreaker
from .config import BackendGroup, BreakerConfig, Upstream
from .errors import ErrorCode, GatewayError
from .loadbalancer import LoadBalancer

# 可安全重放（幂等）的方法才允许重试，避免重复副作用
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})

logger = logging.getLogger("gateway.proxy")


class ProxyEngine:
    """按后端组转发请求，组合负载均衡、熔断与重试。

    引擎与具体（路由, 后端组）绑定；配置切换/回退时按内容指纹复用，
    进行中请求持有的引擎引用永远有效、不会被原地修改。
    """

    def __init__(self, route_name: str, group: BackendGroup,
                 breaker_config: BreakerConfig,
                 client: httpx.AsyncClient) -> None:
        self._route_name = route_name
        self._group = group
        self._breaker_config = breaker_config
        self._client = client
        self._lb = LoadBalancer(group)
        self._breakers: dict[str, CircuitBreaker] = {
            u.name: CircuitBreaker(u.name, breaker_config)
            for u in group.upstreams
        }

    def breakers(self) -> dict[str, CircuitBreaker]:
        return self._breakers

    async def _send(self, upstream: Upstream, method: str, path: str,
                    headers: dict[str, str], body: bytes) -> httpx.Response:
        url = upstream.base_url.rstrip("/") + path
        return await self._client.request(
            method, url, headers=headers, content=body,
            timeout=self._breaker_config.timeout_seconds)

    async def forward(self, method: str, path: str, headers: dict[str, str],
                      body: bytes) -> httpx.Response:
        """转发请求并返回上游响应；失败归一化为 GatewayError。

        重试仅作用于幂等方法；非幂等方法（如 POST）只发一次，
        不会因重试产生重复副作用。
        """
        method = method.upper()
        idempotent = method in IDEMPOTENT_METHODS
        attempts = 1 + (self._group.max_retries if idempotent else 0)
        logger.info("request %s %s route=%s group=%s idempotent=%s attempts=%d",
                    method, path, self._route_name, self._group.name,
                    idempotent, attempts)

        last_error: GatewayError | None = None
        tried: set[str] = set()
        for attempt in range(attempts):
            # 选择上游：优先未试过且熔断关闭的实例
            upstream: Upstream | None = None
            breaker: CircuitBreaker | None = None
            now = time.monotonic()
            pool = [u for u in self._group.upstreams if u.name not in tried]
            for _ in range(len(pool) + 1):
                try:
                    candidate = self._lb.pick(exclude=tried)
                except GatewayError:
                    break
                cb = self._breakers[candidate.name]
                try:
                    cb.before_request(now)
                    upstream, breaker = candidate, cb
                    break
                except GatewayError as e:
                    logger.info("breaker reject upstream=%s reason=%s",
                                candidate.name, e.message)
                    tried.add(candidate.name)
                    last_error = e
            if upstream is None or breaker is None:
                break
            tried.add(upstream.name)

            self._lb.on_request_start(upstream)
            try:
                resp = await self._send(upstream, method, path, headers, body)
            except httpx.TimeoutException:
                breaker.on_failure(time.monotonic())
                last_error = GatewayError(
                    ErrorCode.UPSTREAM_TIMEOUT,
                    f"上游 {upstream.name} 超时（>{self._breaker_config.timeout_seconds}s）",
                    504)
                logger.info("upstream timeout upstream=%s attempt=%d",
                            upstream.name, attempt + 1)
            except httpx.HTTPError as exc:
                breaker.on_failure(time.monotonic())
                last_error = GatewayError(ErrorCode.UPSTREAM_UNAVAILABLE,
                                          f"上游 {upstream.name} 连接失败: {exc}", 502)
                logger.info("upstream connect error upstream=%s attempt=%d err=%s",
                            upstream.name, attempt + 1, exc)
            else:
                if resp.status_code >= 500:
                    # 上游 5xx：计入熔断，归一化为可区分错误
                    breaker.on_failure(time.monotonic())
                    last_error = GatewayError(
                        ErrorCode.UPSTREAM_UNAVAILABLE,
                        f"上游 {upstream.name} 返回 {resp.status_code}", 502)
                    logger.info("upstream 5xx upstream=%s status=%d attempt=%d",
                                upstream.name, resp.status_code, attempt + 1)
                else:
                    breaker.on_success()
                    logger.info("upstream ok upstream=%s status=%d attempt=%d",
                                upstream.name, resp.status_code, attempt + 1)
                    return resp
            finally:
                self._lb.on_request_end(upstream)

            if not idempotent:
                break  # 非幂等请求绝不重试

        if last_error is not None:
            raise last_error
        raise GatewayError(ErrorCode.NO_HEALTHY_UPSTREAM,
                           f"路由 {self._route_name} 后端组 {self._group.name} "
                           f"无可用上游实例", 503)
