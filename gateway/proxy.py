"""转发执行：超时、重试（仅幂等请求）、错误归一化。"""
from __future__ import annotations

import logging
from enum import Enum

import httpx

logger = logging.getLogger("gateway.proxy")


class GatewayErrorKind(str, Enum):
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    UPSTREAM_ERROR = "upstream_error"
    CIRCUIT_OPEN = "circuit_open"


class GatewayError(Exception):
    def __init__(self, kind: GatewayErrorKind, detail: str = ""):
        self.kind = kind
        self.detail = detail
        super().__init__(f"{kind}: {detail}")


IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


def is_retryable(method: str) -> bool:
    """只有幂等方法可安全重放；POST/PATCH 等重试可能产生重复副作用，禁止重试。"""
    return method.upper() in IDEMPOTENT_METHODS


class ProxyExecutor:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def forward(self, base_url: str, method: str, path: str,
                      headers: dict[str, str], body: bytes,
                      timeout_ms: int, max_retries: int) -> httpx.Response:
        """转发请求并归一化异常。

        重试仅作用于幂等方法（is_retryable），且只对传输层错误
        （超时/连接失败）重试；非幂等方法 max_retries 一律视为 0，
        避免重复副作用。
        """
        client = await self._get_client()
        attempts = 1 + (max_retries if is_retryable(method) else 0)
        url = base_url.rstrip("/") + "/" + path.lstrip("/")
        last_error: GatewayError | None = None
        for attempt in range(1, attempts + 1):
            try:
                logger.info("forward attempt=%d/%d %s %s", attempt, attempts, method, url)
                resp = await client.request(
                    method, url,
                    headers={k: v for k, v in headers.items()
                             if k.lower() not in ("host", "content-length")},
                    content=body,
                    timeout=timeout_ms / 1000.0,
                )
                return resp
            except httpx.TimeoutException as e:
                last_error = GatewayError(GatewayErrorKind.UPSTREAM_TIMEOUT, str(e))
            except httpx.TransportError as e:
                last_error = GatewayError(GatewayErrorKind.UPSTREAM_UNAVAILABLE, str(e))
            logger.warning("attempt %d failed: %s", attempt, last_error.kind.value)
        raise last_error  # type: ignore[misc]
