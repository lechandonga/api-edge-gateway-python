"""测试基建：本地真实上游实例 + 网关客户端。

上游用 uvicorn 跑在独立线程的 TCP 端口上，网关通过 httpx 真实访问，
以验证请求分发、限流与上游异常下的行为。
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request

from gateway.app import create_app
from gateway.config import GatewayConfig, config_from_dict

# 单测日志：打印请求、判定依据与熔断状态变化
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class UpstreamServer:
    """一个可控行为的本地上游实例。"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.requests: list[dict] = []      # 记录收到的请求（用于副作用计数）
        self.behavior = "ok"                # ok / slow / fail500 / down
        self.slow_seconds = 1.0
        self._app = FastAPI(title=f"upstream-{name}")
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self._setup_routes()

    def _setup_routes(self) -> None:
        @self._app.api_route("/{path:path}",
                             methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
        async def handle(request: Request, path: str) -> dict:
            body = await request.body()
            self.requests.append({"method": request.method, "path": path,
                                  "body": body.decode(errors="replace")})
            if self.behavior == "slow":
                await asyncio.sleep(self.slow_seconds)
            if self.behavior == "fail500":
                from fastapi.responses import JSONResponse
                return JSONResponse({"err": "boom"}, status_code=500)
            return {"upstream": self.name, "method": request.method,
                    "path": path, "body": body.decode(errors="replace")}

    def start(self) -> "UpstreamServer":
        config = uvicorn.Config(self._app, host="127.0.0.1", port=self.port,
                                log_level="warning")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 10
        while not self._server.started:
            if time.monotonic() > deadline:
                raise RuntimeError(f"upstream {self.name} 启动超时")
            time.sleep(0.02)
        return self

    def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5)


@pytest.fixture()
def upstream_factory():
    servers: list[UpstreamServer] = []

    def make(name: str) -> UpstreamServer:
        srv = UpstreamServer(name).start()
        servers.append(srv)
        return srv

    yield make
    for s in servers:
        s.stop()


class GatewayClient:
    """对网关应用的异步客户端（ASGI 直连，上游走真实 HTTP）。"""

    def __init__(self, config_dict: dict) -> None:
        cfg: GatewayConfig = config_from_dict(config_dict)
        self.app = create_app(cfg)
        transport = httpx.ASGITransport(app=self.app)
        self.client = httpx.AsyncClient(transport=transport,
                                        base_url="http://gateway")

    async def __aenter__(self) -> "GatewayClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.client.aclose()
        await self.app.state.gateway.close()

    def __getattr__(self, item):
        return getattr(self.client, item)


def base_config(upstreams: list[UpstreamServer], **route_kw) -> dict:
    return {
        "version": 1,
        "routes": [{
            "name": "main",
            "path_prefix": "/api",
            "strategy": route_kw.pop("strategy", "round_robin"),
            "max_retries": route_kw.pop("max_retries", 1),
            "upstreams": [{"name": u.name, "base_url": u.base_url}
                          for u in upstreams],
            "breaker": route_kw.pop("breaker", {
                "failure_threshold": 3, "reset_timeout": 0.5,
                "half": 1, "timeout_seconds": 0.5}),
        }],
        "rate_limits": route_kw.pop("rate_limits", []),
    }
