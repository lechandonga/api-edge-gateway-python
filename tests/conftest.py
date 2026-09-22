import logging
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from gateway.app import create_app
from gateway.config import ConfigStore, GatewayConfig
from upstream_demo import create_upstream

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s %(name)s %(message)s")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class UpstreamServer:
    """在后台线程中运行的本地上游实例。"""

    def __init__(self, name: str):
        self.name = name
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(create_upstream(name), host="127.0.0.1",
                                port=self.port, log_level="warning")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self):
        self._thread.start()
        deadline = time.monotonic() + 10
        while not self._server.started:
            if time.monotonic() > deadline:
                raise RuntimeError(f"upstream {self.name} failed to start")
            time.sleep(0.02)
        return self

    def stop(self):
        self._server.should_exit = True
        self._thread.join(timeout=5)


@pytest.fixture
def upstream_factory():
    servers = []

    def make(name: str) -> UpstreamServer:
        srv = UpstreamServer(name).start()
        servers.append(srv)
        return srv

    yield make
    for s in servers:
        s.stop()


@pytest.fixture
def gateway():
    """返回 (app, store, client_factory)；client 走 ASGI transport。"""
    store = ConfigStore()
    app = create_app(store)

    async def make_client():
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway")

    return app, store, make_client
