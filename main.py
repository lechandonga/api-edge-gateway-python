"""网关启动入口：uv run python main.py [port]

启动后可用：
  代理入口   http://127.0.0.1:8000/proxy/<path>
  配置查看   GET  http://127.0.0.1:8000/admin/config
  配置热更新 PUT  http://127.0.0.1:8000/admin/config
另需启动上游实例：uv run python upstream_demo.py 9001 --name u1
"""
import sys

import uvicorn

from gateway.app import create_app
from gateway.config import (
    GatewayConfig, RouteConfig, RouteMatch, UpstreamConfig,
)
from gateway.config import ConfigStore

DEFAULT_CONFIG = GatewayConfig(routes=[RouteConfig(
    name="api",
    match=RouteMatch(path_prefix="/api"),
    upstreams=[UpstreamConfig(name="u1", base_url="http://127.0.0.1:9001")],
)])

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    uvicorn.run(create_app(ConfigStore(DEFAULT_CONFIG)),
                host="127.0.0.1", port=port)
