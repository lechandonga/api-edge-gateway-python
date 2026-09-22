"""冒烟：网关把请求转发到本地真实上游并带回响应。"""

from tests.conftest import GatewayClient, base_config


async def test_proxy_roundtrip(upstream_factory):
    up = upstream_factory("u1")
    async with GatewayClient(base_config([up])) as gw:
        resp = await gw.get("/api/hello")
        assert resp.status_code == 200
        assert resp.json()["upstream"] == "u1"
        assert up.requests[0]["path"] == "api/hello"
