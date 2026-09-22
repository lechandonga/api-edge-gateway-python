"""请求分发：按策略在多上游实例间分配流量。"""

from tests.conftest import GatewayClient, base_config


async def test_round_robin_distribution(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1, u2])) as gw:
        hits = [(await gw.get("/api/x")).json()["upstream"] for _ in range(6)]
        assert hits == ["u1", "u2", "u1", "u2", "u1", "u2"]
        assert len(u1.requests) == 3 and len(u2.requests) == 3


async def test_route_matching_by_path_prefix(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    cfg = {
        "version": 1,
        "routes": [
            {"name": "api", "path_prefix": "/api",
             "upstreams": [{"name": "u1", "base_url": u1.base_url}]},
            {"name": "web", "path_prefix": "/web",
             "upstreams": [{"name": "u2", "base_url": u2.base_url}]},
        ],
    }
    async with GatewayClient(cfg) as gw:
        assert (await gw.get("/api/a")).json()["upstream"] == "u1"
        assert (await gw.get("/web/b")).json()["upstream"] == "u2"
        resp = await gw.get("/other")
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "route_not_found"
