"""路由匹配与负载均衡：按请求特征分发、策略选择目标。"""
from gateway.config import (
    GatewayConfig, LBStrategy, RouteConfig, RouteMatch, UpstreamConfig,
)


def two_upstream_config(u1: str, u2: str,
                        strategy: LBStrategy = LBStrategy.ROUND_ROBIN,
                        headers: dict | None = None) -> GatewayConfig:
    return GatewayConfig(routes=[RouteConfig(
        name="api",
        match=RouteMatch(path_prefix="/api", headers=headers or {}),
        upstreams=[UpstreamConfig(name="u1", base_url=u1),
                   UpstreamConfig(name="u2", base_url=u2)],
        strategy=strategy,
    )])


async def test_round_robin_distributes_evenly(gateway, upstream_factory):
    app, store, make_client = gateway
    u1, u2 = upstream_factory("u1"), upstream_factory("u2")
    store.update(two_upstream_config(u1.base_url, u2.base_url))
    async with await make_client() as c:
        hits = [(await c.get("/proxy/api/echo")).json()["upstream"]
                for _ in range(6)]
    assert hits == ["u1", "u2", "u1", "u2", "u1", "u2"]


async def test_weighted_round_robin(gateway, upstream_factory):
    app, store, make_client = gateway
    u1, u2 = upstream_factory("u1"), upstream_factory("u2")
    cfg = two_upstream_config(u1.base_url, u2.base_url)
    cfg.routes[0].upstreams[0].weight = 3
    store.update(cfg)
    async with await make_client() as c:
        hits = [(await c.get("/proxy/api/echo")).json()["upstream"]
                for _ in range(8)]
    assert hits.count("u1") == 6 and hits.count("u2") == 2


async def test_header_based_routing(gateway, upstream_factory):
    app, store, make_client = gateway
    u1, u2 = upstream_factory("u1"), upstream_factory("u2")
    # 两条路由前缀不冲突：/api 与 /beta
    store.update(GatewayConfig(routes=[
        RouteConfig(name="stable", match=RouteMatch(path_prefix="/api"),
                    upstreams=[UpstreamConfig(name="u1", base_url=u1.base_url)]),
        RouteConfig(name="beta",
                    match=RouteMatch(path_prefix="/beta",
                                     headers={"x-canary": "1"}),
                    upstreams=[UpstreamConfig(name="u2", base_url=u2.base_url)]),
    ]))
    async with await make_client() as c:
        assert (await c.get("/proxy/api/echo")).json()["upstream"] == "u1"
        # 带特征头的请求分发到 beta 上游
        r = await c.get("/proxy/beta/echo", headers={"x-canary": "1"})
        assert r.json()["upstream"] == "u2"
        # 缺少特征头 -> 不匹配 -> 404
        r = await c.get("/proxy/beta/echo")
        assert r.status_code == 404 and r.json()["error"] == "no_route"


async def test_method_mismatch_no_route(gateway, upstream_factory):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(GatewayConfig(routes=[RouteConfig(
        name="ro", match=RouteMatch(path_prefix="/api", methods=["GET"]),
        upstreams=[UpstreamConfig(name="u1", base_url=u1.base_url)],
    )]))
    async with await make_client() as c:
        assert (await c.get("/proxy/api/echo")).status_code == 200
        r = await c.post("/proxy/api/count")
        assert r.status_code == 404 and r.json()["error"] == "no_route"
