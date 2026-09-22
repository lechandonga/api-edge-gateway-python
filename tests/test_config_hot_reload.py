"""配置热更新：原子生效、进行中请求不中断、非法配置被拒绝且原因可区分。"""
import asyncio

import pytest

from gateway.config import (
    CircuitBreakerConfig, GatewayConfig, RateLimitConfig, RouteConfig,
    RouteMatch, UpstreamConfig,
)


def make_config(upstream_url: str, name: str = "u1") -> GatewayConfig:
    return GatewayConfig(routes=[RouteConfig(
        name="api",
        match=RouteMatch(path_prefix="/api"),
        upstreams=[UpstreamConfig(name=name, base_url=upstream_url)],
    )])


async def test_valid_config_applies_immediately(gateway, upstream_factory):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    store.update(make_config(u1.base_url, "u1"))
    async with await make_client() as c:
        r = await c.get("/proxy/api/echo")
        assert r.status_code == 200 and r.json()["upstream"] == "u1"
        # 热更新：新请求立即采用新配置
        resp = await c.put("/admin/config",
                           content=make_config(u2.base_url, "u2").model_dump_json())
        assert resp.status_code == 200
        r = await c.get("/proxy/api/echo")
        assert r.status_code == 200 and r.json()["upstream"] == "u2"


@pytest.mark.parametrize("cfg,reason", [
    (GatewayConfig(routes=[
        RouteConfig(name="a", match=RouteMatch(path_prefix="/x"),
                    upstreams=[UpstreamConfig(name="u", base_url="http://h")]),
        RouteConfig(name="a", match=RouteMatch(path_prefix="/y"),
                    upstreams=[UpstreamConfig(name="u", base_url="http://h")]),
    ]), "duplicate_route_name"),
    (GatewayConfig(routes=[
        RouteConfig(name="a", match=RouteMatch(path_prefix="/api"),
                    upstreams=[UpstreamConfig(name="u", base_url="http://h")]),
        RouteConfig(name="b", match=RouteMatch(path_prefix="/api/v2"),
                    upstreams=[UpstreamConfig(name="u", base_url="http://h")]),
    ]), "conflicting_path_prefix"),
    (GatewayConfig(routes=[
        RouteConfig(name="a", match=RouteMatch(path_prefix="/api"),
                    upstreams=[UpstreamConfig(name="u", base_url="ftp://bad")]),
    ]), "invalid_upstream_url"),
    (GatewayConfig(routes=[
        RouteConfig(name="a", match=RouteMatch(path_prefix="/api"), upstreams=[]),
    ]), "empty_upstreams"),
    (GatewayConfig(rate_limits=[
        RateLimitConfig(caller="c", rate_per_second=0, burst=1),
    ]), "invalid_rate_limit"),
    (GatewayConfig(circuit_breaker=CircuitBreakerConfig(failure_threshold=0)),
     "invalid_circuit_breaker"),
])
async def test_invalid_config_rejected_with_reason(gateway, upstream_factory,
                                                   cfg, reason):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    good = make_config(u1.base_url)
    store.update(good)
    async with await make_client() as c:
        resp = await c.put("/admin/config", content=cfg.model_dump_json())
        assert resp.status_code == 409
        assert resp.json()["error"] == reason
        # 旧配置未被污染，仍然生效
        assert (await c.get("/proxy/api/echo")).json()["upstream"] == "u1"


async def test_inflight_request_not_interrupted_by_update(gateway, upstream_factory):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    store.update(make_config(u1.base_url, "u1"))
    async with await make_client() as c:
        # 发起一个慢请求（持有旧快照）
        slow = asyncio.create_task(c.get("/proxy/api/slow?ms=400"))
        await asyncio.sleep(0.1)  # 确保慢请求已在进行中
        # 切换配置到 u2
        resp = await c.put("/admin/config",
                           content=make_config(u2.base_url, "u2").model_dump_json())
        assert resp.status_code == 200
        # 新请求走新配置
        assert (await c.get("/proxy/api/echo")).json()["upstream"] == "u2"
        # 进行中的请求不受切换影响，仍由 u1 完成
        r = await slow
        assert r.status_code == 200 and r.json()["upstream"] == "u1"
