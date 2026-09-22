"""熔断：closed/open/half_open 状态转换、恢复、重试不重复副作用、错误归一化。"""
import asyncio

import httpx
import pytest

from gateway.config import (
    CircuitBreakerConfig, GatewayConfig, RouteConfig, RouteMatch, UpstreamConfig,
)
from gateway.proxy import GatewayError, GatewayErrorKind, ProxyExecutor


def make_config(upstream_url: str, **cb_kw) -> GatewayConfig:
    cb = {"failure_threshold": 2, "open_seconds": 0.3, "half_open_max_probes": 1}
    cb.update(cb_kw)
    return GatewayConfig(
        routes=[RouteConfig(
            name="api", match=RouteMatch(path_prefix="/api"),
            upstreams=[UpstreamConfig(name="u1", base_url=upstream_url)],
            timeout_ms=500, max_retries=0,
        )],
        circuit_breaker=CircuitBreakerConfig(**cb),
    )


async def test_breaker_opens_and_recovers(gateway, upstream_factory, caplog):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(make_config(u1.base_url))
    async with await make_client() as c, httpx.AsyncClient() as direct:
        # 上游连续失败 2 次 -> 熔断打开
        await direct.post(f"{u1.base_url}/flaky/reset?fail_times=2")
        for _ in range(2):
            r = await c.get("/proxy/api/flaky")
            assert r.status_code == 502 and r.json()["error"] == "upstream_error"
        # 熔断打开：直接拒绝，不再打到上游
        r = await c.get("/proxy/api/flaky")
        assert r.status_code == 503 and r.json()["error"] == "circuit_open"
        # open_seconds 后进入半开，探测成功 -> 关闭并恢复
        await asyncio.sleep(0.35)
        r = await c.get("/proxy/api/flaky")
        assert r.status_code == 200 and r.json()["ok"] is True
        assert (await c.get("/proxy/api/echo")).status_code == 200
    # 日志中可见熔断状态变化
    text = caplog.text
    assert "closed -> open" in text and "open -> half_open" in text
    assert "half_open -> closed" in text


async def test_half_open_probe_failure_reopens(gateway, upstream_factory, caplog):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(make_config(u1.base_url))
    async with await make_client() as c, httpx.AsyncClient() as direct:
        await direct.post(f"{u1.base_url}/flaky/reset?fail_times=10")
        for _ in range(2):
            await c.get("/proxy/api/flaky")
        assert (await c.get("/proxy/api/flaky")).status_code == 503
        await asyncio.sleep(0.35)
        # 半开探测仍失败 -> 重新打开
        r = await c.get("/proxy/api/flaky")
        assert r.status_code == 502
        r = await c.get("/proxy/api/flaky")
        assert r.status_code == 503 and r.json()["error"] == "circuit_open"
    assert "half_open -> open" in caplog.text


async def test_timeout_normalized_and_trips_breaker(gateway, upstream_factory):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(make_config(u1.base_url))
    async with await make_client() as c:
        for _ in range(2):
            r = await c.get("/proxy/api/slow?ms=1000")  # timeout_ms=500
            assert r.status_code == 504
            assert r.json()["error"] == "upstream_timeout"
        # 超时计入连续失败，熔断打开
        r = await c.get("/proxy/api/echo")
        assert r.status_code == 503 and r.json()["error"] == "circuit_open"


async def test_post_not_retried_no_duplicate_side_effect(gateway, upstream_factory):
    """POST 非幂等：即使配置了重试也不得重放，上游计数必须恰好为 1。"""
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    cfg = make_config(u1.base_url)
    cfg.routes[0].max_retries = 3
    store.update(cfg)
    async with await make_client() as c:
        r = await c.post("/proxy/api/count")
        assert r.status_code == 200 and r.json()["count"] == 1
        # 直接查上游确认只命中一次
        assert (await c.post("/proxy/api/count")).json()["count"] == 2


async def test_retry_only_on_transport_error_for_idempotent():
    """幂等方法在传输错误时重试；非幂等方法不重试。"""
    calls = {"n": 0}

    def flaky_transport(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, json={"ok": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(flaky_transport))
    ex = ProxyExecutor(client)
    resp = await ex.forward("http://up", "GET", "/x", {}, b"", 1000, max_retries=2)
    assert resp.status_code == 200 and calls["n"] == 2  # GET 重试成功

    calls["n"] = 0
    with pytest.raises(GatewayError) as ei:
        await ex.forward("http://up", "POST", "/x", {}, b"", 1000, max_retries=2)
    assert ei.value.kind == GatewayErrorKind.UPSTREAM_UNAVAILABLE
    assert calls["n"] == 1  # POST 不重试
