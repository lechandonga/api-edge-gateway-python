"""熔断与重试：状态转换、上游异常归一化、重试不产生重复副作用。"""

import asyncio

from tests.conftest import GatewayClient, base_config


async def test_breaker_opens_after_consecutive_failures_and_recovers(
        upstream_factory):
    up = upstream_factory("u1")
    up.behavior = "fail500"
    cfg = base_config([up], breaker={
        "failure_threshold": 3, "reset_timeout": 0.3,
        "half": 1, "timeout_seconds": 0.5})
    async with GatewayClient(cfg) as gw:
        # 连续失败达到阈值：每次都归一化为 upstream_unavailable
        for _ in range(3):
            resp = await gw.get("/api/x")
            assert resp.status_code == 502
            assert resp.json()["error"]["code"] == "upstream_unavailable"
        assert (await gw.get("/admin/breakers")).json()["main"]["u1"] == "open"

        # 熔断打开：快速失败，不再打到上游
        before = len(up.requests)
        resp = await gw.get("/api/x")
        assert resp.status_code == 503
        assert resp.json()["error"]["code"] == "circuit_open"
        assert "Retry-After" in resp.headers
        assert len(up.requests) == before

        # 等待 reset_timeout 后进入半开；上游恢复，探测成功则关闭
        up.behavior = "ok"
        await asyncio.sleep(0.4)
        resp = await gw.get("/api/x")
        assert resp.status_code == 200
        assert (await gw.get("/admin/breakers")).json()["main"]["u1"] == "closed"


async def test_half_open_probe_failure_reopens(upstream_factory):
    up = upstream_factory("u1")
    up.behavior = "fail500"
    cfg = base_config([up], breaker={
        "failure_threshold": 2, "reset_timeout": 0.3,
        "half": 1, "timeout_seconds": 0.5})
    async with GatewayClient(cfg) as gw:
        for _ in range(2):
            await gw.get("/api/x")
        assert (await gw.get("/admin/breakers")).json()["main"]["u1"] == "open"
        await asyncio.sleep(0.4)
        # 半开探测仍然失败 -> 重新打开
        resp = await gw.get("/api/x")
        assert resp.status_code == 502
        assert (await gw.get("/admin/breakers")).json()["main"]["u1"] == "open"


async def test_upstream_timeout_normalized(upstream_factory):
    up = upstream_factory("u1")
    up.behavior = "slow"
    up.slow_seconds = 1.0
    cfg = base_config([up], breaker={
        "failure_threshold": 5, "reset_timeout": 1.0,
        "half": 1, "timeout_seconds": 0.2})
    async with GatewayClient(cfg) as gw:
        resp = await gw.get("/api/x")
        assert resp.status_code == 504
        assert resp.json()["error"]["code"] == "upstream_timeout"


async def test_retry_only_for_idempotent_methods(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    u1.behavior = "fail500"
    cfg = base_config([u1, u2], max_retries=2)
    async with GatewayClient(cfg) as gw:
        # GET 幂等：u1 失败后重试到 u2 成功
        resp = await gw.get("/api/x")
        assert resp.status_code == 200
        assert resp.json()["upstream"] == "u2"
        assert len(u1.requests) == 1 and len(u2.requests) == 1

        # POST 非幂等：失败不重试，u1 只收到 1 个请求，u2 收不到
        # -> 不会产生重复副作用
        u1_failures_before = len(u1.requests)
        resp = await gw.post("/api/order", content="order-1")
        assert resp.status_code == 502
        assert resp.json()["error"]["code"] == "upstream_unavailable"
        assert len(u1.requests) == u1_failures_before + 1
        assert len(u2.requests) == 1  # 没有新增
        assert [r["body"] for r in u1.requests if r["method"] == "POST"] == ["order-1"]
