"""限流与配额：并发计数准确、拒绝原因可区分、窗口恢复后放行。"""
import asyncio

from gateway.config import (
    GatewayConfig, RateLimitConfig, RouteConfig, RouteMatch, UpstreamConfig,
)


def make_config(upstream_url: str, rl: RateLimitConfig) -> GatewayConfig:
    return GatewayConfig(
        routes=[RouteConfig(
            name="api", match=RouteMatch(path_prefix="/api"),
            upstreams=[UpstreamConfig(name="u1", base_url=upstream_url)],
        )],
        rate_limits=[rl],
    )


async def test_concurrent_rate_limit_counts_exactly(gateway, upstream_factory):
    """burst=20、rate 极低时，100 个并发请求必须恰好放行 20 个。"""
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(make_config(u1.base_url, RateLimitConfig(
        caller="c1", rate_per_second=0.001, burst=20)))
    async with await make_client() as c:
        results = await asyncio.gather(*[
            c.get("/proxy/api/echo", headers={"x-caller": "c1"})
            for _ in range(100)
        ])
    ok = [r for r in results if r.status_code == 200]
    limited = [r for r in results if r.status_code == 429]
    assert len(ok) == 20, f"allowed={len(ok)}"
    assert len(limited) == 80
    assert all(r.json()["error"] == "rate_limited" for r in limited)
    assert all("Retry-After" in r.headers for r in limited)


async def test_quota_exceeded_is_distinguishable(gateway, upstream_factory):
    """速率足够但配额耗尽时，拒绝原因必须是 quota_exceeded。"""
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(make_config(u1.base_url, RateLimitConfig(
        caller="c1", rate_per_second=1000, burst=1000,
        quota=5, quota_window_seconds=60)))
    async with await make_client() as c:
        results = [await c.get("/proxy/api/echo", headers={"x-caller": "c1"})
                   for _ in range(7)]
    assert [r.status_code for r in results] == [200]*5 + [429]*2
    assert results[-1].json()["error"] == "quota_exceeded"
    assert results[-1].headers["X-Quota-Remaining"] == "0"
    # 放行响应携带剩余配额提示
    assert results[0].headers["X-Quota-Remaining"] == "4"


async def test_recovers_after_window(gateway, upstream_factory):
    """令牌补充 / 配额窗口重置后应能正常放行。"""
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(make_config(u1.base_url, RateLimitConfig(
        caller="c1", rate_per_second=5, burst=2,
        quota=2, quota_window_seconds=0.4)))
    async with await make_client() as c:
        h = {"x-caller": "c1"}
        assert (await c.get("/proxy/api/echo", headers=h)).status_code == 200
        assert (await c.get("/proxy/api/echo", headers=h)).status_code == 200
        r = await c.get("/proxy/api/echo", headers=h)
        assert r.status_code == 429 and r.json()["error"] == "quota_exceeded"
        await asyncio.sleep(0.5)  # 等待配额窗口重置
        assert (await c.get("/proxy/api/echo", headers=h)).status_code == 200


async def test_unconfigured_caller_not_limited(gateway, upstream_factory):
    app, store, make_client = gateway
    u1 = upstream_factory("u1")
    store.update(make_config(u1.base_url, RateLimitConfig(
        caller="c1", rate_per_second=0.001, burst=1)))
    async with await make_client() as c:
        for _ in range(5):
            r = await c.get("/proxy/api/echo", headers={"x-caller": "other"})
            assert r.status_code == 200
