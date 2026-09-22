"""限流与配额：并发下计数准确、拒绝原因可区分、窗口/配额恢复后放行。"""

import asyncio

from tests.conftest import GatewayClient, base_config


def cfg_with_limit(up, rate, burst, quota=None):
    cfg = base_config([up], rate_limits=[{
        "caller": "alice", "rate_per_second": rate,
        "burst": burst, "quota": quota}])
    return cfg


async def test_concurrent_requests_never_exceed_burst(upstream_factory):
    up = upstream_factory("u1")
    # 速率极低：窗口内只能靠 burst 放行
    cfg = cfg_with_limit(up, rate=0.001, burst=10)
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        results = await asyncio.gather(*[
            gw.get("/api/x", headers=headers) for _ in range(50)])
        ok = [r for r in results if r.status_code == 200]
        limited = [r for r in results if r.status_code == 429]
        # 并发 50 个请求，恰好放行 10 个，不超发
        assert len(ok) == 10, f"放行 {len(ok)} 个，期望 10"
        assert len(limited) == 40
        assert all(r.json()["error"]["code"] == "rate_limited" for r in limited)
        assert all("Retry-After" in r.headers for r in limited)
        # 上游实际收到的请求数与放行数一致，计数不丢失
        assert len(up.requests) == 10


async def test_quota_exhausted_distinguishable_from_rate_limit(upstream_factory):
    up = upstream_factory("u1")
    # 速率充足但配额只有 3
    cfg = cfg_with_limit(up, rate=1000, burst=1000, quota=3)
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        codes = []
        for _ in range(5):
            resp = await gw.get("/api/x", headers=headers)
            codes.append(resp.status_code)
        assert codes == [200, 200, 200, 429, 429]
        resp = await gw.get("/api/x", headers=headers)
        assert resp.json()["error"]["code"] == "quota_exhausted"
        # 其他调用方不受 alice 配额影响
        assert (await gw.get("/api/x",
                             headers={"x-caller-id": "bob"})).status_code == 200


async def test_window_recovery_allows_requests_again(upstream_factory):
    up = upstream_factory("u1")
    cfg = cfg_with_limit(up, rate=5, burst=1)  # 每秒补 5 个令牌
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 429
        await asyncio.sleep(0.3)  # 窗口恢复
        assert (await gw.get("/api/x", headers=headers)).status_code == 200


async def test_quota_recovers_after_config_update(upstream_factory):
    up = upstream_factory("u1")
    cfg = cfg_with_limit(up, rate=1000, burst=1000, quota=2)
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        resp = await gw.get("/api/x", headers=headers)
        assert resp.json()["error"]["code"] == "quota_exhausted"
        # 配置更新：配额提升，恢复放行
        new_cfg = cfg_with_limit(up, rate=1000, burst=1000, quota=10)
        new_cfg["version"] = 2
        assert (await gw.put("/admin/config", json=new_cfg)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
