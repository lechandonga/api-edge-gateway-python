"""配额周期重置：并发撞重置不超发、状态查询可见、拒绝/放行均带剩余量。"""

import asyncio

from tests.conftest import GatewayClient, base_config


def window_cfg(up, *, rate, burst, quota, window):
    return base_config([up], rate_limits=[{
        "caller": "alice", "rate_per_second": rate, "burst": burst,
        "quota": quota, "quota_window_seconds": window}])


async def test_quota_resets_automatically_after_window(upstream_factory):
    up = upstream_factory("up")
    cfg = window_cfg(up, rate=1000, burst=1000, quota=2, window=0.3)
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        assert [ (await gw.get("/api/x", headers=headers)).status_code
                for _ in range(2)] == [200, 200]
        resp = await gw.get("/api/x", headers=headers)
        assert resp.status_code == 429
        assert resp.json()["error"]["code"] == "quota_exhausted"

        status = await gw.get("/admin/ratelimits/alice")
        body = status.json()
        assert body["remaining_quota"] == 0
        assert body["last_rejected_reason"] == "quota_exhausted"
        assert body["quota_window_seconds"] == 0.3
        assert body["quota_resets_in"] is not None

        await asyncio.sleep(0.35)  # 跨过周期边界
        # 新窗口额度完整恢复，不会把上一周期的已用量带过来
        assert [ (await gw.get("/api/x", headers=headers)).status_code
                for _ in range(2)] == [200, 200]
        status = await gw.get("/admin/ratelimits/alice")
        assert status.json()["remaining_quota"] == 0


async def test_concurrent_requests_at_window_boundary_never_over_admit(
        upstream_factory):
    up = upstream_factory("up")
    # 容量充足，只有配额约束；用两批并发分别撞重置前与重置后
    cfg = window_cfg(up, rate=1000, burst=1000, quota=10, window=0.2)
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        # 先在第一周期用掉 6 个
        first = await asyncio.gather(*[
            gw.get("/api/x", headers=headers) for _ in range(6)])
        assert sum(r.status_code == 200 for r in first) == 6

        async def fire_after(delay, count):
            async def one():
                await asyncio.sleep(delay)
                return await gw.get("/api/x", headers=headers)
            return await asyncio.gather(*[one() for _ in range(count)])

        # 重置前的并发：旧窗口只剩 4 个额度，恰好放 4 个，绝不提前借新额度
        before = await fire_after(0.1, 8)
        assert sum(r.status_code == 200 for r in before) == 4
        assert sum(r.status_code == 429 for r in before) == 4

        # 重置后的并发：新窗口整额 10 个，即使同时撞进来也恰好放 10 个
        after = await fire_after(0.15, 40)   # 累计时刻 0.25，已过边界
        admitted = sum(r.status_code == 200 for r in after)
        rejected = [r for r in after if r.status_code == 429]
        assert admitted == 10, f"重置后并发放行 {admitted} 个，期望 10"
        assert len(rejected) == 30
        # 上游实际收到 6 + 4 + 10 = 20 个，计数不丢失、不多放
        assert len(up.requests) == 20


async def test_rejected_response_carries_wait_and_remaining(upstream_factory):
    up = upstream_factory("up")
    cfg = window_cfg(up, rate=0.001, burst=1, quota=5, window=10)
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        await gw.get("/api/x", headers=headers)  # 用掉唯一突发令牌
        resp = await gw.get("/api/x", headers=headers)
        assert resp.status_code == 429
        assert resp.json()["error"]["code"] == "rate_limited"
        # 需要等待的时长（补一个令牌）与剩余量
        assert float(resp.headers["Retry-After"]) > 0
        assert resp.headers["x-ratelimit-remaining-burst"] == "0"
        assert resp.headers["x-ratelimit-remaining-quota"] == "4"
        assert resp.json()["error"]["remaining_quota"] == 4

        status = await gw.get("/admin/ratelimits/alice")
        body = status.json()
        assert body["last_rejected_reason"] == "rate_limited"
        assert body["last_rejected_seconds_ago"] is not None


async def test_allowed_response_carries_remaining_quota(upstream_factory):
    up = upstream_factory("up")
    cfg = window_cfg(up, rate=1000, burst=1000, quota=3, window=10)
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        resp = await gw.get("/api/x", headers=headers)
        assert resp.status_code == 200
        assert resp.headers["x-ratelimit-remaining-burst"] is not None
        assert resp.headers["x-ratelimit-remaining-quota"] == "2"


async def test_status_404_for_caller_without_rule(upstream_factory):
    up = upstream_factory("up")
    async with GatewayClient(base_config([up])) as gw:
        resp = await gw.get("/admin/ratelimits/nobody")
        assert resp.status_code == 404


async def test_legacy_config_without_window_field_works(upstream_factory):
    up = upstream_factory("up")
    cfg = base_config([up], rate_limits=[{
        "caller": "alice", "rate_per_second": 1000, "burst": 1000,
        "quota": 2}])  # 不带 quota_window_seconds：配额不周期重置
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 429
        await asyncio.sleep(0.1)
        # 旧语义：没有周期，等待不会恢复配额
        assert (await gw.get("/api/x", headers=headers)).status_code == 429
        status = (await gw.get("/admin/ratelimits/alice")).json()
        assert status["quota_window_seconds"] is None
        assert status["quota_resets_in"] is None
