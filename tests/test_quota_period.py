"""配额周期重置与状态查询：并发下不超发、到期自动恢复、状态可见。"""

import asyncio

from tests.conftest import GatewayClient, base_config


def cfg_with_period(up, quota, period, rate=1000, burst=1000):
    return base_config([up], rate_limits=[{
        "caller": "alice", "rate_per_second": rate, "burst": burst,
        "quota": quota, "quota_period_seconds": period}])


async def test_quota_resets_after_period(upstream_factory):
    up = upstream_factory("u1")
    async with GatewayClient(cfg_with_period(up, quota=2, period=0.3)) as gw:
        headers = {"x-caller-id": "alice"}
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        resp = await gw.get("/api/x", headers=headers)
        assert resp.status_code == 429
        assert resp.json()["error"]["code"] == "quota_exhausted"
        # 拒绝响应带等待时长与剩余量
        assert resp.json()["error"]["retry_after"] > 0
        assert resp.json()["error"]["remaining_quota"] == 0
        # 周期到期后自动恢复
        await asyncio.sleep(0.4)
        assert (await gw.get("/api/x", headers=headers)).status_code == 200


async def test_period_reset_under_concurrency_never_over_issues(
        upstream_factory):
    up = upstream_factory("u1")
    async with GatewayClient(cfg_with_period(up, quota=5, period=0.3)) as gw:
        headers = {"x-caller-id": "alice"}
        # 第一个周期：并发 20 个请求，恰好放行 5 个
        results = await asyncio.gather(*[
            gw.get("/api/x", headers=headers) for _ in range(20)])
        assert sum(r.status_code == 200 for r in results) == 5
        assert sum(r.status_code == 429 for r in results) == 15
        # 等周期重置后：再并发 20 个，仍然恰好放行 5 个，不多放
        await asyncio.sleep(0.4)
        results = await asyncio.gather(*[
            gw.get("/api/x", headers=headers) for _ in range(20)])
        assert sum(r.status_code == 200 for r in results) == 5
        # 上游实际收到的不超过两个周期的配额总和
        assert len(up.requests) == 10


async def test_status_endpoint_reports_remaining_and_reject_reason(
        upstream_factory):
    up = upstream_factory("u1")
    async with GatewayClient(cfg_with_period(up, quota=2, period=10)) as gw:
        headers = {"x-caller-id": "alice"}
        resp = await gw.get("/api/x", headers=headers)
        # 放行响应带剩余量
        assert resp.headers["x-ratelimit-remaining-quota"] == "1"
        assert "x-ratelimit-remaining-burst" in resp.headers

        status = (await gw.get("/admin/ratelimits/alice")).json()
        assert status["remaining_quota"] == 1
        assert status["last_reject"] is None
        assert status["quota_reset_in"] is not None

        await gw.get("/api/x", headers=headers)
        assert (await gw.get("/api/x", headers=headers)).status_code == 429
        status = (await gw.get("/admin/ratelimits/alice")).json()
        assert status["remaining_quota"] == 0
        assert status["last_reject"] == "quota_exhausted"
        assert 0 < status["quota_reset_in"] <= 10

        # 无规则的调用方
        status = (await gw.get("/admin/ratelimits/nobody")).json()
        assert status["rule"] is None
