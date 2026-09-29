"""变更记录与回退：历史可查、回退立即生效、可来回切换、失败不破坏当前配置。"""

import asyncio

from tests.conftest import GatewayClient, base_config


async def test_history_records_each_commit(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1])) as gw:
        cfg2 = base_config([u2])
        cfg2["version"] = 2
        resp = await gw.put("/admin/config", json=cfg2)
        assert resp.status_code == 200
        rev2 = resp.json()["revision"]

        history = (await gw.get("/admin/config/history")).json()["history"]
        assert len(history) == 2          # 初始 + 一次提交
        assert history[0]["revision"] == rev2   # 新的在前
        assert history[0]["version"] == 2
        assert history[1]["version"] == 1
        assert all(h["timestamp"] > 0 for h in history)


async def test_rollback_takes_effect_immediately(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1])) as gw:
        cfg2 = base_config([u2])
        cfg2["version"] = 2
        await gw.put("/admin/config", json=cfg2)
        assert (await gw.get("/api/x")).json()["upstream"] == "u2"

        # 回退到 revision=1（u1），紧接着的请求立即走 u1
        resp = await gw.post("/admin/config/rollback", json={"revision": 1})
        assert resp.status_code == 200
        assert resp.json()["rolled_back_to"] == 1
        assert (await gw.get("/api/x")).json()["upstream"] == "u1"


async def test_switch_back_and_forth_between_revisions(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1])) as gw:
        cfg2 = base_config([u2])
        cfg2["version"] = 2
        rev2 = (await gw.put("/admin/config", json=cfg2)).json()["revision"]

        # 在两个版本之间来回切，每次切完立即生效
        for _ in range(2):
            await gw.post("/admin/config/rollback", json={"revision": rev2})
            assert (await gw.get("/api/x")).json()["upstream"] == "u2"
            await gw.post("/admin/config/rollback", json={"revision": 1})
            assert (await gw.get("/api/x")).json()["upstream"] == "u1"

        # 每次切换都留有记录
        history = (await gw.get("/admin/config/history")).json()["history"]
        assert len(history) == 2 + 4
        assert history[0]["rolled_back_to"] == 1


async def test_rollback_restores_rate_limit_params(upstream_factory):
    u1 = upstream_factory("u1")
    cfg1 = base_config([u1], rate_limits=[
        {"caller": "alice", "rate_per_second": 1000, "burst": 1000,
         "quota": 1}])
    async with GatewayClient(cfg1) as gw:
        headers = {"x-caller-id": "alice"}
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 429

        # 提交只调限流参数的新配置（配额放宽）
        cfg2 = base_config([u1], rate_limits=[
            {"caller": "alice", "rate_per_second": 1000, "burst": 1000,
             "quota": 100}])
        cfg2["version"] = 2
        await gw.put("/admin/config", json=cfg2)
        assert (await gw.get("/api/x", headers=headers)).status_code == 200

        # 回退到严格配额：立即按 quota=1 生效（放行 1 个后即 429）
        await gw.post("/admin/config/rollback", json={"revision": 1})
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 429


async def test_rollback_unknown_revision_keeps_current_config(upstream_factory):
    u1 = upstream_factory("u1")
    async with GatewayClient(base_config([u1])) as gw:
        resp = await gw.post("/admin/config/rollback", json={"revision": 999})
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "revision_not_found"
        # 当前配置不受影响
        assert (await gw.get("/api/x")).json()["upstream"] == "u1"


async def test_inflight_request_not_interrupted_by_rollback(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    u1.behavior = "slow"
    u1.slow_seconds = 0.4
    async with GatewayClient(base_config([u1])) as gw:
        cfg2 = base_config([u2])
        cfg2["version"] = 2
        await gw.put("/admin/config", json=cfg2)
        # 制造一个仍走 u1 的慢请求：先回退到 u1，发起慢请求，再切回 u2
        await gw.post("/admin/config/rollback", json={"revision": 1})
        inflight = asyncio.create_task(gw.get("/api/slow"))
        await asyncio.sleep(0.1)
        await gw.post("/admin/config/rollback", json={"revision": 2})
        resp = await inflight
        assert resp.status_code == 200
        assert resp.json()["upstream"] == "u1"   # 进行中请求未被打断
        assert (await gw.get("/api/fast")).json()["upstream"] == "u2"


async def test_old_config_without_new_fields_still_works(upstream_factory):
    u1 = upstream_factory("u1")
    # 老配置：无 rules / quota_period_seconds 字段
    cfg = {
        "version": 1,
        "routes": [{
            "name": "main", "path_prefix": "/api",
            "upstreams": [{"name": "u1", "base_url": u1.base_url}],
        }],
        "rate_limits": [{"caller": "alice", "rate_per_second": 10,
                         "burst": 5}],
    }
    async with GatewayClient(cfg) as gw:
        assert (await gw.get("/api/x")).status_code == 200
        cfg["version"] = 2
        assert (await gw.put("/admin/config", json=cfg)).status_code == 200
        assert (await gw.get("/api/x")).status_code == 200
