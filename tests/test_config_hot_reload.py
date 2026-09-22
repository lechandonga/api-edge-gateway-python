"""配置热更新：原子生效、进行中请求不中断、非法/冲突配置可区分地拒绝。"""

import asyncio

from tests.conftest import GatewayClient, base_config


async def test_new_requests_use_new_config_immediately(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    cfg = base_config([u1])
    async with GatewayClient(cfg) as gw:
        assert (await gw.get("/api/x")).json()["upstream"] == "u1"
        # 原子切换到 u2
        new_cfg = base_config([u2])
        new_cfg["version"] = 2
        resp = await gw.put("/admin/config", json=new_cfg)
        assert resp.status_code == 200
        # 新请求立即采用新配置
        assert (await gw.get("/api/x")).json()["upstream"] == "u2"


async def test_inflight_request_not_interrupted_by_switch(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    u1.behavior = "slow"
    u1.slow_seconds = 0.4
    async with GatewayClient(base_config([u1])) as gw:
        # 发起一个慢请求（进行中）
        inflight = asyncio.create_task(gw.get("/api/slow"))
        await asyncio.sleep(0.1)  # 确认请求已到达上游
        # 切换配置：移除 u1，只留 u2
        new_cfg = base_config([u2])
        new_cfg["version"] = 2
        assert (await gw.put("/admin/config", json=new_cfg)).status_code == 200
        # 进行中的请求仍走旧引擎/旧上游并成功完成
        resp = await inflight
        assert resp.status_code == 200
        assert resp.json()["upstream"] == "u1"
        # 新请求走新配置
        assert (await gw.get("/api/fast")).json()["upstream"] == "u2"


async def test_invalid_config_rejected_with_distinguishable_reasons(
        upstream_factory):
    u1 = upstream_factory("u1")
    async with GatewayClient(base_config([u1])) as gw:
        # 字段非法 -> config_invalid (400)
        bad_field = base_config([u1])
        bad_field["routes"][0]["strategy"] = "no_such_strategy"
        resp = await gw.put("/admin/config", json=bad_field)
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "config_invalid"

        # 结构冲突：路由前缀互相包含 -> config_conflict (409)
        conflict = base_config([u1])
        conflict["routes"].append({
            "name": "sub", "path_prefix": "/api/v2",
            "upstreams": [{"name": "u1", "base_url": u1.base_url}],
        })
        resp = await gw.put("/admin/config", json=conflict)
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "config_conflict"

        # 旧配置保持不变，服务不受影响
        assert (await gw.get("/api/x")).status_code == 200
        current = await gw.get("/admin/config")
        assert current.json()["version"] == 1
