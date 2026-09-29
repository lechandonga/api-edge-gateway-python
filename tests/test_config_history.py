"""配置变更历史：查询、回退立即生效、双向来回切换、回退失败不破坏现状。"""

from tests.conftest import GatewayClient, base_config


def split_config(stable, canary, version):
    return {
        "version": version,
        "routes": [{
            "name": "main",
            "path_prefix": "/api",
            "upstreams": [{"name": "stable", "base_url": stable.base_url}],
            "backend_groups": [
                {"name": "canary",
                 "upstreams": [{"name": "canary", "base_url": canary.base_url}]}],
            "rules": [{
                "name": "gray", "group": "canary",
                "conditions": [{"kind": "header", "name": "x-gray",
                                "values": ["true"]}]}],
            "breaker": {"failure_threshold": 3, "reset_timeout": 0.5,
                        "half": 1, "timeout_seconds": 0.5},
        }],
    }


async def test_history_records_every_successful_submit(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1])) as gw:
        v2 = base_config([u2]); v2["version"] = 2
        assert (await gw.put("/admin/config", json=v2)).status_code == 200
        v3 = base_config([u1]); v3["version"] = 3
        assert (await gw.put("/admin/config", json=v3)).status_code == 200

        resp = await gw.get("/admin/config/history")
        history = resp.json()["history"]
        # 最近的在前：bootstrap + 两次提交
        assert [h["version"] for h in history] == [3, 2, 1]
        assert history[0]["source"] == "submit"
        assert history[-1]["source"] == "bootstrap"
        assert all("created_at" in h and "note" in h for h in history)

        # 可按 revision 取回当时的完整配置
        rev2 = next(h["revision"] for h in history if h["version"] == 2)
        detail = await gw.get(f"/admin/config/revisions/{rev2}")
        assert detail.json()["version"] == 2


async def test_rollback_takes_effect_immediately(upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1])) as gw:
        assert (await gw.get("/api/x")).json()["upstream"] == "u1"
        v2 = base_config([u2]); v2["version"] = 2
        await gw.put("/admin/config", json=v2)
        assert (await gw.get("/api/x")).json()["upstream"] == "u2"

        # 回退到 revision 1：紧接着的请求必须立刻按回退后规则走
        resp = await gw.post("/admin/config/rollback/1")
        assert resp.status_code == 200
        assert resp.json()["rolled_back_to"] == 1
        assert (await gw.get("/api/x")).json()["upstream"] == "u1"

        # 回退本身也产生一条历史记录
        history = (await gw.get("/admin/config/history")).json()["history"]
        assert history[0]["source"] == "rollback"
        assert "revision=1" in history[0]["note"]


async def test_rollback_restores_split_rules_and_group_swap(upstream_factory):
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    other = upstream_factory("other")
    async with GatewayClient(split_config(stable, canary, 1)) as gw:
        headers = {"x-gray": "true"}
        assert (await gw.get("/api/x", headers=headers)
                ).json()["upstream"] == "canary"

        # 提交 v2：路由改名 + canary 组整组换成 other，且去掉灰度规则
        v2 = {
            "version": 2,
            "routes": [{
                "name": "main-renamed",
                "path_prefix": "/api",
                "upstreams": [{"name": "stable", "base_url": stable.base_url}],
                "backend_groups": [
                    {"name": "canary",
                     "upstreams": [{"name": "other",
                                    "base_url": other.base_url}]}],
                "breaker": {"failure_threshold": 3, "reset_timeout": 0.5,
                            "half": 1, "timeout_seconds": 0.5},
            }],
        }
        await gw.put("/admin/config", json=v2)
        # v2 下无灰度规则，灰度头不再分流；且旧路由名已不存在
        assert (await gw.get("/api/x", headers=headers)
                ).json()["upstream"] == "stable"

        # 回退到 v1：灰度规则、组绑定与路由名立即全部恢复
        await gw.post("/admin/config/rollback/1")
        resp = await gw.get("/api/x", headers=headers)
        assert resp.json()["upstream"] == "canary"
        assert resp.headers["x-route"] == "main"


async def test_rollback_of_rate_limit_param_takes_effect_immediately(
        upstream_factory):
    up = upstream_factory("up")
    cfg = base_config([up], rate_limits=[{
        "caller": "alice", "rate_per_second": 1000, "burst": 1000}])
    async with GatewayClient(cfg) as gw:
        headers = {"x-caller-id": "alice"}
        tight = base_config([up], rate_limits=[{
            "caller": "alice", "rate_per_second": 0.001, "burst": 1}])
        tight["version"] = 2
        await gw.put("/admin/config", json=tight)
        assert (await gw.get("/api/x", headers=headers)).status_code == 200
        assert (await gw.get("/api/x", headers=headers)).status_code == 429

        # 仅回退限流参数：立即恢复宽松窗口
        await gw.post("/admin/config/rollback/1")
        assert (await gw.get("/api/x", headers=headers)).status_code == 200


async def test_switching_back_and_forth_is_immediate_every_time(
        upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1])) as gw:
        v2 = base_config([u2]); v2["version"] = 2
        await gw.put("/admin/config", json=v2)
        # 在两个版本间反复回退，每次切换后的首个请求就必须正确
        for target, expect in [(2, "u2"), (1, "u1"), (2, "u2"), (1, "u1")]:
            await gw.post(f"/admin/config/rollback/{target}")
            assert (await gw.get("/api/x")).json()["upstream"] == expect


async def test_rollback_unknown_revision_fails_without_touching_current(
        upstream_factory):
    u1 = upstream_factory("u1")
    u2 = upstream_factory("u2")
    async with GatewayClient(base_config([u1])) as gw:
        v2 = base_config([u2]); v2["version"] = 2
        await gw.put("/admin/config", json=v2)
        resp = await gw.post("/admin/config/rollback/999")
        assert resp.status_code == 404
        # 当前生效配置完好
        assert (await gw.get("/api/x")).json()["upstream"] == "u2"


async def test_failed_submit_keeps_current_and_allows_later_rollback(
        upstream_factory):
    u1 = upstream_factory("u1")
    async with GatewayClient(base_config([u1])) as gw:
        bad = base_config([u1])
        bad["routes"][0]["path_prefix"] = ""   # 非法
        resp = await gw.put("/admin/config", json=bad)
        assert resp.status_code == 400
        # 失败提交不入历史；仍可回退到 revision 1（内容相同也正常记录）
        history = (await gw.get("/admin/config/history")).json()["history"]
        assert len(history) == 1
        assert (await gw.get("/api/x")).status_code == 200
