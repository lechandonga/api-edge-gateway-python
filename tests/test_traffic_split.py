"""分流规则：按请求头/查询参数命中、兜底默认组、非法配置拒绝。"""

from tests.conftest import GatewayClient, base_config


def split_config(stable, gray, **kw):
    cfg = base_config([stable])
    cfg["routes"][0]["rules"] = [{
        "name": "gray",
        "match": {"header": "x-gray", "value": "1"},
        "upstreams": [{"name": gray.name, "base_url": gray.base_url}],
    }, {
        "name": "beta-query",
        "match": {"query": "track", "value": "beta"},
        "upstreams": [{"name": gray.name, "base_url": gray.base_url}],
    }]
    cfg.update(kw)
    return cfg


async def test_header_rule_hits_and_default_fallback(upstream_factory):
    stable = upstream_factory("stable")
    gray = upstream_factory("gray")
    async with GatewayClient(split_config(stable, gray)) as gw:
        # 带灰度标识 -> 灰度上游组
        resp = await gw.get("/api/x", headers={"x-gray": "1"})
        assert resp.json()["upstream"] == "gray"
        assert resp.headers["x-upstream-group"] == "gray"
        # 值不匹配 -> 默认组
        resp = await gw.get("/api/x", headers={"x-gray": "0"})
        assert resp.json()["upstream"] == "stable"
        assert resp.headers["x-upstream-group"] == "main"
        # 不带标识 -> 默认组
        resp = await gw.get("/api/x")
        assert resp.json()["upstream"] == "stable"


async def test_query_param_rule(upstream_factory):
    stable = upstream_factory("stable")
    gray = upstream_factory("gray")
    async with GatewayClient(split_config(stable, gray)) as gw:
        resp = await gw.get("/api/x?track=beta")
        assert resp.json()["upstream"] == "gray"
        resp = await gw.get("/api/x?track=prod")
        assert resp.json()["upstream"] == "stable"


async def test_rule_order_is_deterministic(upstream_factory):
    stable = upstream_factory("stable")
    gray = upstream_factory("gray")
    async with GatewayClient(split_config(stable, gray)) as gw:
        # 同时命中两条规则时，排在前面的规则生效（结果确定）
        resp = await gw.get("/api/x?track=beta", headers={"x-gray": "1"})
        assert resp.headers["x-upstream-group"] == "gray"


async def test_duplicate_match_condition_rejected(upstream_factory):
    stable = upstream_factory("stable")
    gray = upstream_factory("gray")
    cfg = split_config(stable, gray)
    cfg["routes"][0]["rules"].append({
        "name": "gray-dup",
        "match": {"header": "x-gray", "value": "1"},  # 与已有规则条件重复
        "upstreams": [{"name": gray.name, "base_url": gray.base_url}],
    })
    async with GatewayClient(split_config(stable, gray)) as gw:
        resp = await gw.put("/admin/config", json=cfg)
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "config_conflict"
        assert "重复" in resp.json()["error"]["message"]
        # 当前配置未被破坏
        assert (await gw.get("/api/x")).status_code == 200


async def test_rule_with_empty_upstreams_rejected(upstream_factory):
    stable = upstream_factory("stable")
    gray = upstream_factory("gray")
    cfg = split_config(stable, gray)
    cfg["routes"][0]["rules"][0]["upstreams"] = []
    async with GatewayClient(split_config(stable, gray)) as gw:
        resp = await gw.put("/admin/config", json=cfg)
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "config_invalid"
