"""灰度分流：按请求头/查询参数命中后端组、兜底默认组、规则优先级确定。"""

import pytest

from tests.conftest import GatewayClient


def canary_config(stable, canary, *, rules=None):
    return {
        "version": 1,
        "routes": [{
            "name": "main",
            "path_prefix": "/api",
            "upstreams": [{"name": "stable", "base_url": stable.base_url}],
            "backend_groups": [
                {"name": "canary",
                 "upstreams": [{"name": "canary", "base_url": canary.base_url}]}],
            "rules": rules if rules is not None else [{
                "name": "gray-header",
                "group": "canary",
                "conditions": [
                    {"kind": "header", "name": "x-gray", "values": ["true"]}],
            }],
            "breaker": {"failure_threshold": 3, "reset_timeout": 0.5,
                        "half": 1, "timeout_seconds": 0.5},
        }],
    }


def two_group_config(stable, canary, rules):
    cfg = canary_config(stable, canary, rules=rules)
    cfg["routes"][0]["backend_groups"].append(
        {"name": "beta",
         "upstreams": [{"name": "canary", "base_url": canary.base_url}]})
    return cfg


async def test_header_rule_routes_to_canary_and_default_fallback(upstream_factory):
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    async with GatewayClient(canary_config(stable, canary)) as gw:
        # 带灰度标识 -> 新版本组
        resp = await gw.get("/api/x", headers={"x-gray": "true"})
        assert resp.json()["upstream"] == "canary"
        assert resp.headers["x-upstream-group"] == "canary"
        assert resp.headers["x-route"] == "main"
        # 未命中任何规则 -> 默认组
        resp = await gw.get("/api/x")
        assert resp.json()["upstream"] == "stable"
        assert resp.headers["x-upstream-group"] == "default"
        # 头名大小写不敏感
        resp = await gw.get("/api/x", headers={"X-Gray": "true"})
        assert resp.json()["upstream"] == "canary"


async def test_query_param_rule_routes_to_named_group(upstream_factory):
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    cfg = canary_config(stable, canary, rules=[{
        "name": "gray-query", "group": "canary",
        "conditions": [{"kind": "query", "name": "channel",
                        "values": ["beta"]}]}])
    async with GatewayClient(cfg) as gw:
        assert (await gw.get("/api/x?channel=beta")).json()["upstream"] == "canary"
        assert (await gw.get("/api/x?channel=ga")).json()["upstream"] == "stable"
        assert (await gw.get("/api/x")).json()["upstream"] == "stable"


async def test_multi_value_condition_is_or_and_multi_condition_is_and(
        upstream_factory):
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    cfg = canary_config(stable, canary, rules=[{
        "name": "gray-combo", "group": "canary",
        "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true", "1"]},
            {"kind": "query", "name": "channel", "values": ["beta"]}]}])
    async with GatewayClient(cfg) as gw:
        resp = await gw.get("/api/x?channel=beta", headers={"x-gray": "1"})
        assert resp.json()["upstream"] == "canary"
        # 只满足一个条件：不命中
        assert (await gw.get("/api/x?channel=beta")).json()["upstream"] == "stable"
        assert (await gw.get("/api/x", headers={"x-gray": "true"})
                ).json()["upstream"] == "stable"


async def test_more_specific_rule_wins_regardless_of_declared_order(
        upstream_factory):
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    # 两条规则：宽条件 header=x-gray:true 与更具体的（再加 query 区分）。
    # 以两种声明顺序分别提交，分流结论必须一致：更具体者优先。
    broad = {"name": "broad", "group": "canary",
             "conditions": [{"kind": "header", "name": "x-gray",
                             "values": ["true"]}]}
    narrow = {"name": "narrow", "group": "default",
              "conditions": [
                  {"kind": "header", "name": "x-gray", "values": ["true"]},
                  {"kind": "query", "name": "user", "values": ["vip"]}]}
    for ordered in ([broad, narrow], [narrow, broad]):
        cfg = canary_config(stable, canary, rules=list(ordered))
        async with GatewayClient(cfg) as gw:
            assert (await gw.get("/api/x?user=vip",
                                 headers={"x-gray": "true"})
                    ).json()["upstream"] == "stable"   # 命中 narrow -> default
            assert (await gw.get("/api/x?user=other",
                                 headers={"x-gray": "true"})
                    ).json()["upstream"] == "canary"   # 只命中 broad


@pytest.mark.parametrize("rules,expected_fragment", [
    # 条件完全重复
    ([
        {"name": "r1", "group": "canary", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true"]}]},
        {"name": "r2", "group": "canary", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true"]}]}],
     "完全重复"),
    # 两条规则都能被同一请求命中（header 维度取值有交集）、互不为超集，
    # 且目标组不同：按声明顺序结果不同，无法确定谁优先
    ([
        {"name": "r1", "group": "canary", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true"]},
            {"kind": "query", "name": "a", "values": ["1"]}]},
        {"name": "r2", "group": "beta", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true"]},
            {"kind": "query", "name": "b", "values": ["1"]}]}],
     "分流结果取决于"),
    # 维度互不相交、目标组不同：同一请求（同时带两个特征）可命中两者，
    # 先后顺序决定去向
    ([
        {"name": "r1", "group": "canary", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true"]}]},
        {"name": "r2", "group": "beta", "conditions": [
            {"kind": "query", "name": "channel", "values": ["beta"]}]}],
     "分流结果取决于"),
], ids=["duplicate", "overlapping-incomparable", "disjoint-dims-diff-group"])
async def test_ambiguous_rules_rejected_at_submit(upstream_factory, rules,
                                                  expected_fragment):
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    cfg = two_group_config(stable, canary, rules)
    async with GatewayClient(canary_config(stable, canary)) as gw:
        resp = await gw.put("/admin/config", json=cfg)
        assert resp.status_code == 409
        body = resp.json()["error"]
        assert body["code"] == "config_conflict"
        assert expected_fragment in body["message"]
        # 消息指出了具体冲突的两条规则
        assert "r1" in body["message"] and "r2" in body["message"]
        # 当前生效配置未被破坏，仍按旧规则分流
        assert (await gw.get("/api/x", headers={"x-gray": "true"})
                ).json()["upstream"] == "canary"


async def test_disjoint_match_values_are_unambiguous(upstream_factory):
    """同维度匹配互斥值（true/false）：请求集不相交，声明顺序无关，允许。"""
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    rules = [
        {"name": "gray-on", "group": "canary", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true"]}]},
        {"name": "gray-off", "group": "default", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["false"]}]}]
    async with GatewayClient(two_group_config(stable, canary, rules)) as gw:
        assert (await gw.get("/api/x", headers={"x-gray": "true"})
                ).json()["upstream"] == "canary"
        assert (await gw.get("/api/x", headers={"x-gray": "false"})
                ).json()["upstream"] == "stable"
        assert (await gw.get("/api/x")).json()["upstream"] == "stable"


async def test_incomparable_rules_to_same_group_allowed(upstream_factory):
    """维度互不相交但落向同一组：即使同时命中结果也唯一，允许。"""
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    cfg = canary_config(stable, canary, rules=[
        {"name": "by-header", "group": "canary", "conditions": [
            {"kind": "header", "name": "x-gray", "values": ["true"]}]},
        {"name": "by-query", "group": "canary", "conditions": [
            {"kind": "query", "name": "channel", "values": ["beta"]}]}])
    async with GatewayClient(cfg) as gw:
        assert (await gw.get("/api/x", headers={"x-gray": "true"})
                ).json()["upstream"] == "canary"
        assert (await gw.get("/api/x?channel=beta")
                ).json()["upstream"] == "canary"
        # 同时命中两者也没有歧义
        assert (await gw.get("/api/x?channel=beta",
                             headers={"x-gray": "true"})
                ).json()["upstream"] == "canary"


async def test_rule_referencing_missing_group_rejected(upstream_factory):
    stable = upstream_factory("stable")
    canary = upstream_factory("canary")
    cfg = canary_config(stable, canary, rules=[{
        "name": "bad-ref", "group": "ghost",
        "conditions": [{"kind": "header", "name": "x-gray",
                        "values": ["true"]}]}])
    async with GatewayClient(canary_config(stable, canary)) as gw:
        resp = await gw.put("/admin/config", json=cfg)
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "config_invalid"
        assert "ghost" in resp.json()["error"]["message"]
