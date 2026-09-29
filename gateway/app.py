"""FastAPI 应用装配：配置管理接口 + 反向代理入口。

热更新语义：
- 配置以不可变快照整体替换，新请求立即使用新快照；
- 每个请求开始时取一次快照与代理引擎引用，进行中请求沿用旧引用，
  配置切换不会中断它们；
- 非法/冲突配置在 PUT 时被拒绝，旧配置保持不变。
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .config import (ConfigRevision, ConfigStore, GatewayConfig, Route,
                     SplitRule, config_from_dict, config_to_dict)
from .errors import ErrorCode, GatewayError
from .proxy import ProxyEngine
from .ratelimit import RateLimitRegistry

logger = logging.getLogger("gateway.app")


class Gateway:
    """网关核心：持有配置存储、限流注册表与按版本缓存的代理引擎。"""

    def __init__(self, initial_config: GatewayConfig) -> None:
        self.store = ConfigStore(initial_config)
        self.rate_limits = RateLimitRegistry()
        self.rate_limits.apply_rules(initial_config.rate_limits)
        self.client = httpx.AsyncClient()
        self._engine_revision = -1
        self._engines: dict[str, ProxyEngine] = {}

    def _engines_for(self, revision: int,
                     cfg: GatewayConfig) -> dict[str, ProxyEngine]:
        """按变更修订号缓存引擎；修订号变化时为新请求构建新引擎。

        每个路由的默认上游组与每条分流规则的上游组各有一个引擎。
        旧引擎对象不被修改，进行中请求持有的引用继续有效。
        """
        if revision != self._engine_revision:
            engines: dict[str, ProxyEngine] = {}
            for r in cfg.routes:
                engines[r.name] = ProxyEngine(r, self.client)
                for rule in r.rules:
                    engines[f"{r.name}:{rule.name}"] = ProxyEngine(
                        r, self.client, group=rule.name,
                        upstreams=rule.upstreams)
            self._engines = engines
            self._engine_revision = revision
            logger.info("engines rebuilt for revision=%d groups=%s",
                        revision, list(self._engines))
        return self._engines

    def update_config(self, data: dict) -> ConfigRevision:
        """校验并原子替换配置；失败抛 GatewayError，旧配置不变。"""
        cfg = config_from_dict(data)
        rev = self.store.update(cfg)    # 校验 + 原子替换 + 记录历史
        self.rate_limits.apply_rules(cfg.rate_limits)
        logger.info("config updated revision=%d version=%d",
                    rev.revision, cfg.version)
        return rev

    def rollback(self, revision: int) -> ConfigRevision:
        """回退到指定修订号；立即生效，失败时当前配置不变。"""
        rev = self.store.rollback(revision)   # 找不到时抛错，当前配置不动
        self.rate_limits.apply_rules(rev.config.rate_limits)
        logger.info("config rolled back revision=%d -> rolled_back_to=%d",
                    rev.revision, revision)
        return rev

    @staticmethod
    def match_split_rule(route: Route, request: Request) -> SplitRule | None:
        """按配置顺序求值分流规则，返回第一个命中的规则。"""
        for rule in route.rules:
            if rule.match_kind == "header":
                actual = request.headers.get(rule.match_key)
            else:
                actual = request.query_params.get(rule.match_key)
            if actual is not None and actual == rule.match_value:
                return rule
        return None

    @staticmethod
    def match_route(cfg: GatewayConfig, path: str) -> Route | None:
        """最长前缀匹配。"""
        best: Route | None = None
        for r in cfg.routes:
            if path.startswith(r.path_prefix):
                if best is None or len(r.path_prefix) > len(best.path_prefix):
                    best = r
        return best

    async def handle(self, request: Request) -> Response:
        cfg = self.store.get()                      # 请求开始时取快照
        revision = self.store.current_revision()
        engines = self._engines_for(revision, cfg)
        path = "/" + request.path_params.get("path", "")
        if request.url.query:
            path += "?" + request.url.query
        caller = request.headers.get("x-caller-id", "anonymous")

        route = self.match_route(cfg, path)
        if route is None:
            raise GatewayError(ErrorCode.ROUTE_NOT_FOUND,
                               f"无匹配路由: {path}", 404)

        # 分流：命中规则走规则的上游组，否则走路由默认上游组
        split = self.match_split_rule(route, request)
        group = split.name if split else route.name
        engine_key = f"{route.name}:{split.name}" if split else route.name
        if split:
            logger.info("split rule hit route=%s rule=%s match=%s %s=%s",
                        route.name, split.name, split.match_kind,
                        split.match_key, split.match_value)

        decision = self.rate_limits.check(caller, time.monotonic())
        logger.info("request %s %s caller=%s route=%s ratelimit=%s "
                    "remaining_burst=%.2f remaining_quota=%s",
                    request.method, path, caller, route.name, decision.reason,
                    decision.remaining_burst, decision.remaining_quota)
        if not decision.allowed:
            code = (ErrorCode.RATE_LIMITED if decision.reason == "rate_limited"
                    else ErrorCode.QUOTA_EXHAUSTED)
            raise GatewayError(code,
                               f"调用方 {caller} {decision.reason}", 429,
                               retry_after=decision.retry_after,
                               details={
                                   "remaining_burst": round(
                                       decision.remaining_burst, 3),
                                   "remaining_quota": decision.remaining_quota,
                               })

        body = await request.body()
        headers = {k: v for k, v in request.headers.items()
                   if k.lower() not in ("host", "content-length")}
        engine = engines[engine_key]                # 本次请求固定使用该引擎
        upstream_resp = await engine.forward(request.method, path, headers, body)
        resp_headers = {
            "x-upstream": upstream_resp.headers.get("x-upstream", ""),
            "x-upstream-group": group,
            "x-ratelimit-remaining-burst": f"{decision.remaining_burst:.3f}",
            "content-type": upstream_resp.headers.get(
                "content-type", "application/octet-stream"),
        }
        if decision.remaining_quota is not None:
            resp_headers["x-ratelimit-remaining-quota"] = str(
                decision.remaining_quota)
        return Response(content=upstream_resp.content,
                        status_code=upstream_resp.status_code,
                        headers=resp_headers)

    async def close(self) -> None:
        await self.client.aclose()


def create_app(initial_config: GatewayConfig) -> FastAPI:
    """构建网关应用。"""
    gateway = Gateway(initial_config)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        await gateway.close()

    app = FastAPI(title="api-edge-gateway", lifespan=lifespan)
    app.state.gateway = gateway

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_req: Request, exc: GatewayError) -> JSONResponse:
        headers = {}
        if exc.retry_after is not None:
            headers["Retry-After"] = f"{exc.retry_after:.3f}"
        return JSONResponse(exc.to_dict(), status_code=exc.http_status,
                            headers=headers)

    @app.get("/admin/config")
    async def get_config() -> dict:
        return config_to_dict(gateway.store.get())

    @app.put("/admin/config")
    async def put_config(request: Request) -> dict:
        data = await request.json()
        rev = gateway.update_config(data)
        return {"ok": True, "revision": rev.revision,
                "version": rev.config.version}

    @app.get("/admin/config/history")
    async def get_config_history(limit: int = 20) -> dict:
        """最近的变更记录，新的在前。"""
        return {"history": [{
            "revision": r.revision,
            "version": r.config.version,
            "timestamp": r.timestamp,
            "rolled_back_to": r.rolled_back_to,
        } for r in gateway.store.history(limit)]}

    @app.post("/admin/config/rollback")
    async def rollback_config(request: Request) -> dict:
        """回退到指定修订号的配置，立即生效。"""
        data = await request.json()
        rev = gateway.rollback(int(data["revision"]))
        return {"ok": True, "revision": rev.revision,
                "rolled_back_to": rev.rolled_back_to,
                "version": rev.config.version}

    @app.get("/admin/ratelimits/{caller}")
    async def get_ratelimit_status(caller: str) -> dict:
        """调用方配额/限流状态：剩余量、最近拒绝原因、恢复时间。"""
        status = gateway.rate_limits.status(caller, time.monotonic())
        if status is None:
            return {"caller": caller, "rule": None}
        return status

    @app.get("/admin/breakers")
    async def get_breakers() -> dict:
        cfg = gateway.store.get()
        engines = gateway._engines_for(gateway.store.current_revision(), cfg)
        return {name: {u: b.state.value for u, b in eng.breakers().items()}
                for name, eng in engines.items()}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE",
                                            "HEAD", "OPTIONS", "PATCH"])
    async def proxy(request: Request) -> Response:
        return await gateway.handle(request)

    return app
