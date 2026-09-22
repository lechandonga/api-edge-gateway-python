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

from .config import (ConfigStore, GatewayConfig, Route, config_from_dict,
                     config_to_dict)
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
        self._engine_version = -1
        self._engines: dict[str, ProxyEngine] = {}

    def _engines_for(self, cfg: GatewayConfig) -> dict[str, ProxyEngine]:
        """按配置版本缓存引擎；版本变化时为新请求构建新引擎。

        旧引擎对象不被修改，进行中请求持有的引用继续有效。
        """
        if cfg.version != self._engine_version:
            self._engines = {r.name: ProxyEngine(r, self.client) for r in cfg.routes}
            self._engine_version = cfg.version
            logger.info("engines rebuilt for config version=%d routes=%s",
                        cfg.version, list(self._engines))
        return self._engines

    def update_config(self, data: dict) -> GatewayConfig:
        """校验并原子替换配置；失败抛 GatewayError，旧配置不变。"""
        cfg = config_from_dict(data)
        self.store.update(cfg)          # 校验 + 原子替换
        self.rate_limits.apply_rules(cfg.rate_limits)
        logger.info("config updated version=%d", cfg.version)
        return cfg

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
        engines = self._engines_for(cfg)
        path = "/" + request.path_params.get("path", "")
        if request.url.query:
            path += "?" + request.url.query
        caller = request.headers.get("x-caller-id", "anonymous")

        route = self.match_route(cfg, path)
        if route is None:
            raise GatewayError(ErrorCode.ROUTE_NOT_FOUND,
                               f"无匹配路由: {path}", 404)

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
                               retry_after=decision.retry_after)

        body = await request.body()
        headers = {k: v for k, v in request.headers.items()
                   if k.lower() not in ("host", "content-length")}
        engine = engines[route.name]                # 本次请求固定使用该引擎
        upstream_resp = await engine.forward(request.method, path, headers, body)
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers={"x-upstream": upstream_resp.headers.get("x-upstream", ""),
                     "content-type": upstream_resp.headers.get(
                         "content-type", "application/octet-stream")},
        )

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
        cfg = gateway.update_config(data)
        return {"ok": True, "version": cfg.version}

    @app.get("/admin/breakers")
    async def get_breakers() -> dict:
        cfg = gateway.store.get()
        engines = gateway._engines_for(cfg)
        return {name: {u: b.state.value for u, b in eng.breakers().items()}
                for name, eng in engines.items()}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE",
                                            "HEAD", "OPTIONS", "PATCH"])
    async def proxy(request: Request) -> Response:
        return await gateway.handle(request)

    return app
