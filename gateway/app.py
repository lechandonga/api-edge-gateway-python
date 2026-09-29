"""FastAPI 应用装配：配置管理（含历史/回退）+ 灰度分流代理入口。

热更新与回退语义：
- 配置以不可变快照整体替换，新请求在入口取一次快照，立即按新规则分流；
- 代理引擎按配置“内容指纹”缓存并按 (路由, 后端组) 构建，
  进行中请求持有自己的引擎引用；回退到旧内容时复用旧引擎，
  来回切换也不会出现“提交成功仍按老规则转发”的中间态；
- 非法/冲突配置在提交时被拒绝，当前生效配置保持不变。
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager
from urllib.parse import parse_qs

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .config import (ConfigHistory, ConfigRevision, ConfigStore, GatewayConfig,
                     Route, config_fingerprint, config_from_dict, config_to_dict)
from .errors import ErrorCode, GatewayError
from .proxy import ProxyEngine
from .ratelimit import RateDecision, RateLimitRegistry

logger = logging.getLogger("gateway.app")


class Gateway:
    """网关核心：配置存储、变更历史、限流注册表与按内容缓存的代理引擎。"""

    def __init__(self, initial_config: GatewayConfig) -> None:
        now = time.time()
        self.store = ConfigStore(initial_config)
        self.history = ConfigHistory(ConfigRevision(
            revision=1, config=initial_config, created_at=now,
            source="bootstrap", note="启动时加载的初始配置"))
        self.rate_limits = RateLimitRegistry()
        self.rate_limits.apply_rules(initial_config.rate_limits)
        self.client = httpx.AsyncClient()
        self._next_revision = 2
        self._engine_lock = threading.Lock()
        # 内容指纹 -> {路由名: {后端组名: ProxyEngine}}
        self._engines: dict[str, dict[str, dict[str, ProxyEngine]]] = {}

    # ---- 配置变更 ----

    def _build_engines(
            self, cfg: GatewayConfig) -> dict[str, dict[str, ProxyEngine]]:
        """按配置内容指纹获取/构建 (路由 -> 后端组 -> 引擎) 表。"""
        fingerprint = config_fingerprint(cfg)
        with self._engine_lock:
            cached = self._engines.get(fingerprint)
            if cached is None:
                cached = {}
                for route in cfg.routes:
                    cached[route.name] = {
                        group_name: ProxyEngine(
                            route.name, group, route.breaker, self.client)
                        for group_name, group in route.groups().items()}
                self._engines[fingerprint] = cached
                logger.info(
                    "engines built config_fingerprint=%s routes=%s",
                    fingerprint[:12],
                    {r.name: list(cached[r.name]) for r in cfg.routes})
            return cached

    def _commit(self, cfg: GatewayConfig, source: str, note: str) -> ConfigRevision:
        """校验、原子替换并写入变更历史；任一步失败都不破坏当前配置。"""
        self.store.update(cfg)            # 校验失败直接抛出，旧配置不变
        revision = ConfigRevision(
            revision=self._next_revision, config=cfg,
            created_at=time.time(), source=source, note=note)
        self._next_revision += 1
        self.history.append(revision)
        self.rate_limits.apply_rules(cfg.rate_limits)
        self._build_engines(cfg)          # 提前构建，首个请求无需等待
        logger.info("config committed revision=%d source=%s version=%d note=%s",
                    revision.revision, source, cfg.version, note)
        return revision

    def submit_config(self, data: dict) -> ConfigRevision:
        """提交一份新配置（PUT）。"""
        cfg = config_from_dict(data)
        return self._commit(cfg, "submit", "通过 PUT /admin/config 提交")

    def rollback(self, revision: int) -> ConfigRevision:
        """回退到历史中的任意一次配置，回退本身也产生一条新记录。"""
        target = self.history.get(revision)
        if target is None:
            raise GatewayError(
                ErrorCode.CONFIG_INVALID,
                f"变更记录不存在: revision={revision}", 404)
        # 历史配置在写入时已校验通过；原子替换 + 记录在同一流程内完成，
        # 失败不会发生在替换之后，当前配置不会被破坏。
        return self._commit(
            target.config, "rollback",
            f"回退到 revision={revision}（当时 version={target.config.version}）")

    # ---- 请求处理 ----

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
        cfg = self.store.get()                      # 请求入口取一次快照
        engines = self._build_engines(cfg)
        raw_path = "/" + request.path_params.get("path", "")
        query_string = request.url.query
        forward_path = raw_path + ("?" + query_string if query_string else "")
        caller = request.headers.get("x-caller-id", "anonymous")
        headers = {k: v for k, v in request.headers.items()
                   if k.lower() not in ("host", "content-length")}
        query = parse_qs(query_string, keep_blank_values=True)

        route = self.match_route(cfg, raw_path)
        if route is None:
            raise GatewayError(ErrorCode.ROUTE_NOT_FOUND,
                               f"无匹配路由: {raw_path}", 404)

        group_name = route.match_group(headers, query)

        now_mono = time.monotonic()
        decision = self.rate_limits.check(caller, now_mono)
        matched_rule = next(
            (r.name for r in route.ordered_rules()
             if r.matches(headers, query)), None)
        logger.info(
            "request %s %s caller=%s route=%s matched_rule=%s group=%s "
            "ratelimit=%s remaining_burst=%.2f remaining_quota=%s",
            request.method, raw_path, caller, route.name, matched_rule,
            group_name, decision.reason, decision.remaining_burst,
            decision.remaining_quota)
        if not decision.allowed:
            code = (ErrorCode.RATE_LIMITED if decision.reason == "rate_limited"
                    else ErrorCode.QUOTA_EXHAUSTED)
            raise GatewayError(code, f"调用方 {caller} {decision.reason}", 429,
                               retry_after=decision.retry_after,
                               rate_decision=decision)

        body = await request.body()
        engine = engines[route.name][group_name]    # 本次请求固定该引擎
        upstream_resp = await engine.forward(
            request.method, forward_path, headers, body)
        resp_headers = {
            "x-upstream": upstream_resp.headers.get("x-upstream", ""),
            "x-route": route.name,
            "x-upstream-group": group_name,
            "x-ratelimit-remaining-burst": f"{decision.remaining_burst:.0f}",
            "content-type": upstream_resp.headers.get(
                "content-type", "application/octet-stream"),
        }
        if decision.remaining_quota is not None:
            resp_headers["x-ratelimit-remaining-quota"] = str(
                decision.remaining_quota)
        if decision.quota_reset_at is not None:
            resp_headers["x-quota-resets-in"] = (
                f"{max(0.0, decision.quota_reset_at - now_mono):.3f}")
        return Response(
            content=upstream_resp.content,
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
        body = exc.to_dict()
        headers = {}
        if exc.retry_after is not None:
            headers["Retry-After"] = f"{max(0.0, exc.retry_after):.3f}"
        decision = getattr(exc, "rate_decision", None)
        if decision is not None:
            err = body["error"]
            err["remaining_burst"] = int(decision.remaining_burst)
            if decision.remaining_quota is not None:
                err["remaining_quota"] = decision.remaining_quota
            if decision.quota_reset_at is not None:
                err["quota_resets_in"] = round(
                    max(0.0, decision.quota_reset_at - time.monotonic()), 3)
                headers["x-quota-resets-in"] = (
                    f"{err['quota_resets_in']:.3f}")
            headers["x-ratelimit-remaining-burst"] = str(
                int(decision.remaining_burst))
            if decision.remaining_quota is not None:
                headers["x-ratelimit-remaining-quota"] = str(
                    decision.remaining_quota)
        return JSONResponse(body, status_code=exc.http_status, headers=headers)

    @app.get("/admin/config")
    async def get_config() -> dict:
        return config_to_dict(gateway.store.get())

    @app.put("/admin/config")
    async def put_config(request: Request) -> dict:
        data = await request.json()
        revision = gateway.submit_config(data)
        return {"ok": True, "revision": revision.revision,
                "version": revision.config.version}

    @app.get("/admin/config/history")
    async def get_history(limit: int = 20) -> dict:
        items = [{
            "revision": r.revision,
            "version": r.config.version,
            "created_at": r.created_at,
            "source": r.source,
            "note": r.note,
        } for r in gateway.history.list(limit=limit)]
        return {"history": items}

    @app.get("/admin/config/revisions/{revision}")
    async def get_revision(revision: int) -> dict:
        record = gateway.history.get(revision)
        if record is None:
            raise GatewayError(ErrorCode.CONFIG_INVALID,
                               f"变更记录不存在: revision={revision}", 404)
        return config_to_dict(record.config)

    @app.post("/admin/config/rollback/{revision}")
    async def rollback(revision: int) -> dict:
        record = gateway.rollback(revision)
        return {"ok": True, "revision": record.revision,
                "rolled_back_to": revision,
                "version": record.config.version}

    @app.get("/admin/ratelimits/{caller}")
    async def get_ratelimit_status(caller: str) -> dict:
        status = gateway.rate_limits.status(caller, time.monotonic())
        if status is None:
            raise GatewayError(
                ErrorCode.CONFIG_INVALID,
                f"调用方 {caller} 没有限流规则", 404)
        return {
            "caller": status.caller,
            "rate_per_second": status.rate_per_second,
            "burst": status.burst,
            "remaining_burst": status.remaining_burst,
            "quota": status.quota,
            "remaining_quota": status.remaining_quota,
            "quota_window_seconds": status.quota_window_seconds,
            "quota_resets_in": status.quota_resets_in,
            "last_rejected_reason": status.last_rejected_reason,
            "last_rejected_seconds_ago": (
                round(status.last_rejected_at, 3)
                if status.last_rejected_at is not None else None),
            "last_rejected_retry_after_was": status.last_rejected_retry_after,
        }

    @app.get("/admin/breakers")
    async def get_breakers() -> dict:
        cfg = gateway.store.get()
        engines = gateway._build_engines(cfg)
        result: dict = {}
        for route in cfg.routes:
            group_engines = engines[route.name]
            if not route.backend_groups:
                # 老配置形态（只有默认组）：保持 {路由: {上游: 状态}}
                default_eng = group_engines[route.default_group]
                result[route.name] = {
                    u: b.state.value for u, b in default_eng.breakers().items()}
            else:
                result[route.name] = {
                    group_name: {u: b.state.value
                                 for u, b in eng.breakers().items()}
                    for group_name, eng in group_engines.items()}
        return result

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE",
                                            "HEAD", "OPTIONS", "PATCH"])
    async def proxy(request: Request) -> Response:
        return await gateway.handle(request)

    return app
