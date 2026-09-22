"""FastAPI 应用：代理入口 + 配置管理端点。

请求处理流程（每一步的判定依据都会写入日志）：
1. 取当前配置快照（整个请求期间持有同一快照，热更新不影响进行中的请求）
2. 限流/配额判定（RateLimiter）
3. 路由匹配（Router）
4. 负载均衡选择上游（LoadBalancer），跳过熔断打开的上游
5. 熔断器前置检查（CircuitBreaker.before_request）
6. 转发（ProxyExecutor：超时、仅幂等重试、错误归一化）
7. 按结果回报熔断器并释放在途计数
"""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from gateway.circuitbreaker import CircuitBreaker, CircuitOpenError
from gateway.config import ConfigError, ConfigStore, GatewayConfig
from gateway.proxy import GatewayError, GatewayErrorKind, ProxyExecutor
from gateway.ratelimit import Decision, RateLimiter
from gateway.routing import LoadBalancer, NoRouteError, Router

logger = logging.getLogger("gateway.app")


class _Runtime:
    """由一份配置快照构建的运行时组件；随快照一起被原子替换。"""

    def __init__(self, config: GatewayConfig, rate_state: dict):
        self.config = config
        self.router = Router(config)
        self.rate_limiter = RateLimiter(config.rate_limits, state=rate_state)
        self.load_balancers = {r.name: LoadBalancer(r) for r in config.routes}
        self.breakers = {
            u.name: CircuitBreaker(u.name, config.circuit_breaker)
            for r in config.routes for u in r.upstreams
        }


def _error_response(status: int, kind: str, detail: str,
                    extra_headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(status_code=status,
                        content={"error": kind, "detail": detail},
                        headers=extra_headers)


def create_app(store: ConfigStore | None = None,
               executor: ProxyExecutor | None = None) -> FastAPI:
    app = FastAPI(title="api-edge-gateway")
    app.state.config_store = store or ConfigStore()
    app.state.executor = executor or ProxyExecutor()
    app.state.rate_state = {}   # 限流计数跨配置热更新保持连续
    app.state.runtimes = {}     # id(config) -> _Runtime

    def runtime_for(config: GatewayConfig) -> _Runtime:
        key = id(config)
        rt = app.state.runtimes.get(key)
        if rt is None:
            # 新快照生效：重建运行时并丢弃旧快照的运行时；
            # 进行中的请求已持有旧 _Runtime 引用，不受影响。
            app.state.runtimes = {key: _Runtime(config, app.state.rate_state)}
            rt = app.state.runtimes[key]
            logger.info("runtime rebuilt for new config snapshot "
                        "(routes=%d, rate_limits=%d)",
                        len(config.routes), len(config.rate_limits))
        return rt

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/admin/config")
    async def get_config():
        return app.state.config_store.snapshot().model_dump(mode="json")

    @app.put("/admin/config")
    async def put_config(request: Request):
        try:
            cfg = GatewayConfig.model_validate(await request.json())
        except Exception as e:
            return _error_response(422, "invalid_config_schema", str(e))
        try:
            app.state.config_store.update(cfg)
        except ConfigError as e:
            logger.info("config update rejected: %s (%s)", e.reason, e.detail)
            return _error_response(409, e.reason, e.detail)
        logger.info("config updated: routes=%d rate_limits=%d",
                    len(cfg.routes), len(cfg.rate_limits))
        return {"status": "applied"}

    @app.api_route("/proxy/{path:path}",
                   methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
    async def proxy(path: str, request: Request):
        # 1. 快照：整个请求期间使用同一份配置
        rt = runtime_for(app.state.config_store.snapshot())
        caller = request.headers.get("x-caller", "anonymous")
        method = request.method
        full_path = "/" + path
        headers = {k.lower(): v for k, v in request.headers.items()}
        logger.info("request %s %s caller=%s", method, full_path, caller)

        # 2. 限流 / 配额
        rl = rt.rate_limiter.check(caller)
        logger.info("rate_limit caller=%s decision=%s remaining_quota=%s",
                    caller, rl.decision.value, rl.remaining_quota)
        if rl.decision == Decision.RATE_LIMITED:
            return _error_response(429, "rate_limited",
                               f"caller {caller} 超出速率限制",
                               {"Retry-After": f"{rl.retry_after_seconds:.3f}"})
        if rl.decision == Decision.QUOTA_EXCEEDED:
            return _error_response(429, "quota_exceeded",
                                   f"caller {caller} 配额耗尽",
                                   {"Retry-After": f"{rl.retry_after_seconds:.3f}",
                                    "X-Quota-Remaining": "0"})

        # 3. 路由匹配
        try:
            route = rt.router.match(full_path, method, headers)
        except NoRouteError as e:
            logger.info("route decision: no route for %s %s", method, full_path)
            return _error_response(404, "no_route", str(e))
        logger.info("route decision: %s %s -> route=%s strategy=%s",
                    method, full_path, route.name, route.strategy.value)

        # 4. 负载均衡（仅考虑熔断未打开的上游）
        lb = rt.load_balancers[route.name]
        healthy = [u.name for u in route.upstreams
                   if rt.breakers[u.name].state != "open"]
        if not healthy:
            logger.warning("route %s: all upstreams circuit-open", route.name)
            return _error_response(503, "circuit_open",
                                   f"route {route.name} 所有上游均已熔断")
        try:
            upstream = lb.pick(healthy)
        except Exception as e:  # pragma: no cover
            return _error_response(503, "no_healthy_upstream", str(e))
        breaker = rt.breakers[upstream.name]
        logger.info("lb decision: route=%s upstream=%s breaker=%s",
                    route.name, upstream.name, breaker.state.value)

        # 5. 熔断前置检查
        try:
            breaker.before_request()
        except CircuitOpenError as e:
            return _error_response(503, "circuit_open", str(e))

        # 6. 转发（剥离路由前缀）
        lb.acquire(upstream.name)
        body = await request.body()
        upstream_path = full_path[len(route.match.path_prefix):] or "/"
        if request.url.query:
            upstream_path += "?" + request.url.query
        try:
            resp = await app.state.executor.forward(
                upstream.base_url, method, upstream_path,
                headers, body, route.timeout_ms, route.max_retries)
        except GatewayError as e:
            breaker.on_failure()
            status = (504 if e.kind == GatewayErrorKind.UPSTREAM_TIMEOUT
                      else 502)
            logger.warning("upstream %s failed: %s (%s)",
                           upstream.name, e.kind.value, e.detail)
            return _error_response(status, e.kind.value, e.detail)
        finally:
            lb.release(upstream.name)

        # 7. 回报熔断器：5xx 视为上游失败
        if resp.status_code >= 500:
            breaker.on_failure()
            logger.warning("upstream %s returned %d -> upstream_error",
                           upstream.name, resp.status_code)
            return _error_response(502, "upstream_error",
                                   f"上游 {upstream.name} 返回 {resp.status_code}")
        breaker.on_success()
        excluded = {"content-length", "transfer-encoding", "connection"}
        resp_headers = {k: v for k, v in resp.headers.items()
                        if k.lower() not in excluded}
        if rl.remaining_quota is not None:
            resp_headers["X-Quota-Remaining"] = str(rl.remaining_quota)
        return Response(content=resp.content, status_code=resp.status_code,
                        headers=resp_headers)

    return app


app = create_app()
