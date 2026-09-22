"""本地演示上游：提供可预期行为的端点，用于验证网关分发/限流/熔断。

用法: uv run python upstream_demo.py <port> [--name NAME]
端点:
  GET  /echo        返回实例名与请求路径
  GET  /slow?ms=    延迟指定毫秒后返回（验证超时）
  GET  /flaky       前 fail_times 次返回 500，之后返回 200（验证熔断与恢复）
  ANY  /count       命中计数 +1 并返回当前计数（验证重试不产生重复副作用）
  POST /flaky/reset 重置 flaky 计数
"""
from __future__ import annotations

import argparse
import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn


def create_upstream(name: str) -> FastAPI:
    app = FastAPI(title=f"upstream-{name}")
    state = {"flaky_calls": 0, "fail_times": 3, "count": 0}

    @app.get("/echo")
    async def echo(request: Request):
        return {"upstream": name, "path": str(request.url.path)}

    @app.get("/slow")
    async def slow(ms: int = 500):
        await asyncio.sleep(ms / 1000.0)
        return {"upstream": name, "slept_ms": ms}

    @app.get("/flaky")
    async def flaky():
        state["flaky_calls"] += 1
        if state["flaky_calls"] <= state["fail_times"]:
            return JSONResponse({"upstream": name, "error": "flaky"},
                                status_code=500)
        return {"upstream": name, "ok": True, "calls": state["flaky_calls"]}

    @app.post("/flaky/reset")
    async def flaky_reset(fail_times: int = 3):
        state["flaky_calls"] = 0
        state["fail_times"] = fail_times
        return {"reset": True}

    @app.api_route("/count", methods=["GET", "POST"])
    async def count():
        state["count"] += 1
        return {"upstream": name, "count": state["count"]}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int)
    parser.add_argument("--name", default=None)
    args = parser.parse_args()
    uvicorn.run(create_upstream(args.name or str(args.port)),
                host="127.0.0.1", port=args.port, log_level="warning")
