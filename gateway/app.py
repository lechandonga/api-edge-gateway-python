"""FastAPI 应用装配：配置管理接口 + 反向代理入口。"""

from __future__ import annotations

from fastapi import FastAPI

from .config import ConfigStore, GatewayConfig


def create_app(initial_config: GatewayConfig) -> FastAPI:
    """构建网关应用。"""
    app = FastAPI(title="api-edge-gateway")
    store = ConfigStore(initial_config)
    app.state.config_store = store
    return app
