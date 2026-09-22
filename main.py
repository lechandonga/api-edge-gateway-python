"""本地运行入口：启动网关。

用法：
    uv run python main.py                 # 使用 gateway.config.json（若存在）或内置示例
    GATEWAY_CONFIG=my.json uv run python main.py
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import uvicorn

from gateway.app import create_app
from gateway.config import config_from_dict

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")

DEFAULT_CONFIG = {
    "version": 1,
    "routes": [{
        "name": "main",
        "path_prefix": "/api",
        "strategy": "round_robin",
        "max_retries": 1,
        "upstreams": [
            {"name": "u1", "base_url": "http://127.0.0.1:9001"},
            {"name": "u2", "base_url": "http://127.0.0.1:9002"},
        ],
        "breaker": {"failure_threshold": 3, "reset_timeout": 5.0,
                    "half": 1, "timeout_seconds": 2.0},
    }],
    "rate_limits": [
        {"caller": "anonymous", "rate_per_second": 20, "burst": 40},
    ],
}


def load_config() -> dict:
    path = os.environ.get("GATEWAY_CONFIG", "gateway.config.json")
    if Path(path).exists():
        return json.loads(Path(path).read_text())
    return DEFAULT_CONFIG


app = create_app(config_from_dict(load_config()))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=18080)
