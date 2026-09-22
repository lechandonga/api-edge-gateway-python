"""网关对外错误模型：所有上游/内部异常归一化为可区分的错误。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """对外可区分的错误码。"""

    # 配置类
    CONFIG_INVALID = "config_invalid"          # 配置非法（字段缺失/类型错误）
    CONFIG_CONFLICT = "config_conflict"        # 配置冲突（重复路由/上游引用不存在）
    # 限流类
    RATE_LIMITED = "rate_limited"              # 超出速率窗口
    QUOTA_EXHAUSTED = "quota_exhausted"        # 配额耗尽
    # 熔断类
    CIRCUIT_OPEN = "circuit_open"              # 熔断器打开，快速失败
    # 上游类
    UPSTREAM_TIMEOUT = "upstream_timeout"      # 上游超时
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"  # 连接失败/5xx 等
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"    # 无可用上游实例
    # 其他
    ROUTE_NOT_FOUND = "route_not_found"        # 无匹配路由


class GatewayError(Exception):
    """网关内部统一异常，携带对外错误码与 HTTP 状态。"""

    def __init__(self, code: ErrorCode, message: str, http_status: int = 502,
                 retry_after: float | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.retry_after = retry_after

    def to_dict(self) -> dict:
        body: dict = {"error": {"code": self.code.value, "message": self.message}}
        if self.retry_after is not None:
            body["error"]["retry_after"] = self.retry_after
        return body
