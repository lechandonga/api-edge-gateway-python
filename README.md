# API 网关与边缘代理（本地可运行）

基于 FastAPI 的本地 API 网关：按请求特征分发流量到多个上游实例，支持限流/配额、
熔断与受限重试，配置可运行时原子热更新。

## 快速开始

```bash
# 启动两个本地上游实例
uv run python upstream_demo.py 9001 --name u1 &
uv run python upstream_demo.py 9002 --name u2 &

# 启动网关（默认监听 8000，默认路由 /api -> http://127.0.0.1:9001）
uv run python main.py 8000

# 代理请求
curl http://127.0.0.1:8000/proxy/api/echo

# 查看 / 热更新配置
curl http://127.0.0.1:8000/admin/config
curl -X PUT http://127.0.0.1:8000/admin/config \
  -H 'content-type: application/json' -d @config.json

# 运行测试
uv run pytest tests/ -q
```

## 路由与负载均衡

请求路径为 `/proxy/<path>`，网关按路由配置依次匹配：

- `match.path_prefix`：路径前缀（必须以 `/` 开头）；命中后转发时**剥离该前缀**。
- `match.methods`：HTTP 方法列表，空列表表示匹配全部方法。
- `match.headers`：请求头精确匹配（键不区分大小写），可用于灰度/租户分流。

匹配到路由后按 `strategy` 在健康上游（熔断未打开）中选择目标：

| 策略 | 行为 |
|---|---|
| `round_robin` | 按 `weight` 展开的序列轮询 |
| `random` | 按 `weight` 加权随机 |
| `least_connections` | 选择在途请求数最少的上游 |

路由配置项：`timeout_ms`（上游超时）、`max_retries`（最大重试次数，仅对幂等方法生效）。

## 配置热更新

- `PUT /admin/config` 提交完整配置，**先校验后原子替换**：新请求立即使用新配置；
  进行中的请求持有旧快照，不受切换影响，不会因更新而失败。
- 非法/冲突配置返回 `409`，`error` 字段给出可区分原因：
  `duplicate_route_name`、`conflicting_path_prefix`（两路由前缀互为前缀）、
  `empty_upstreams`、`duplicate_upstream_name`、`invalid_upstream_url`、
  `invalid_weight`、`invalid_timeout`、`invalid_retries`、`duplicate_caller`、
  `invalid_rate_limit`、`invalid_quota`、`invalid_circuit_breaker`；
  JSON 结构不合法返回 `422 invalid_config_schema`。
- 被拒绝的更新不会污染当前生效配置。

## 限流与配额

按调用方（请求头 `x-caller`，缺省 `anonymous`）维度控制，未配置的调用方不限流：

- **速率限制**：令牌桶，`rate_per_second` 为补充速率，`burst` 为桶容量（初始满桶）。
- **配额**：`quota` 为 `quota_window_seconds` 窗口内允许的总次数，窗口到期自动重置。

判定与扣减在同一把锁内原子完成，并发下计数不超发、不丢失。
超限返回 `429`，原因可区分：

| error | 含义 | 响应提示 |
|---|---|---|
| `rate_limited` | 超出速率 | `Retry-After`（秒，等到有令牌） |
| `quota_exceeded` | 配额耗尽 | `Retry-After`（秒，等到窗口重置）+ `X-Quota-Remaining: 0` |

放行响应携带 `X-Quota-Remaining` 表示剩余配额。令牌补充/窗口重置后自动恢复放行。
限流计数在配置热更新后保持连续（计数器独立于配置快照）。

## 熔断与重试

每个上游一个熔断器，状态机：`closed -> open -> half_open`：

- `closed`：正常放行；**连续**失败（超时、连接错误、上游 5xx）达到
  `failure_threshold` 后打开，成功会清零计数。
- `open`：直接拒绝（`503 circuit_open`），`open_seconds` 后进入半开。
- `half_open`：放行至多 `half_open_max_probes` 个探测请求；探测成功则关闭，
  失败则重新打开。状态转换均写入日志。

**重试策略**：仅幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）可重试，且只对传输层错误
（超时、连接失败）重试；POST/PATCH 等一律不重试，避免重复副作用。

**错误归一化**（对外可区分）：

| HTTP | error | 含义 |
|---|---|---|
| 404 | `no_route` | 无匹配路由 |
| 429 | `rate_limited` / `quota_exceeded` | 限流 / 配额 |
| 502 | `upstream_error` | 上游返回 5xx |
| 502 | `upstream_unavailable` | 连接失败等传输错误 |
| 503 | `circuit_open` | 熔断打开 |
| 504 | `upstream_timeout` | 上游超时 |

## 配置项总览

```jsonc
{
  "routes": [{
    "name": "api",
    "match": {"path_prefix": "/api", "methods": [], "headers": {}},
    "upstreams": [{"name": "u1", "base_url": "http://127.0.0.1:9001", "weight": 1}],
    "strategy": "round_robin",      // round_robin | random | least_connections
    "timeout_ms": 3000,
    "max_retries": 1
  }],
  "rate_limits": [{
    "caller": "c1",
    "rate_per_second": 10,
    "burst": 20,
    "quota": 1000,                  // 可省略 = 不限配额
    "quota_window_seconds": 60
  }],
  "circuit_breaker": {
    "failure_threshold": 3,
    "open_seconds": 5,
    "half_open_max_probes": 1
  }
}
```

## 测试

`uv run pytest tests/ -q`，覆盖：

- `test_config_hot_reload.py`：热更新原子生效、进行中请求不中断、非法配置拒绝原因；
- `test_ratelimit.py`：100 并发下计数精确（恰好放行 burst 个）、两种 429 可区分、
  窗口恢复后放行；
- `test_circuitbreaker.py`：熔断打开/半开/恢复、探测失败重新打开、超时归一化、
  POST 不重试（上游计数恰好为 1）、幂等重试仅限传输错误；
- `test_routing_lb.py`：轮询/加权轮询分发、请求头特征路由、方法不匹配 404。

测试日志（`gateway.app` / `gateway.circuitbreaker` logger）会打印每个请求、
限流/路由/负载均衡判定依据与熔断状态变化。
