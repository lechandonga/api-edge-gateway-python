# API 网关与边缘代理（本地可运行）

基于 FastAPI + httpx 的本地 API 网关：按请求特征分发流量到多个上游实例，
支持限流/配额、熔断与有限重试，配置可运行时原子热更新。

## 运行

```bash
uv sync
uv run python main.py            # 网关监听 127.0.0.1:8080
uv run pytest                    # 运行全部测试（自动启动本地上游实例）
```

配置文件默认为 `gateway.config.json`（不存在时用内置示例），
可用环境变量 `GATEWAY_CONFIG` 指定路径。

## 路由与负载均衡

- **路由匹配**：按 `path_prefix` 最长前缀匹配请求路径，未匹配返回
  `404 route_not_found`。
- **负载均衡策略**（路由级 `strategy`）：
  - `round_robin`：按 `weight` 展开的序列轮转（默认）；
  - `random`：随机选择；
  - `least_connections`：选择当前在途请求数最少的实例。
- 每个上游实例有独立熔断器；某实例熔断打开时，本轮选择会跳过它，
  全部不可用时返回 `503 no_healthy_upstream` / `circuit_open`。

## 配置热更新

- `GET /admin/config` 查看当前配置；`PUT /admin/config` 整体替换。
- 配置是**不可变快照**：校验通过后原子替换，新请求立即生效；
  进行中的请求在进入时已持有旧快照与旧引擎引用，**不会被切换中断**。
- 非法/冲突配置被拒绝且旧配置保持不变，原因可区分：
  - `400 config_invalid`：字段非法（缺字段、负速率、权重≤0、URL 非法等）；
  - `409 config_conflict`：结构冲突（路由名/前缀重复或互相包含、
    上游名重复、限流调用方重复）。
- `GET /admin/breakers` 查看各路由各上游的熔断器状态。

## 限流与配额

按调用方维度（请求头 `x-caller-id`，缺省 `anonymous`）：

- **限流**：令牌桶，`rate_per_second` 为补充速率，`burst` 为桶容量。
  超限返回 `429 rate_limited`，并带 `Retry-After` 头（补满一个令牌所需秒数）。
- **配额**：`quota` 为调用方累计请求上限（`null` 表示不限）。
  耗尽返回 `429 quota_exhausted`，与速率超限可区分。
- 计数在单把锁内完成"补充—判定—扣减"，并发下不超发、不丢失
  （测试：50 并发对 burst=10，恰好放行 10 个）。
- 窗口随时间自动恢复；配额可通过 `PUT /admin/config` 提升后恢复放行。
  规则未变的调用方在配置更新后**保留已用计数**。

## 熔断与重试

- **熔断器**（每上游实例一个）三态转换：
  - `closed`：正常；连续失败（超时/连接错误/5xx）达 `failure_threshold` 次 → `open`；
  - `open`：快速失败返回 `503 circuit_open`（带 `Retry-After`），
    `reset_timeout` 秒后 → `half_open`；
  - `half_open`：放行 `half` 个探测请求，成功 → `closed`，失败 → 立即回到 `open`。
- **重试**：仅对幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）生效，
  最多 `max_retries` 次，且每次换到尚未尝试过的上游实例；
  POST 等非幂等方法**绝不重试**，不会产生重复副作用。
- **错误归一化**：上游异常统一映射为对外可区分错误——
  `504 upstream_timeout`（超时）、`502 upstream_unavailable`（连接失败/5xx）、
  `503 circuit_open` / `no_healthy_upstream`。

## 配置项

| 字段 | 说明 | 默认 |
|---|---|---|
| `routes[].path_prefix` | 路由匹配前缀 | 必填 |
| `routes[].upstreams[]` | `name`/`base_url`/`weight` | 必填/1 |
| `routes[].strategy` | 负载均衡策略 | `round_robin` |
| `routes[].max_retries` | 幂等请求最大重试次数 | 1 |
| `routes[].breaker.failure_threshold` | 连续失败熔断阈值 | 3 |
| `routes[].breaker.reset_timeout` | 打开到半开的秒数 | 5.0 |
| `routes[].breaker.half` | 半开探测并发数 | 1 |
| `routes[].breaker.timeout_seconds` | 上游请求超时 | 2.0 |
| `rate_limits[].caller` | 调用方标识 | 必填 |
| `rate_limits[].rate_per_second` / `burst` | 令牌桶速率/容量 | 必填 |
| `rate_limits[].quota` | 累计配额，`null` 不限 | `null` |

## 测试

`uv run pytest` 覆盖：配置热更新（原子生效/进行中请求不中断/非法拒绝）、
并发限流（计数准确、拒绝原因可区分、窗口与配额恢复）、
熔断（三态转换、半开探测失败重开、超时归一化、重试仅幂等且无重复副作用）。
测试日志会打印每个请求、限流判定依据与熔断状态变化。
