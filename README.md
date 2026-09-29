# API 网关与边缘代理（本地可运行）

基于 FastAPI + httpx 的本地 API 网关：按请求特征分发流量到多个上游实例，
支持限流/配额、熔断与有限重试，配置可运行时原子热更新。

## 运行

```bash
uv sync
uv run python main.py            # 网关监听 127.0.0.1:18080
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
    上游名重复、限流调用方重复、分流匹配条件重复等）。
- `GET /admin/breakers` 查看各路由各上游的熔断器状态。

## 分流规则（灰度发布）

同一路由下可配多条 `rules`，按请求头或查询参数的值把流量引到各自的
上游组；都未命中时落到该路由的默认上游组（`upstreams`）：

```json
{
  "name": "main",
  "path_prefix": "/api",
  "upstreams": [{"name": "stable", "base_url": "http://127.0.0.1:9001"}],
  "rules": [
    {"name": "gray",
     "match": {"header": "x-gray", "value": "1"},
     "upstreams": [{"name": "gray-1", "base_url": "http://127.0.0.1:9003"}]},
    {"name": "beta",
     "match": {"query": "track", "value": "beta"},
     "upstreams": [{"name": "beta-1", "base_url": "http://127.0.0.1:9004"}]}
  ]
}
```

- **求值顺序确定**：规则按配置数组顺序求值，先命中先生效；
  同时命中多条时排在前面的生效。
- **提交时校验**：同一路由下匹配条件重复（同类型、同键、同值）的规则
  会被拒绝（`409 config_conflict`），因为任何排序都无法消歧；
  规则的上游组为空、键/值为空等会被拒绝（`400 config_invalid`），
  错误消息指明具体规则与原因。
- 每条规则的上游组有独立的负载均衡器与熔断器集合；响应头
  `x-upstream-group` 标明本次命中的组（默认组为路由名）。

## 变更记录与回退

- 每次成功提交（含回退）追加一条不可变的变更记录，分配单调递增的
  `revision`；`PUT /admin/config` 的响应会返回该 `revision`。
- `GET /admin/config/history?limit=20` 查看最近的变更记录
  （`revision`、提交时的 `version`、时间戳、回退来源），新的在前。
- `POST /admin/config/rollback`，请求体 `{"revision": N}`：
  把当前配置退回到第 N 次记录，**紧接着的请求立即按回退后的规则走**
  （路由改名、整组换上游、只调限流参数都一样）；回退本身也追加一条
  记录（`rolled_back_to` 指向来源），可在多个版本间来回切换。
- 回退不修改历史；进行中的请求持有旧快照引用，不会被打断。
- 目标 `revision` 不存在时返回 `404 revision_not_found`，
  当前生效的配置保持不变。

## 限流状态查询与配额周期

- `GET /admin/ratelimits/{caller}` 查询调用方状态：`remaining_burst`、
  `remaining_quota`、`last_reject`（最近一次拒绝原因）、
  `retry_after`（补满一个令牌还需多久）、`quota_reset_in`
  （距配额周期重置的秒数）。无规则的调用方返回 `{"rule": null}`。
- `rate_limits[].quota_period_seconds`：配额按周期自动重置（秒），
  缺省为 `null` 表示累计不重置（老行为）。重置与并发请求在同一把锁内
  完成，不会多放，也不会把未恢复的额度算成可用。
- 被拒响应（`429`）带 `Retry-After` 头，错误体含 `retry_after`、
  `remaining_burst`、`remaining_quota`；正常放行的响应带
  `x-ratelimit-remaining-burst` / `x-ratelimit-remaining-quota` 头。

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
| `rate_limits[].quota_period_seconds` | 配额重置周期（秒），`null` 不重置 | `null` |
| `routes[].rules[]` | 分流规则（见上） | `[]` |
| `routes[].rules[].match` | `{"header": 名, "value": 值}` 或 `{"query": 名, "value": 值}` | 必填 |
| `routes[].rules[].upstreams[]` | 该规则命中的上游组 | 必填 |

## 兼容性

不带 `rules`、`quota_period_seconds` 等新字段的老配置可原样提交，
行为与之前完全一致（无分流、配额累计不重置）。

## 本地验证

```bash
uv sync
uv run pytest            # 全部测试（含分流、历史回退、配额周期）
uv run python main.py    # 启动网关后：
curl -X PUT localhost:18080/admin/config -d @gateway.config.json \
     -H 'content-type: application/json'
curl localhost:18080/admin/config/history
curl -X POST localhost:18080/admin/config/rollback \
     -d '{"revision": 1}' -H 'content-type: application/json'
curl localhost:18080/admin/ratelimits/anonymous
curl -H 'x-gray: 1' localhost:18080/api/echo   # 命中灰度规则
```

日志中可看到每个请求最终命中的路由/上游组与上游实例、限流判定依据，
以及每次配置提交与回退（`config updated revision=...` /
`config rolled back ...`）。

## 测试

`uv run pytest` 覆盖：配置热更新（原子生效/进行中请求不中断/非法拒绝）、
并发限流（计数准确、拒绝原因可区分、窗口与配额恢复）、
熔断（三态转换、半开探测失败重开、超时归一化、重试仅幂等且无重复副作用）。
测试日志会打印每个请求、限流判定依据与熔断状态变化。
