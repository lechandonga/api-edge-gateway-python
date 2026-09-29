# API 网关与边缘代理（本地可运行）

基于 FastAPI + httpx 的本地 API 网关：按路径与请求特征（头/查询参数）
把流量灰度分发到多个上游组，支持限流/周期配额、熔断与有限重试，
配置可运行时原子热更新，并保留变更历史、可随时回退。

## 运行

```bash
uv sync
uv run python main.py            # 网关监听 127.0.0.1:18080
uv run pytest                    # 运行全部测试（自动启动本地上游实例）
```

配置文件默认为 `gateway.config.json`（不存在时用内置示例），
可用环境变量 `GATEWAY_CONFIG` 指定路径。

本地手工验证灰度：

```bash
# 分别在 9001/9002（默认组）和 9003（canary 组）起一个回显服务，例如：
python -m http.server 9001   # 或任意能返回 200 的服务
uv run python main.py
curl -s 127.0.0.1:18080/api/hello              # -> 默认组 u1/u2
curl -s -H 'x-gray: true' 127.0.0.1:18080/api/hello   # -> canary 组 c1
curl -s '127.0.0.1:18080/api/hello?channel=beta'      # -> canary 组 c1
# 响应头 x-route / x-upstream-group / x-upstream 标明最终落点
```

## 路由与灰度分流

- **路由匹配**：按 `path_prefix` 最长前缀匹配请求路径，未匹配返回
  `404 route_not_found`。
- **后端组**：一条路由包含一个默认组（顶层 `upstreams`）和零到多个
  具名组 `backend_groups[]`，每个组可独立配置 `strategy`、`max_retries`
  与上游实例；熔断器仍按路由配置、在每个上游实例上独立工作。
- **分流规则** `rules[]`：
  - 每条规则有 `name`、目标 `group` 和一组 `conditions`；
  - 条件类型为 `header`（头名大小写不敏感）或 `query`，按值**精确匹配**；
    `values` 是“或”，同一条规则内多个条件是“与”；
  - 请求按规则依次匹配，命中第一条即用其 `group`，都不命中走默认组；
  - 响应头回传命中结果：`x-route`、`x-upstream-group`、`x-upstream`。
- **确定的优先级**：规则先后只由匹配条件决定——条件维度更多、取值更窄
  （包含关系）的规则总是更优先，与 JSON 里的声明顺序无关。因此同一份
  规则无论怎么排列，分流结论都一致。
- **提交期拒绝歧义配置**（`409 config_conflict`，消息指出具体规则与维度）：
  - 两条规则匹配条件完全重复；
  - 两条规则在同一维度匹配互不包含的不同值（如 `x-gray=true` 与
    `x-gray=false`），怎么排都分不出谁更合适——需收窄为包含关系、
    合并成一条多值规则，或增加额外区分条件；
  - 同一条规则内同一维度写了多个条件（应合并为多值列表）；
  - 规则引用了不存在的后端组、组名与默认组保留名 `default` 冲突等，
    属字段非法，返回 `400 config_invalid`。

```json
{
  "routes": [{
    "name": "main", "path_prefix": "/api",
    "upstreams": [{"name": "u1", "base_url": "http://127.0.0.1:9001"}],
    "backend_groups": [
      {"name": "canary",
       "upstreams": [{"name": "c1", "base_url": "http://127.0.0.1:9003"}]}
    ],
    "rules": [
      {"name": "vip-gray", "group": "canary", "conditions": [
        {"kind": "header", "name": "x-gray", "values": ["true"]},
        {"kind": "query", "name": "user", "values": ["vip"]}]},
      {"name": "gray", "group": "canary", "conditions": [
        {"kind": "header", "name": "x-gray", "values": ["true"]}]}
    ]
  }]
}
```

上例中 `vip-gray` 条件更具体，`x-gray: true` 且 `user=vip` 时它优先
（即使声明在前面）；普通灰度请求命中 `gray`；无头无参数走默认组。

- **负载均衡策略**（组级 `strategy`）：`round_robin`（默认，按 `weight`
  展开的序列轮转）、`random`、`least_connections`（在途请求最少）。

## 配置变更历史与回退

- `GET /admin/config` 查看当前配置；`PUT /admin/config` 整体替换。
  每次**成功**提交返回自增的 `revision`（失败提交不留记录、不改变现状）。
- `GET /admin/config/history?limit=20`：最近变更列表（新的在前），每条含
  `revision`、配置 `version`、`created_at`、`source`（`bootstrap`/
  `submit`/`rollback`）与 `note`；历史最多保留 50 条（启动基线始终保留）。
- `GET /admin/config/revisions/{revision}`：取回某次变更时的完整配置。
- `POST /admin/config/rollback/{revision}`：把当前配置退回到任意一次记录；
  回退本身也产生一条新的 `revision`（`source=rollback`，`note` 标明来源），
  可在两个版本间反复来回切换。
- **生效语义**：配置是不可变快照，请求在入口取一次快照；提交/回退成功后
  的下一个请求立即按新规则走——路由改名、某路径的上游整组替换、只调限流
  参数都一样，不会出现“提交成功仍按老规则转发”的窗口。代理引擎按
  **配置内容指纹**缓存（不看版本号），回退到旧内容时复用旧引擎与熔断状态。
  进行中的请求持有各自的旧引擎引用，切换/回退**不会打断**它们；校验失败时
  当前配置保持原样。

## 限流、周期配额与状态查询

按调用方维度（请求头 `x-caller-id`，缺省 `anonymous`）：

- **限流**：令牌桶，`rate_per_second` 为补充速率，`burst` 为桶容量。
  超限返回 `429 rate_limited`。
- **配额与周期**：`quota` 为周期内请求上限（`null` 不限）；
  `quota_window_seconds` 设置后配额按周期自动重置（窗口从该调用方首个
  请求开始计时），跨周期已用量清零；不设置则配额不周期重置（旧语义）。
  耗尽返回 `429 quota_exhausted`。令牌补充、配额判定/扣减与跨窗重置在
  同一把锁内完成，重置瞬间的并发请求不会超发，也不会把未恢复的额度
  算作可用。
- **状态查询**：`GET /admin/ratelimits/{caller}` 返回
  `remaining_burst`、`quota`/`remaining_quota`、`quota_window_seconds`、
  `quota_resets_in`（距下次重置秒数）、`last_rejected_reason`、
  `last_rejected_seconds_ago` 等；无规则的调用方返回 404。
- **响应可见性**：
  - 放行响应带头 `x-ratelimit-remaining-burst`，有配额时还有
    `x-ratelimit-remaining-quota`，周期配额另有 `x-quota-resets-in`；
  - 被拒响应带 `Retry-After`（速率超限=补一个令牌所需时间，配额耗尽=
    距窗口重置时间），错误体与响应头同样给出 `remaining_burst`/
    `remaining_quota`/`quota_resets_in`。
- 规则未变的调用方在配置更新/回退后保留已用计数；规则变化时重建限流器。

## 熔断与重试

- **熔断器**（每上游实例一个）三态：连续失败（超时/连接错误/5xx）达
  `failure_threshold` 次 `closed -> open`；`open` 快速失败
  `503 circuit_open`（带 `Retry-After`），`reset_timeout` 秒后 `half_open`
  放行 `half` 个探测，成功回 `closed`、失败立即重开。
- **重试**：仅幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）生效，最多
  `max_retries` 次且每次换到未试过的上游；POST 等绝不重试。
- **错误归一化**：`504 upstream_timeout`、`502 upstream_unavailable`、
  `503 circuit_open` / `no_healthy_upstream`、`404 route_not_found`。
- `GET /admin/breakers` 查看熔断器状态：只有默认组的路由保持
  `{路由: {上游: 状态}}`；含具名组时为 `{路由: {组: {上游: 状态}}}`。

## 配置项

| 字段 | 说明 | 默认 |
|---|---|---|
| `routes[].path_prefix` | 路由匹配前缀 | 必填 |
| `routes[].upstreams[]` | 默认组，`name`/`base_url`/`weight` | 必填/1 |
| `routes[].strategy` / `max_retries` | 默认组负载均衡策略/重试次数 | `round_robin`/1 |
| `routes[].default_group` | 默认组名字（具名组不可占用） | `default` |
| `routes[].backend_groups[].name` | 具名上游组名 | 无则不灰度 |
| `routes[].backend_groups[].upstreams[]` | 组内上游，字段同默认组 | 必填 |
| `routes[].backend_groups[].strategy` / `max_retries` | 组级策略/重试 | `round_robin`/1 |
| `routes[].rules[].name` / `group` | 分流规则名 / 目标组名 | 必填 |
| `routes[].rules[].conditions[].kind` | `header` 或 `query` | 必填 |
| `routes[].rules[].conditions[].name` / `values` | 匹配名 / 精确值列表（或） | 必填 |
| `routes[].breaker.*` | `failure_threshold`/`reset_timeout`/`half`/`timeout_seconds` | 3/5.0/1/2.0 |
| `rate_limits[].caller` | 调用方标识 | 必填 |
| `rate_limits[].rate_per_second` / `burst` | 令牌桶速率/容量 | 必填 |
| `rate_limits[].quota` | 周期配额，`null` 不限 | `null` |
| `rate_limits[].quota_window_seconds` | 配额重置周期秒数，缺省不重置 | `null` |

### 与老配置的兼容范围

不带新字段的旧配置提交后行为完全不变：

- 没有 `backend_groups`/`rules`/`default_group` 时，顶层 `upstreams`
  就是唯一的默认组，分流结果与以前一致，`/admin/breakers` 返回结构不变；
- 没有 `quota_window_seconds` 时配额不周期重置，只能靠提交新配置提升；
- `GET /admin/config` 的输出新增了 `backend_groups`、`rules`、
  `default_group`、`quota_window_seconds` 等字段（老配置下为空/`null`）；
  提交时这些字段全部可省略。条件值也支持简写 `value`（等价于单值
  `values`）。

## 测试

`uv run pytest` 共 34 个用例，覆盖：

- 灰度分流：头/查询命中、多值“或”与多条件“与”、兜底默认组、
  具体度优先与声明顺序无关、重复/不可比较规则提交被拒并指出位置、
  引用不存在的组被拒；
- 变更历史与回退：历史查询与按 revision 取回、回退后立即生效
  （路由改名/整组换上游/仅限流参数）、两个版本来回切换每次立即生效、
  回退不存在的 revision 与非法提交不破坏现状；
- 限流配额：并发不超发、速率/配额拒绝可区分、窗口与配额恢复、
  重置边界两批并发分别精确放行、状态查询与响应头、老配置无周期语义；
- 既有用例：配置热更新、进行中请求不中断、负载均衡、熔断三态与幂等重试、
  冒烟转发，全部照旧通过。

日志中可看到每个请求的最终路由与上游组（`route=`/`matched_rule=`/
`group=`/`upstream=`）、限流判定依据与剩余量、熔断器状态变化，
以及每次配置提交与回退（`config committed revision=... source=...`）。
