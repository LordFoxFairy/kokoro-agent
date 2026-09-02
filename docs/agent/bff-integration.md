# kokoro-bff ↔ kokoro-agent 集成边界

状态：当前集成约束，2026-09-01。

本文是 `kokoro-bff` 与 `kokoro-agent` 的边界说明，不是新的 wire schema，也不替代根仓
`contract/` 的 API/AIP 定义。所有跨仓字段、版本和兼容规则必须以已发布的版本化契约为准。

## 1. 三仓职责

```text
浏览器
  -> kokoro Web /api/session/*
  -> kokoro-bff/modules/chat /v1/sessions/*
  -> Agent business adapter
  -> kokoro-agent HTTP ingress
  -> kokoro-agent Redis worker
  -> PostgreSQL durable facts + Redis transport
```

- `kokoro-bff` 是 Web-facing 业务层：负责 Chat 会话/消息/标题/分享投影、鉴权、幂等、错误归一、SSE 连接和业务编排。
- `kokoro-agent` 是执行层：负责 Agent loop、Run/control/HITL、恢复、执行安全事件和持久化执行事实。
- `kokoro-bff` 不直接访问 Agent 的 Redis、PostgreSQL、checkpoint、RunLedger、`chat_messages` 或 `chat_events`。
- Agent 由独立的 HTTP ingress 与 Redis worker 进程组成；HTTP ingress 先写 durable dispatch admission，再投递 worker。BFF 通过版本化 HTTP business ingress 调用 Agent，不接触 Agent 的 PG/Redis。
- `session_id` 仍是 Chat 的稳定业务标识，但不意味着存在独立的 `kokoro-session` 仓库或服务。

## 2. 当前 Agent HTTP business ingress

当前 v1 ingress 由本仓 `kokoro-agent-http` 提供，执行仍由独立的
`kokoro-agent-worker` 完成：

| 方法 | 路径 | 作用 | 成功 |
|---|---|---|---|
| `POST` | `/v1/runs` | launch：durable admission 后投递 Run | `202` |
| `POST` | `/v1/runs/{run_id}/control` | cancel/resume/steer | `202` |
| `GET` | `/v1/runs/{run_id}/events` | Run evidence，按 `after_seq` 分页 | `200` |
| `GET` | `/v1/sessions` | identity-scoped durable session list | `200` |
| `GET` | `/v1/sessions/{session_id}/messages` | 安全 session history | `200` |
| `GET` | `/v1/sessions/{session_id}/events` | 安全 session replay | `200` |

另外提供未经内部认证保护的 `GET /healthz`，以及检查 Agent-owned PostgreSQL/Redis 的
`GET /readyz`；`/readyz` 与所有业务路由一样受内部服务认证保护。HTTP ingress 不执行 Agent
loop，也不把 Redis stream 暴露给 BFF。

### 请求、认证与响应约束

- `POST /v1/runs` 接受 Root `LaunchRunRequest` 的 JSON transport 映射：`request_id`、`run_id`、
  `session_id`、`feature_key`、`execution_identity`、顶层 `message_id`/`content`，以及可选
  `requested_model_label`/`trace`。
- `POST /v1/runs/{run_id}/control` 接受 `run.cancel`、`run.resume`、`run.steer`；当前 strict
  transport body 要求 `kind`、`session_id`、`decision_id`，steer 另需 `message_id`/`content`。
  cancel/resume 通过 run 隔离的 Redis control stream 交给 worker 的 durable inbox，按
  `decision_id` 去重并用 resume fingerprint 做恢复时的 stale 判定；steer 按 `message_id`
  keep-first 入账。
- 除 `/healthz` 外的请求始终要求配置可信的 `KOKORO_INTERNAL_SECRET_AGENT`，并必须带
  `x-kokoro-service: kokoro-bff` 和 `x-kokoro-internal-secret`。未配置 secret 时返回
  `503 service_auth_not_configured`，认证缺失或错误时返回 `403 service_auth_failed`。
  session list/history/replay 还必须带
  `x-kokoro-tenant-ref`、`x-kokoro-subject-ref`、`x-kokoro-actor-ref`、
  `x-kokoro-identity-assertion-ref`；可选 kind 头只允许 `user`、`project`、`service`。
- 业务路由成功响应使用 `{data, meta:{request_id}}`，错误响应使用
  `{error:{code,message}, meta:{request_id}}`。`x-kokoro-request-id` 用于响应 meta；未提供时
  ingress 使用稳定默认值；`/healthz` 和 `/readyz` 保留轻量 health payload。错误不泄露
  Python、Redis 或 SQL 细节。
- launch 在 Agent-owned `run_dispatches` 中以不可变 `sha256` fence 先行受理；同一 `run_id`
  和相同 body 的重试复用 receipt，body 漂移返回 `409 run_identity_conflict`。这保证了
  BFF 可以安全重试 HTTP 请求而不重复投递不同的 Run。

## 3. BFF 业务边界

Agent ingress 只提供上表中的执行、证据、history 和 replay 入口；它不提供 BFF 的完整
Session 业务 API。以下能力仍属于 BFF owner，并不因 Agent ingress 已上线而视为已实现：

- session detail 业务查询；session list 只消费 Agent 的 durable list projection；
- session title 更新；
- session share、公开 snapshot 和 delete；
- 浏览器鉴权、SSE 连接生命周期及 AG-UI/ProductEvent 对外投影。

这些边界由 BFF 自己的业务存储和 contract 实现；BFF 不得为实现它们而直读 Agent
PostgreSQL/Redis，也不得把 Agent 的内部 envelope 透传给浏览器。

BFF 的 adapter 只依赖显式版本化 HTTP endpoint，不依赖 Redis stream/key、consumer group、
checkpoint 或 Python 类型。未配置 endpoint 时，BFF 应返回明确的
`503 upstream_not_configured`，不得静默回退到旧 Session 服务。

建议环境配置：

```text
KOKORO_AGENT_HTTP_BASE_URL=<agent-http-base-url>
KOKORO_AGENT_HTTP_CONTRACT_VERSION=v1
```

## 4. 存储边界：PostgreSQL + Redis

当前 Agent ingress/worker 只保留两个基础设施：

| 组件 | Owner | 用途 |
|---|---|---|
| PostgreSQL | Agent/各业务服务各自的 adapter | Durable truth：执行事实、Chat 事实索引及各业务服务自己的领域数据 |
| Redis | transport/worker owner | stream、队列、lease、heartbeat、wakeup、短缓存和限流 |

- Web 不连接 PostgreSQL 或 Redis。
- BFF 不连接 Agent Redis 或 Agent PostgreSQL；BFF 自己的持久化接入也必须通过其业务 adapter。
- Redis 丢失时从 PostgreSQL 重建；PostgreSQL 不可用时不能由 Redis 假装成功。
- 不新增 MySQL、MongoDB 或旧的独立 Session 存储依赖。

## 5. Chat / SSE 边界

`kokoro-bff/modules/chat` 是 Web-facing Chat owner：

- 消息提交、标题、删除、分享和公开 snapshot；
- 鉴权、namespace/project scope、请求幂等和错误 envelope；
- `Last-Event-ID` replay、SSE keep-alive 和浏览器连接生命周期；
- 将 Agent 执行结果投影成对外 Chat/ProductEvent。

`kokoro-agent` 只拥有执行侧 session metadata、`chat_messages`、`chat_events` 等安全事实和 Run 状态；BFF 通过
版本化 Agent HTTP business ingress 消费 session list/history/replay，不直接读表。Agent 不向浏览器发布
事件，也不创建第二套 Chat API、SSE stream 或独立事件序列。

## 6. 跨仓允许的连接面

| 连接面 | 用途 | 约束 |
|---|---|---|
| BFF Chat v1 HTTP | Web → BFF | 根仓契约生成、BFF 自己的鉴权/幂等/错误 envelope |
| Agent business HTTP v1 | BFF → Agent 业务层 | 只走 HTTP ingress；不直连 Agent PG/Redis；兼容性由双方 contract 测试守住 |
| Redis worker contract | transport owner → Agent | 只消费已定义 internal envelope；BFF 不直连 |
| 环境变量/secret | 选择 adapter、endpoint、数据库和 Redis | 不把地址、凭据、key 写入源码或业务 payload |
| 独立 CI | 各仓自证实现和兼容性 | 不跨仓 import 私有模块，不共享数据库来代替契约 |

不得通过共享源码路径、未发布内部包、手工复制 DTO 或共享运行时数据库建立隐式依赖。
每个子仓只闭环自己的代码、测试、Docker 和发布门禁；跨仓只交换版本化 contract fixture、
兼容性结果和发布元数据。

## 7. 当前验收边界

- [x] Agent HTTP ingress 支持 launch、control、Run events evidence、session history 和 session replay。
- [x] BFF 到 Agent 只走版本化 HTTP；BFF 不读取 Agent PostgreSQL/Redis。
- [x] 除 `/healthz` 外的请求始终由 `x-kokoro-service` + `x-kokoro-internal-secret` 认证；未配置
  secret 时 fail-closed 返回 `503 service_auth_not_configured`，history/replay 使用受信 identity headers。
- [x] 响应使用统一 envelope；launch 以不可变 fence 幂等，control 由 durable inbox 去重和恢复。
- [x] BFF session list 通过 Agent durable identity-scoped ingress 投影；detail、title、share、delete、public snapshot、浏览器 SSE/AG-UI 仍由 BFF 自己实现。
- [x] Agent PostgreSQL + Redis 执行事实、Run/control/HITL、outbox/recovery worker 门禁由本仓测试覆盖。
