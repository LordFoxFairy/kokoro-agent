# kokoro-agent v1 API 契约

状态：Agent-owned 当前契约，2026-09-02。

本仓维护 Agent ingress、执行控制、内部 Redis command/event protocol、查询/replay 和对应 contract tests。
Root 不保存或生成 Agent wire。BFF 的公开 Chat、AG-UI 与浏览器生命周期由 BFF 自己维护；Capability、Storage、
IAM、Model 的业务 API 由各自 owner 维护，Agent 只通过窄 client port 消费。

## 1. 代码与契约归属

| 内容 | 本仓位置 | 职责 |
|---|---|---|
| HTTP ingress | `src/kokoro_agent/http/` | v1 请求校验、可信上下文、admission、响应与错误映射 |
| 执行 command/event | `src/kokoro_agent/protocol/` | Agent-owned 严格 Pydantic wire；不包含其他 owner 的数据库模型 |
| Application | `src/kokoro_agent/services/`、`execution/` | 用例、Run/control/HITL 和安全产品投影 |
| Repository port/records | `src/kokoro_agent/repositories/` | Agent 自己的运行持久化边界 |
| 数据库/Redis 实现 | `src/kokoro_agent/infrastructure/`、`streams/` | PostgreSQL、checkpoint 与 Redis 技术实现 |
| Workspace naming | `src/kokoro_agent/sandbox/workspace.py` | Agent 本地/S3 工作区 key；不是 Storage 服务契约 |

旧 `contract/storage.py` 中的 BSON、Skill/MCP document、collection 与跨仓 receipt 镜像已退出运行包。
本仓也不保留未接线的 Root-generated gRPC consumer。

## 2. HTTP ingress

HTTP ingress 只负责 transport/admission；Agent 执行由独立的 `kokoro-agent-worker` 进程完成。
BFF 只调用版本化入口，不读 Agent PostgreSQL、Redis、checkpoint 或 RunRepository。

| 方法 | 路径 | 作用 | 成功 |
|---|---|---|---|
| `GET` | `/healthz` | 进程存活 | `200` |
| `GET` | `/readyz` | PostgreSQL + Redis 可用 | `200` |
| `POST` | `/v1/runs` | durable admission 后投递 Run | `202` |
| `POST` | `/v1/runs/{run_id}/control` | cancel/resume/steer | `202` |
| `GET` | `/v1/runs/{run_id}/events` | 内部执行证据，按 `after_seq` 分页 | `200` |
| `GET` | `/v1/sessions` | identity-scoped 会话摘要 | `200` |
| `GET` | `/v1/sessions/{session_id}/messages` | 安全 Chat history | `200` |
| `GET` | `/v1/sessions/{session_id}/events` | 安全 Chat replay | `200` |

Session detail、title、share、delete 和 public snapshot 属于 BFF，不通过直接访问 Agent 数据库补齐。

`GET /v1/sessions` 支持 `project_ref`、`limit`（1..100）和不透明 `cursor`。返回 `data.sessions[]`，包含
`session_id`、`project_ref`、`title`、`created_at`、`updated_at`；有下一页时返回 `next_cursor`。排序为
`updated_at DESC, session_id ASC`，namespace 从可信身份派生。

### Run admission

`POST /v1/runs` 接受 `request_id`、`run_id`、`session_id`、`feature_key`、`execution_identity`、顶层
`message_id`、`content`，以及可选的 `requested_model_label`、`trace`。HTTP ingress 将其校验后映射为
本仓内部 `RunRequest`，其中消息放在 `input` 对象。两种 transport shape 不形成两个业务 owner。

Agent 在自己的 `run_dispatches` 中保存不可变 request digest，随后发布 `REQUESTS_STREAM`。
同一 `run_id` 和 body 重试复用 receipt；body 漂移返回 `409 run_identity_conflict`。

### Control

`POST /v1/runs/{run_id}/control` 使用 `kind`=`run.cancel`、`run.resume` 或 `run.steer`，要求 `session_id`。
steer 还需 `message_id`、`content`；resume 还需非空 `decisions`。请求通过 `Idempotency-Key` 提供稳定 `command_id`。

Agent 在 `run_control_commands` 按 `(run_id, command_id)` 保存 admission 和 canonical request digest，再发布到
该 run 的 Redis control stream。相同 key/digest 重放原 receipt；digest 漂移返回 `409 command_digest_mismatch`。
HTTP receipt 状态为 `pending`、`succeeded`、`failed`，是同一条控制记录的投影，不维护第二张 receipt 表。

## 3. 认证、上下文与响应

除 `/healthz` 外，入口要求 `KOKORO_INTERNAL_SECRET_AGENT` 和标准 `Authorization: Bearer <secret>`。
未配置 secret 返回 `503 service_auth_not_configured`；认证缺失或错误返回 `401 service_auth_failed`。

Session query/replay 通过 `x-kokoro-tenant-ref`、`x-kokoro-subject-ref`、`x-kokoro-actor-ref`、
`x-kokoro-identity-assertion-ref` 接收可信服务上下文。浏览器 `X-Domain` 不参与身份或隔离计算。
`X-Request-Id` 用于响应 metadata，control 额外要求 `Idempotency-Key`。

成功使用 `{data, meta:{request_id}}`，错误使用 `{error:{code,message}, meta:{request_id}}`。
health endpoint 使用轻量 status payload。响应不泄露 Python 堆栈、SQL、Redis 或 provider secret。

## 4. 内部协议与执行边界

```text
Agent v1 HTTP request
  -> strict ingress validation
  -> Agent-owned protocol/control.py
  -> Redis worker -> RunRepository claim
  -> FeatureCatalog -> AgentFactory -> DeepAgents/official Swarm
  -> native checkpoint + Agent chat facts
  -> Agent Chat HTTP query/replay -> BFF-owned AG-UI projection
```

`ExecutionIdentity.tenant_ref + subject` 派生内部 `RuntimeNamespace`；actor/assertion 不参与隔离 key。
caller 不提交 namespace、thread、Agent、Skill、MCP、Tool、provider 或 Feature 配方。
`feature_key` 只索引 worker-local Feature，不是用户可写的 Agent selector。

## 5. 安全 Chat 事件

Agent 的 `chat_events` 保存安全归一化类型：

```text
run.started | assistant.delta | assistant.completed | activity
interaction | delivery | run.completed | run.failed
```

BFF 通过本仓 HTTP replay 按 `seq` 读取，再按 BFF 自己的 AG-UI contract 投影。
raw thinking、tool args/results、subagent text、sandbox path、object key、prompt、secret 和 LangChain native state
不进入安全 Chat projection。内部执行证据与浏览器产品事件不是同一个数据面。

LangChain checkpoint 与 Agent 的 `chat_messages`、`chat_events` 分开。Agent 不读取或改造框架 checkpoint 表，
不创建 `conversation_messages`、`run_events` 或独立 `event_outbox`。事件以 `chat_event_id + seq` 保持幂等和顺序。

## 6. 本仓变更与验证

```text
本仓 API/protocol + 状态机
  -> Application/Repository/HTTP 实现
  -> contract/unit/integration/acceptance tests
  -> 本仓 API 与技术文档
  -> BFF 消费者在自己的仓库更新 client 和 contract tests
```

本仓运行 `uv run ruff check .`、`uv run pyright`、`uv run pytest`；真实 PostgreSQL/Redis acceptance 另按
`ACCEPTANCE.md` 运行。Root 的 topology/E2E 只编排和验证，不替代本仓 contract，也不生成源代码。
