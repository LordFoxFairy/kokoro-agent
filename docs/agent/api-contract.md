# kokoro-agent API/AIP 契约摘录

状态：当前 API/AIP 消费契约，2026-08-31。

跨仓同步规则见根仓 [51-跨子仓 API/AIP 契约与技术方案同步](../../../docs/kokoro-handbook/technical/51-cross-repository-contract-sync.md)。

Root `contract/`（Proto、manifest、生成器）是唯一 wire authority。本页只说明 GA consumer 需要实现的边界，不能作为独立 schema
来源，也不能在 `kokoro-agent` 内修改字段编号或复制 Root DTO。当前 worker 仍从 Redis 接收
内部 launch envelope，使用严格 Python adapter；它与 Root RPC 语义一致，但字段形状由 transport
adapter 映射（Root 的 `message_id/content` 是顶层字段，Redis envelope 暂用 `input` 对象）。
generated consumer/transport 接入后只替换这层映射，不改变 Feature/Agent API。

## Agent business HTTP ingress（v1）

HTTP ingress 位于本仓 `src/kokoro_agent/http/`，只负责 transport/admission；执行仍由独立的
`kokoro-agent-worker` 进程完成。BFF 只调用下面的版本化入口，不读 Redis、PostgreSQL、checkpoint
或 RunLedger。

| 方法 | 路径 | 作用 | 成功 |
|---|---|---|---|
| `GET` | `/healthz` | 进程存活 | `200` |
| `GET` | `/readyz` | PostgreSQL + Redis 可用 | `200` |
| `POST` | `/v1/runs` | durable admission 后投递一个 Run | `202` |
| `POST` | `/v1/runs/{run_id}/control` | cancel/resume/steer | `202` |
| `GET` | `/v1/runs/{run_id}/events` | Agent 内部证据回放，按 `after_seq` 分页 | `200` |
| `GET` | `/v1/sessions/{session_id}/messages` | 安全 Chat history | `200` |
| `GET` | `/v1/sessions/{session_id}/events` | 安全 Chat replay | `200` |

上表是当前已实现的 Agent v1 business ingress；它不包含 BFF 的 session list/detail、title、share、
delete 或 public snapshot。上述 BFF 业务能力不属于 Agent ingress，也不能通过直读 Agent
PostgreSQL/Redis 来补齐。

`POST /v1/runs` body 是 Root `LaunchRunRequest` 的 JSON transport 映射：
`request_id`、`run_id`、`session_id`、`feature_key`、`execution_identity`、顶层
`message_id`、`content`，以及可选 `requested_model_label`/`trace`。ingress 先在 Agent-owned
PostgreSQL `run_dispatches` 中写入不可变 `sha256` fence，再发布现有
`REQUESTS_STREAM`；超时重试同一 `run_id` 和 body 会复用 receipt，body 漂移返回
`409 run_identity_conflict`。

`POST /v1/runs/{run_id}/control` body 使用 `kind`=`run.cancel`、`run.resume` 或 `run.steer`，
严格 transport body 要求 `session_id` 和 `decision_id`；steer 另需 Root 定义的 `message_id`/
`content`，cancel/resume 还需 `decisions`（resume）。control 只写该 run 的隔离 Redis
control stream，由 worker 的 durable inbox 和 session 校验继续完成幂等与授权边界；cancel/resume
按 `decision_id` 去重，steer 按 `message_id` keep-first 入账。

Chat history/replay 只返回 `chat_messages`/`chat_events` 的 allowlisted projection；请求通过
`x-kokoro-tenant-ref`、`x-kokoro-subject-ref`、`x-kokoro-actor-ref`、
`x-kokoro-identity-assertion-ref` 提供受信服务上下文。浏览器的 `X-Domain` 不属于该接口，也
不会参与身份或隔离计算。所有非 health 请求在配置内部密钥时要求
`x-kokoro-service: kokoro-bff` 与 `x-kokoro-internal-secret`。

业务响应统一为 `{data, meta:{request_id}}` 或 `{error:{code,message}, meta:{request_id}}`；
health endpoint 保留轻量 status payload。空 body、
非法 JSON、依赖不可用和 run scope 错误均使用稳定错误码，不返回内部 Python、Redis 或 SQL 细节。

## GA 入站

```text
Root `kokoro.agent.v1.LaunchRunRequest`
  request_id (必填)
  run_id
  session_id
  feature_key
  execution_identity { tenant_ref, actor, subject, identity_assertion_ref }
  message_id
  content
  requested_model_label?
  trace_json

Redis worker adapter（当前内部 envelope）
  kind: run.request
  request_id? (本地旧 fixture 可省略)
  run_id / session_id / feature_key / execution_identity
  input { message_id, content }
  requested_model_label?
  trace?

Root `kokoro.agent.v1.ApplyControlRequest`
  request_id / agent_run_id / command / control_kind
  decisions[] / optional message_id + content

Redis worker control envelope（当前内部 adapter）
  run.resume { run_id, session_id, decision_id, decisions[] }
  run.cancel { run_id, session_id, decision_id }
  run.steer { run_id, session_id, message_id, content }

`agent_run_id` 到 `session_id` 的映射由 GA RunLedger/transport adapter 完成；BFF Chat 不把
Root RPC DTO 直接当 Redis JSON 发送。

ForkConversation / CleanupThread
  opaque session/run references only; no history, checkpoint, graph, Agent, Skill or namespace selector
```

GA ingress 从稳定的 `ExecutionIdentity.tenant_ref + subject` 派生内部 `RuntimeNamespace`；actor/assertion 不参与隔离键；caller 不传 namespace、thread、Agent、Skill、MCP、Tool、provider 或
Feature 配方。`feature_key` 只用于索引 worker-local `Feature`，不是用户可写的 Agent selector。
`requested_model_label` 只进入模型选择边界；当前 worker 将其翻译为 provider/name，并由 GA
`model/factory.py` 结合进程级 `ChatModelSettings` 构造 provider。Model public client 接入生产后，
GA 必须在此边界再次校验可用性。凭据和 endpoint 仍由 worker `ChatModelSettings` 提供。

## GA 出站

GA 只产生两类跨仓可见结果：

1. `LaunchRunResponse` / control receipt：表示受理、终态或控制结果；
2. 安全 `ProductEvent` 投影事实：以 GA 内部归一化记录写入 `chat_events`，由 BFF Chat 查询/replay 并投影到 AG-UI/SSE。

`chat_events.event_type` 是 GA 为查询/replay 使用的内部安全归一化类型，与 Root public
`ProductEvent` 的 kind 不要求同名。当前归一化集合为：

```text
run.started | assistant.delta | assistant.completed | activity
interaction | delivery | run.completed | run.failed
```

BFF Chat 在 Root Chat 边界将这些记录映射为对外 ProductEvent；GA 不绕过该边界直接写浏览器事件流。

安全事件只允许 `run_phase`、`assistant_delta/final`、`activity`、`approval_request`、`plan_snapshot`、`artifact_ready`、
`studio_job_linked`、`terminal`。raw thinking、tool args/results、subagent text、sandbox path、object key、prompt、secret 和
LangChain native state 永不出现在 Root public event。

## 存储与 ID

```text
GA: chat_messages.chat_message_id, chat_events.chat_event_id, chat_events.seq
Framework: Message.id, thread_id, checkpoint_id, tool_call_id
```

两组 ID 不互换。GA 不读取或改造 LangChain checkpoint 表，不创建 `conversation_messages`、`run_events` 或独立 `event_outbox`。
`chat_events` 先 durable 写入，BFF Chat 通过 Root Chat query boundary 按 `seq`
replay。GA 的安全投影不直写 BFF browser-live stream；该 stream 的 generated
envelope 和 seq 由 BFF Chat 边界维护。

## 生成与验证

```text
Root contract/proto + manifest
  -> generated GA consumer types
  -> kokoro-agent contract adapter
  -> GA contract tests
```

契约变更必须在根仓完成并重新生成本仓 consumer；本仓 CI 只验证生成物 provenance、严格字段校验、ProductEvent 脱敏和事件幂等。

## 本仓同步规则

本文件是 GA 对 Root API/AIP 的**消费视图**，不是另一份契约。每次跨仓契约变更，按下面顺序在同一交付批次内更新：

```text
Root contract/proto + manifest
  -> 生成 GA consumer types
  -> GA adapter、运行时和 contract tests
  -> 本文件（API/AIP 摘录）
  -> technical-plan.md（实现链路/验收门）
```

更新责任保持清晰：

| 内容 | 权威位置 | `kokoro-agent` 的更新内容 |
|---|---|---|
| wire 字段、编号、oneof、兼容规则 | Root `contract/` | 重新生成 consumer；本文件只同步语义摘录 |
| GA 如何接收、执行、恢复和落库 | GA 技术方案 36/42 | 更新 `technical-plan.md` 与实现/测试 |
| GA 与 BFF Chat/Capability/Storage 的 owner 边界 | Root 方案与各 owner public contract | 更新本文件边界说明和 `current-boundary.md` |

因此，子仓会更新自己的 API/AIP 摘录和技术方案，但不会在子仓新增平行 Proto、OpenAPI、字段编号或 DTO；Root
契约生成物缺失、来源不明或与文档语义漂移时，CI 直接失败。
