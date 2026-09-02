# kokoro-agent 边界

状态：当前 GA 边界，2026-08-31。

完整的 Agent 对象、命名和运行链路见[最终架构](architecture.md)。本页只列 owner 边界，避免把
GA 和外部服务混在一起。

## GA 拥有

- 基于 DeepAgents 的 Agent 执行、native subagent、interrupt、checkpoint 接线。
- `Feature` 装配、`AgentFactory` 和 official `langgraph-swarm` handoff。
- RunRepository、租约、恢复、HITL、工具策略和 workbench。
- Agent 声明 Skill 的解析接线，以及供 DeepAgents 原生 SkillsMiddleware 使用的只读 backend route。
- 真实 provider 的模型适配与请求级模型选择；确定性 fake 只存在于 `tests/support`，不属于 GA 运行包。
- 用户可见的 `chat_messages`、`chat_events` 持久化事实；Web-facing 查询、replay 与投影由 `kokoro-bff/modules/chat` 承接。
- Agent 内部唯一的 `run_control_commands` 可靠性事实；它不属于 Chat 投影，也不对 BFF 开放数据库访问。
- MCP 连接 egress 策略的 worker 启动初始化；连接层只消费进程级配置快照。
- 从受信 `ExecutionIdentity.tenant_ref + subject` 派生的稳定内部 `RuntimeNamespace`；actor/assertion 不参与隔离键。

## GA 不拥有

- BFF Chat 的浏览器鉴权、会话/消息业务投影与 AG-UI/SSE 连接生命周期。
- Root API/AIP 字段和生成器（根仓 `contract/`）。
- 用户、项目、site、IAM 主数据和授权决策。
- Skill/MCP CRUD 与路径元数据（Capability public contract）。
- Blob、Artifact bytes 与生命周期（Storage public contract）。
- Studio Job、Billing、Model catalog 的 owner 数据。
- DeepAgents/LangGraph checkpoint 表结构及 LangChain native message ID。

## 运行边界

```text
Root `LaunchRunRequest(feature_key, message_id, content, ExecutionIdentity)`
  -> Redis internal envelope `input={message_id,content}`
  -> worker / RunRepository claim
  -> FeatureCatalog
  -> AgentFactory
  -> create_deep_agent 或 official create_swarm
  -> native checkpoint + GA chat facts
  -> Root Chat query boundary -> BFF Chat module
```

调用方不提交 namespace、thread、Agent、Skill、MCP、工具或 graph 配方；Root contract 允许的模型
标签/trace 只作为旁路元数据。`RuntimeNamespace` 只在 GA 内部派生；Factory 把可信
`ExecutionIdentity` 和该 namespace 传给外部 Capability client 做本次解析，二者不进入 Agent/Feature
声明。运行依赖由 worker 启动时组装为 `WorkerDependencies`，持有在 `AgentFactory` 实例中；不通过请求参数传播，
也不伪装成业务 `Service`。数据访问通过 `repositories/` 的 repository port，技术实现通过
`infrastructure/` adapter 提供。

LangChain checkpoint/native state 与 GA 聊天事实严格分开：

```text
DeepAgents/LangGraph checkpoint -> native state
GA chat_messages              -> Chat 事实历史
GA chat_events                -> 安全事件事实与 replay
BFF browser stream            -> 独立 generated envelope 的 live transport
```

GA 不创建 `conversation_messages`、`run_events` 或独立 `event_outbox`。代码里的
`run_events_stream` 只是 Redis 临时传输流，不是持久表或历史 owner。

Control 的两个状态机必须分开理解：HTTP admission receipt 只有 `pending/succeeded/failed`，用于
幂等重试与发布结果；`run.control.receipt` 是 Agent event stream 上的执行进度信号，只有
`persisted/applied`，用于 worker recovery 观察。它不是 `chat_messages`，也不应被 BFF 直接查表。

## 入站入口

Root `contract/` 的 `LaunchRunRequest` 是唯一入口语义；当前 Redis worker 使用严格 internal
envelope adapter（把 Root 顶层 `message_id/content` 映射到 `input`），generated consumer/transport
接入后替换映射层。两者不形成第二套业务契约。
Worker 只把这三个业务选择交给 `AgentFactory`：Feature 由 GA 目录决定，身份由 GA 解析为内部
`RuntimeNamespace`，输入交给 DeepAgents。请求不携带 `agent_type`、caller `namespace` 或请求级
Skill/MCP 配方；这些都不是 Session/Client 的配置面。
