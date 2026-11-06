# kokoro-agent 技术方案

状态：当前 GA 实现方案，2026-08-31。

本方案严格以 DeepAgents 为 Agent runtime。LangGraph 只使用其被 DeepAgents 和 official
`langgraph-swarm` 暴露的原生能力；GA 不再创建自己的 compiler、runtime、graph 或 state 抽象。

## 1. 目标链路

```text
Agent v1 Run request
  -> worker 从 Redis 接收并校验
  -> RunRepository claim
  -> FeatureCatalog.get(feature_key)
  -> AgentFactory.build(request)
  -> create_deep_agent(...)                         # 单 Agent
     或 create_deep_agent(...) + create_swarm(...)  # peer handoff
  -> native state/checkpoint
  -> RunRepository、chat_messages、chat_events、workbench
  -> Agent Chat HTTP query boundary -> kokoro-bff Chat module 查询/replay/AG-UI
```

请求表达“进入哪个产品 Feature”和“这次输入什么”；可选模型标签只交给模型选择边界，不表达如何组装 Agent。Feature 是可信的
GA 内部声明，worker 启动时注册；运行中不接收临时 graph JSON、子代理、工具、Skill 或 MCP 配方。

当前 `music` Feature 和真实 provider/model 仍是本地骨架。正式代码的 `model/factory.py`
只负责真实 provider adapter；没有凭证时的确定性循环位于 `tests/support/local_fake.py`，
仅供测试验证，不进入 distribution，也不成为 worker 配置。System 模型解析已在 AgentFactory 实例化模型前接线；CLI必须配置 System/LiteLLM，
RunRequest 保持不变，实际路由由受信 owner 返回，不再解释 caller provider:name。

## 2. 对象与命名

### Agent

`Agent` 是一个完整的、可独立运行的 DeepAgents 能力单元。它可以提供 prompt、固定工具、默认
Skill/MCP 和 native subagent。它不是角色，也不是用户 Session。

### Feature

`Feature` 是对外产品能力的唯一装配入口。它选择一个或多个 Agent，并声明 entry 和 peer
handoff。Feature 可以是单 Agent，也可以是由多个 Agent 组成的产品能力。

```text
music Feature       -> music Agent
chat Feature        -> general Agent
music_chat Feature  -> general Agent + music Agent + declared handoff
```

不创建 `Workflow`、`Team`、`Role` 或额外的编排对象。Feature
本身就是业务组装层。

### AgentFactory

`AgentFactory` 是 GA 内部唯一构造器，worker 启动时持有共享服务，并按 Feature 声明选择构造
路径。运行依赖作为 Factory 实例字段存在，不通过 Feature/Agent API 传递，不使用含义过宽的 `services` 或 `deps` 命名。

```python
factory = AgentFactory(dependencies)
built = await factory.build(request)  # AgentHandle；实际执行对象是 built.runnable
```

Factory 返回只含官方 runnable 与交付说明索引的 `AgentHandle`；Run 执行层从其中取出官方调用
对象，不再定义 GA 自己的图对象。

## 3. 两条构造路径

### 3.1 单 Agent

Feature 只有一个 Agent 时，Factory 直接调用 `deepagents.create_deep_agent`，透传模型、工具、
middleware、backend、checkpointer 和 store。DeepAgents 自己拥有 agent loop、native state、
subagent、interrupt 和 checkpoint。

### 3.2 Peer Swarm

Feature 声明两个或以上需要互相接手会话的 Agent 时：

1. 每个 peer 仍由 DeepAgents `create_deep_agent` 构造；
2. 只为 Feature 声明的边添加官方 `create_handoff_tool`；
3. 交给 `langgraph_swarm.create_swarm`；
4. 使用官方 `SwarmState` 和同一个 outer checkpoint。

GA 不实现 handoff router、不切换 prompt、不维护 `active_agent` 的自有 state。后台隔离工作仍
使用 DeepAgents native subagent，不把两种协作语义混成一个 peer。

直接peer的v3子图投影与native task必须区分：仅Feature已声明直接peer可作为同轮公开正文发布，peer内部task保持子代理边界；异构peer的工具与子代理声明按各自scope装配。handoff正常结束验收以真实peer执行、原Command路由字段完整及同Run持久final绑定为准，不以根图流排空或最后一条AI替代。

2026-10-28锁定SDK兼容缺口尚未关闭：DeepAgents0.6.6文件结果处理会丢Command的graph/goto，且公开用户middleware位置不足以事后修复。落点裁决见本仓TECHNICAL_DESIGN.md本次设计门；无已生效兼容补丁，不宣称多peer正常。

## 4. 目录与依赖

`kokoro-agent/`（仓库/distribution）与 `src/kokoro_agent/`（标准 Python `src layout` 下的 import
package）不是两层业务架构。保留 `src` 隔离，包内只按 GA 的真实职责分目录。

```text
src/kokoro_agent/
├── domain/           领域模型、规则与按 bounded context 放置的 repository port
├── application/      用例、DTO 与 schema operator boundary
├── infrastructure/   PostgreSQL schema、具体 adapter、LangGraph Store/checkpoint
├── interfaces/       HTTP/RPC/event 传输映射
├── agents/          完整、可复用的 DeepAgents Agent 声明
├── features/        对外产品能力与 Agent 组装声明
├── agent_factory.py 唯一内部组装入口，直接调用 DeepAgents
├── swarm.py         official langgraph-swarm handoff 薄接线
├── execution/       Run、control、HITL、事件投影与终态
├── worker/          Redis ingress、共享服务、claim、recovery、drain
├── tools/           GA 固定工具、工具集合与 middleware
├── skills/          Capability Skill 只读 backend adapter
├── clients/         Capability/Storage 窄 client
├── sandbox/         Workbench 与 S3-compatible Workspace adapter
├── mcp/             MCP 配置、连接与 egress
├── model/           模型选择与 provider adapter
├── prompts/         静态提示词资产
└── observability.py metrics、trace、private audit
```

不新增 `ga/`、`factory/`、`framework/`、`ports/`、`compiler/`、`runtime/`、根目录 `agent.py` 或
`deepagents.py` 包装模块、自有 state。`worker` 是 Redis 进程入口；`agent_factory.py` 不是
网络服务入口。

构造辅助职责固定归位：`worker/dependencies.py` 保存 worker 启动时创建的共享资源与部署注入的可选 owner clients；`tools/` 负责
工具解析与 guard chain，但只接收所需窄参数，不反向依赖 `WorkerDependencies`；`prompts/` 只保存静态 prompt 资产；`agents/subagents.py` 负责 DeepAgents
native subagent。`agent_factory.py` 保留构造顺序及唯一的 `create_deep_agent` 调用。

依赖方向：

```text
worker -> features -> agent_factory -> DeepAgents / official Swarm
execution -> infrastructure + narrow public clients
skills/sandbox -> infrastructure + narrow public clients
clients -> owner public APIs through local typed ports
```

## 5. 状态、Session 与身份

- DeepAgents、LangGraph 和 Swarm 拥有各自 native state；GA 不继承或包装它们。
- `ExecutionIdentity` 由入口提供；GA 仅以稳定的 `tenant_ref + subject` 派生内部
  `RuntimeNamespace`。actor/assertion 保留用于授权、审计和计费，不参与隔离键，也不进入 Feature/Agent 选择 API。
- 同一 Session 只有在前一 Run terminal 后，下一次普通调用才继续同一 native checkpoint；
  fork 才建立新的 Session/thread/state。
- Session 不保存 Agent binding、graph version、release ref 或当前 Agent 字段。

LangChain native message/checkpoint ID 与 GA `chat_messages`/`chat_events` ID 分离。GA 不读取
或改造 LangChain checkpoint 表。

## 6. 能力和外部服务

- Run 携带冻结 exact Skill source refs；Platform 当前授权解析，包体每次读取重验后经原生 backend 暴露为 `/.skills/` 只读 route。空选择不查 Skill；非空依赖缺失／读取失败停止，绝不默默删去选择。
- MCP 静态声明使用 `McpConnectionRef`，不是名称或端点。HTTP7.4 候选将 Run 选中 Connection IDs 在 prepare/freeze 前稳定合并至入口 Agent 的不可变副本；catalog 和其它 peer 不变。client 每次按原可信身份、lease/proof 和 Platform 当前资格解析，不携带持久 grant。三件 MCP 工具始终存在，空 refs 不调用 owner。
- 产物由 Agent 显式声明 `delivery=True`。Factory 只在 Storage `DeliveryClient`
  已注入时装配 deliver；工具经同一 DeepAgents backend 读取工作区，由 client
  闭环 upload/asset/artifact，GA 不直写 Storage 内部 key。
- `S3Workspace` 只是 GA Workbench 的 S3-compatible adapter；MinIO 是当前实现，后续可替换。
- CLI 不直读 Platform/Storage 私库；worker 只组装正式 owner client。没有显式选择的可选能力不要求其 client；已有非空选择时缺 client 必须失败，不回退 deployment YAML 或名称猜测。
- 开发可单独验证无外部选择的装配；正式模型执行仍需原 System 路由及 Billing 派发许可，不把缺 client 当免费运行旁路。
- MCP egress 是 worker 级连接策略：`worker.main` 从已校验的 `AppConfig.mcp_egress_mode` 初始化一次，连接层只读取该进程快照，不自行读取环境变量；默认 `strict`，测试 fixture 仅在 `tests/support` 显式使用 `off`。
- 已选择的 Skill/MCP 查询失败会沿原错误与恢复边界终止；不以空集合或另一连接继续，不重写 Session/native checkpoint 或历史请求。

## 7. 事件、聊天与计费

- GA 将用户可见历史写入 `chat_messages`，将安全 replay 事件写入
  `chat_events`。
- LangChain raw event、native state、prompt、secret、sandbox path 和外部响应不会进入产品事件。
- BFF Chat 通过 Agent Chat HTTP query boundary 使用 `chat_events.seq` replay；GA 不把自己的
  internal safe envelope 直写现有 BFF browser stream。
- 计费按 provider accepted invocation 次数结算；token 只用于上下文和限额，不是计费单位。

## 8. 实施顺序

1. Agent 本仓 API 定义 `feature_key`/`ExecutionIdentity`，BFF 通过版本化 HTTP client 消费。
2. 建立 `agents/` 和 `features/`，先实现 `music`、`chat` 两个单 Agent Feature。
3. 建立 `agent_factory.py`，让单 Agent 直接走 `create_deep_agent`。
4. 只有出现真实 peer handoff 需求时才接入 `swarm.py` 和官方 Swarm integration test。
5. 将 RunRepository、聊天事实、Capability/Storage public clients 接入 Factory/worker；不把 owner
   数据库或 bucket 读写写进 Agent。
6. 最后再开放可视化 Builder：Builder 只生成 Feature/Agent 声明，复用同一 Factory，不进入
   Session 或 Run 输入。

开发期 `kokoro-agent inspect [feature] [--json]` 读取同一个 `FeatureCatalog`，用于 CI、调试和未来
Builder 展示。该命令只输出 Agent/Feature 的工具、Skill、MCP、backend 与 handoff 元数据；不输出
prompt 正文、credential、namespace、thread、checkpoint 或任何 Run 状态。

跨仓接线前有一个明确的契约闸：Agent v1 `LaunchRunRequest` 使用顶层
`message_id/content`，Agent v1 `ApplyControlRequest` 使用 `agent_run_id/control_kind`；当前
Redis worker 的 `input`、`run.resume/run.cancel` 是内部 envelope。generated consumers 接入
时必须在 transport 边界完成一次映射，并同时清理 Root 中仍未被 Feature-first 方案使用的
`requested_agent_key`/`manifest_digest` 语义；GA 不为它们创建 Agent 版本或绑定对象。

## 9. 验收标准

- Agent business HTTP v1 已提供 launch、control、Run events evidence、session history 和
  session replay；BFF 只能通过该 HTTP ingress 调用，不读取 Agent PostgreSQL/Redis。
- 匿名例外仅为 exact `GET /healthz` 与 exact `GET|HEAD /v1/execution-proof/jwks`；其余 ingress 始终要求已配置的内部密钥，并校验标准
  `Authorization: Bearer <KOKORO_INTERNAL_SECRET_AGENT>`。未配置密钥返回
  `503 service_auth_not_configured`，认证缺失或错误返回 `401 service_auth_failed`。history/replay 额外校验受信 tenant/subject/actor/identity-assertion
  headers，业务响应使用统一 `{data, meta}` / `{error, meta}` envelope（health endpoint 保留
  轻量 status payload）。
- launch 先持久化不可变 `sha256` fence；同一 `run_id` 重试复用 receipt，body 漂移返回
  `409 run_identity_conflict`；control ingress 使用 `Idempotency-Key` 作为稳定 `command_id`，
  在 PostgreSQL `run_control_commands` 中按 `(run_id, command_id)`/digest 去重，返回 `pending`/`succeeded`/`failed` 与
  `replayed`，worker 继续消费同一 command row，按 command_id keep-first 入账。
- Agent ingress 不实现 BFF session detail、title、share、delete、public snapshot 或浏览器
  SSE/AG-UI；这些仍是 BFF 自己的业务边界。
- `music` 可单独作为 Feature，也能被组合 Feature 复用。
- 多 Agent 只通过 Feature 声明和 official Swarm handoff，不存在自定义 router/state。
- DeepAgents 是唯一 Agent loop；GA 没有第二个 runtime/compiler/graph 层。
- Feature/Agent API 没有 `deps`、namespace、thread、binding 或版本字段。
- Capability、Storage、Studio 等可选旁路短暂不可用时，未声明其操作的 GA 核心仍可运行；Redis、
  RunRepository 与 checkpoint 是当前 worker 执行入口的必要基础设施，不伪装成可选依赖。
- System ModelResolver 已在 worker/AgentFactory 接线，使用 trusted tenant/feature 和可选模型标签；
  实例化前验证可执行路由，无可用路由或凭据时失败关闭。确定性 fake 仅在 tests；live System 联调证据见 ACCEPTANCE。


### 原生文件动作详情（2026-10-28）

既有私有 RunDisplay.activity_details 承接 ls/read_file/glob/grep/write_file/edit_file，HTTP、AG-UI、Schema 不变。
只读取原 Run 的 native checkpoint writes；冻结配方必须证明所有可能 peer 使用 StateBackend，并排除 approval_tools、pause_tools、review_tools 中可能被人工改写的调用。字段缺失不当作空政策；其它 backend 与审批修订来源未证时省略详情，不使用当前文件或模型另行解释历史。

投影绑定 typed AIMessage/ToolMessage、唯一 call ID、原 scope 与 opaque activity ID、实际类别和终态；冲突来源/参数/结果不择首尾。输入仅取该原生工具具名参数，输出仅取原文本结果，沿原有私密过滤、2048/4096字节预览和响应总预算。目录不伪装文件 target；文件目标只承接原活动已经证明的路径。候选以锁定原生序列化名称token筛选，再以typed内容证明，不解码自制Delta；共享32 locator/8MiB/5秒预算保持，纯native路径不加载无关journal。

验证使用实际锁定DeepAgents文件工具及PostgreSQL saver、离线脚本模型；六工具真实返回、重开只读、40个无关checkpoint与129条无关journal、身份/参数冲突及人工策略排除分别覆盖。正常环境是否激活、实际浏览器证据归Root当前事实，不以本说明代替页面验收。
