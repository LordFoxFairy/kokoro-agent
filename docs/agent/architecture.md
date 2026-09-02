# kokoro-agent 最终架构

状态：当前 GA 目标架构，2026-08-31。

这份文档只回答一个问题：GA 如何基于 DeepAgents 提供可复用 Agent，并把它们组装成产品
Feature。DeepAgents 是执行底座；GA 不再包一层自己的 agent runtime、graph compiler 或 state。

## 1. 先锁定四个名词

| 名词 | 含义 | 不承担的职责 |
|---|---|---|
| `Agent` | 一个完整、可独立运行、可复用的 DeepAgent 定义；包含 prompt、固定工具、默认 Skill/MCP 与可选 native subagent | 不代表用户会话、不代表角色、不代表一次 Run |
| `Feature` | 对外产品能力的装配声明；选择一个或多个 Agent，并声明入口和 peer handoff | 不保存 checkpoint、不执行 Redis、不接收临时 graph JSON |
| `AgentFactory` | GA 内部唯一构造器；把 Feature 声明转换为 DeepAgents 原生对象，多个 peer 时调用 official `langgraph-swarm` | 不是 API endpoint、不是 Session 对象、不是 `deps` 注入接口 |
| `Run` | 一次用户调用的执行事实和生命周期 | 不重新选择 Agent，不创建第二套 state |

`Agent` 是能力，`Feature` 是产品组装。Music Agent 可以直接成为 `music` Feature，也可以作为
另一个组合 Feature 的 peer；不需要 composer、arranger、reviewer、Role 或 Team，也不再增加
另一套业务编排目录。

### 命名冻结

以下名字分别对应唯一语义，后续不再用同义词扩层：

| 名字 | 只表示 | 禁止替换成 |
|---|---|---|
| `Agent` | GA 内置的完整 DeepAgents 定义 | `Role`、`Member`、`Persona`、`AgentState` |
| `Feature` | 一个对外产品能力及其 Agent 组合 | `Workflow`、`Team`、`BusinessOrchestration` |
| `AgentFactory` | 将声明转成官方 runnable 的内部装配器 | `Runtime`、`Compiler`、`BuilderRuntime` |
| `AgentHandle` | 当前装配结果的轻量句柄和工具说明索引 | `BuiltAgent`、`Graph`、`State` |
| `ResolvedSkill` | 本次解析得到的 Skill 读取信息 | `SkillGrant`、`SkillBinding`、`SkillVersion` |
| `Run` | 一次调用的生命周期事实 | `Task`、`WorkflowRun`、`SessionPlan` |

`AgentHandle.runnable` 仍然是 DeepAgents/LangGraph 返回的原生对象；`AgentHandle` 不拥有
loop、state、checkpoint 或恢复逻辑。

Factory 内部返回值命名为 `AgentHandle`，表示“指向已装配官方 runnable 的句柄及交付工具索引”，不是
新的 Agent 类型；执行层使用 `AgentRunnable` 和 `NativeStateSnapshot` 两个窄 Protocol，只做
类型边界，不拥有或包装 DeepAgents 的 loop/state。

这里的 `Agent` 仅是 GA 的静态声明；真正执行对象始终是上游
`deepagents.create_deep_agent(...)` 返回的 native runnable。GA 不提供同名构造函数或替代
runtime。

## 2. 唯一运行链路

```text
Agent business HTTP ingress / Root transport
  -> durable dispatch admission
  -> Redis LaunchRunRequest
  -> worker 解析并认领 Run
  -> FeatureCatalog.get(feature_key)
  -> AgentFactory.build(request)
  -> 一个 Agent: create_deep_agent(...)
     多个 peer: create_deep_agent(...) + langgraph_swarm.create_swarm(...)
  -> native DeepAgents/LangGraph state + checkpoint
  -> GA RunRepository / chat facts / workbench
  -> Root Chat query boundary -> BFF Chat 查询、replay、AG-UI/SSE
```

请求只选择可信的 `feature_key` 和本次输入；Root contract 允许的模型标签/trace 仍是旁路元数据，
不参与 Agent 组装。请求不携带 Agent、member、Skill、MCP、graph、namespace 或 worker 依赖。
Feature 的组合在代码/受管目录中声明，worker warm 时注册；运行中不临时改图。

## 3. Agent 与 Feature 的写法

Agent 文件只描述能力，不描述产品流程：

```python
music_agent = Agent(
    key="music",
    prompt=MUSIC_PROMPT,
    tools=(...),
    skills=("music",),
    mcp=("music_provider",),
)
```

Feature 文件只描述产品如何使用 Agent：

```python
music = Feature(
    key="music",
    entry_agent="music",
    agents=(music_agent,),
)

music_chat = Feature(
    key="music_chat",
    entry_agent="general",
    agents=(general_agent, music_agent),
    handoffs=(("general", "music"), ("music", "general")),
)
```

Agent 的默认 Skill/MCP 随 Agent 声明复用；某个 Feature 需要收窄或补充能力时，使用
`agent.configured(skills=..., mcp=..., prompt=...)` 得到该 Feature 的不可变副本。它仍然是
同一个完整 Agent 能力，不引入 Role、Member 或另一种装配对象。

单 Agent Feature 直接运行该 DeepAgent。Feature 需要 peer 之间接手对话时才声明
`handoffs`，由官方 `langgraph-swarm` 生成 handoff tools 和 `SwarmState`。没有 handoff 的多
Agent 配置直接拒绝，避免把 peer、后台 subagent 和固定流程混成一层。

Agent 的 `skills`、`mcp`、prompt 和固定工具是装配输入；Agent 不携带 `ResolvedSkill` 或 MCP
或任何外部授权快照。GA 以 `tenant_ref + subject` 派生稳定内部 `RuntimeNamespace`，actor/assertion
只保留给授权、审计和计费；Factory 在本次构造时把完整 `ExecutionIdentity` 与该 namespace 交给 Skill/MCP client，由 client 返回一次性的读取结果；这些结果只进入当前
DeepAgent 的只读 Skill backend route，不改变 Feature 或 Agent 定义。Factory 将该 route 作为
`skills=["/.skills/"]` 交给 DeepAgents；元数据注入和 `read_file` 渐进读取均由官方 SkillsMiddleware 完成。用户或项目 Skill 的 CRUD 通过 Capability public contract。

## 4. Factory 如何避免 `if` 地狱

`AgentFactory` 在 worker 启动时创建一次，持有模型、checkpointer、RunRepository、workbench 和
clients 等运行依赖。它们是 worker 内部字段，不出现在 Feature/Agent API，也不使用含义过宽的 `services` 或 `deps` 命名；`tools/`、`agents/` 等叶子模块只接收自身所需窄参数，不接收整个 `WorkerDependencies`。

```python
factory = AgentFactory(dependencies)
built = await factory.build(request)  # AgentHandle：官方 runnable + 交付说明索引
runnable = built.runnable
```

Factory 只有两条明确构造路径：

1. `Feature.agents` 只有一个子代理：调用 DeepAgents `create_deep_agent`。
2. Feature 声明 peer handoff：为每个 Agent 调用 DeepAgents `create_deep_agent`，再调用
   `langgraph_swarm.create_swarm`。

`agent_factory.py` 同时拥有构造顺序与唯一的 `create_deep_agent(...)` 调用，不再设置第二个
`factory/` 目录。参数准备按其真实 owner 放置：worker 运行依赖在 `worker/dependencies.py`，工具集合
和 guard chain 在 `tools/`，静态 prompt 资产在 `prompts/`，DeepAgents native subagent 的声明与装配在
`agents/subagents.py`。这些模块不产生 `Graph`/`State`，不保存 Session，也不接收 caller 依赖。

`swarm.py` 中出现的 `.compile(...)` 仅是 `langgraph-swarm` 官方构造器要求的最后一步调用；它不
对应 GA 的 `compiler/` 目录，也不产生 GA 自有的 `CompiledGraph` 类型。

工具面、审批、sandbox 和 Skill backend 接线是 Agent 构造所消费的现有能力，不归入一个
笼统的装配目录；事件投影属于 `execution/`。任何实现文件都不得演化成 GA 自有的 Graph、State、
router 或编译器抽象。部署可通过 `worker.main.serve(config, WorkerClients(...))` 注入 owner public clients；标准 CLI 不直读 Capability/Storage 私库。MCP egress 由 worker 启动时从 `AppConfig.mcp_egress_mode` 初始化，连接层只消费进程级快照（默认 strict，本地 fixture 显式 off）。

## 5. 状态与恢复

- 单 Agent 和 DeepAgents native subagent 使用 DeepAgents 自己的 state/checkpoint。
- Swarm 使用官方 `SwarmState`，`active_agent` 只属于 Swarm checkpoint。
- GA 不继承或包装 DeepAgents 原生 state，也不定义自有 state、prompt-swap middleware 或 router。
- RunScope、租约、计费与外部 owner receipt 写入 GA RunRepository 或 invocation context，不塞进 native state。
- 同一 Session 的下一次普通调用在前一 Run terminal 后继续同一 native checkpoint；fork 才创建
  新 Session、thread 和 state。Session 不保存 Agent 绑定或版本字段。

LangChain 的 `Message.id`、`thread_id`、`checkpoint_id` 与 GA 的 `chat_message_id`、
`chat_event_id` 完全分离。GA 不读取或改造 LangChain checkpoint 表。

## 6. 目录

这里的 `src/kokoro_agent/` 是 Python 标准 `src layout`：仓库/发行包为 `kokoro-agent`，可导入包为
`kokoro_agent`。它只解决打包和 import 隔离，不表示 GA 内部又嵌套了一层架构。

```text
src/kokoro_agent/
├── agents/          完整、可复用的 DeepAgents Agent 声明
├── features/        对外产品能力与 Agent 组装声明
├── agent_factory.py 唯一内部组装入口，直接调用 DeepAgents
├── swarm.py         official langgraph-swarm handoff 薄接线
├── execution/       Run、control、HITL、事件投影与终态
├── worker/          Redis ingress、共享服务、claim、recovery、drain
├── tools/           GA 固定工具、工具集合与 middleware
├── skills/          Capability Skill 只读 backend adapter 与本地 fixture reader
├── clients/         Capability/Storage 窄 client
├── sandbox/         Workbench 与 S3-compatible Workspace adapter
├── repositories/         RunRepository port、运行结果模型、schema；不放数据库驱动
├── infrastructure/       PostgreSQL RunRepository adapter、LangGraph Store 与 checkpoint adapter
├── mcp/             MCP 配置、连接与 egress
├── model/           模型选择与 provider adapter
├── prompts/         静态提示词资产
└── observability.py metrics、trace、private audit
```

`deepagents` 是外部运行底座，不在 `src/kokoro_agent/` 下复制成 `deepagents.py`。
包内也不设置 `factory/`；`agent_factory.py` 是唯一 Agent 构造面。
`model/` 只包含真实 provider adapter；离线确定性模型属于测试夹具，放在
`tests/support/local_fake.py`，不进入正式 distribution，也不成为 worker 配置项。
`worker` 是 Redis 入口；`agent_factory.py` 不是服务入口。Compose/Kubernetes 只提供连接和
secret handle，不决定 Feature、Agent 或 peer 关系。

## 7. 设计收益

1. DeepAgents 的 loop、state、subagent、checkpoint、interrupt 直接复用，不维护平行实现。
2. Feature 可以只暴露一个 Agent，也可以把多个 Agent 组装为一个产品能力；同一 Agent 可复用。
3. 动态组装集中在 Feature 注册/Builder -> AgentFactory，避免 worker 中散落条件分支。
4. Swarm 只解决 peer handoff；需要后台隔离任务时使用 DeepAgents native subagent，不混用两种
   协作语义。
5. 外部 client 缺席时，未声明外部操作的 Feature 仍能执行；GA 核心闭环不依赖 Capability、
   Storage 或 Studio 的内部实现。

## 8. 可检查能力面

`kokoro-agent inspect` 是只读开发入口，直接遍历 worker 使用的同一个 `FeatureCatalog`。它不是
管理 API、动态配置中心或另一种 Manifest owner；JSON 输出可以供 CI 和未来可视化 Builder 使用，
但 caller 仍然只能在 LaunchRun 中选择可信 `feature_key`。

检查输出刻意排除 prompt 正文、secret、ExecutionIdentity、RuntimeNamespace、Session/checkpoint
定位和运行状态，避免诊断能力变成配置或数据泄漏面。
