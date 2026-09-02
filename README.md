# kokoro-agent

Kokoro 的 GA 执行底座：直接使用 DeepAgents 的 agent loop、state、checkpoint 和 interrupt；需要多个
peer 接手会话时使用官方 `langgraph-swarm`。GA worker 从 Redis 接收 Root generated
`LaunchRunRequest`，按 `feature_key` 取得 Feature，执行一个或多个 Agent，并将用户可见结果写入
`chat_messages/chat_events`。

Root `contract/` 是 API/AIP 跨仓契约唯一来源；当前 Redis worker 使用严格 internal
envelope adapter（Root `LaunchRunRequest` 的顶层 `message_id/content` 在 Redis 中位于
`input`），generated consumer/transport 接入后替换映射层，不在本仓复制契约。GA 不面向浏览器，
`kokoro-bff/modules/chat` 通过版本化 Chat contract 查询历史、订阅 replay 并投影 AG-UI/SSE；
本仓只提供执行事实和恢复边界。

## 目标目录（按真实职责）

`kokoro-agent/` 是 Git 仓库和 Python distribution，`src/` 是 import isolation 边界，
`kokoro_agent/` 才是 `import kokoro_agent` 对应的 Python package。三者属于打包结构，不是三层
业务架构；保留标准 `src layout` 可以阻止测试误用仓库根目录中的未安装代码。

```text
src/kokoro_agent/
├── agents/          完整、可复用的 DeepAgents Agent 声明
├── features/        对外产品能力与 Agent 组装声明
├── agent_factory.py 唯一内部组装入口，直接调用 DeepAgents
├── swarm.py         official langgraph-swarm handoff 薄接线
├── execution/       Run、control、HITL、事件投影与终态
├── chat/            GA chat_messages/chat_events 与安全产品投影（供 BFF Chat 使用）
├── worker/          Redis ingress、共享服务、claim、recovery、drain
├── http/             BFF business ingress；durable admission、control、safe replay
├── tools/           GA 固定工具、每次运行的工具集合与 middleware
├── skills/          Capability Skill 只读 backend adapter 与本地 fixture reader
├── clients/         Capability/Storage 窄 client（Skill、MCP、Artifact 交付）
├── sandbox/         Workbench 与 S3-compatible Workspace adapter
├── persistence/         RunRepository、LangGraph Store 与 checkpoint adapter
├── mcp/             MCP 连接、工具与本地 fixture
├── model/           模型选择与 provider adapter
├── prompts/         静态提示词资产
└── observability.py metrics、trace、private audit
```

## 入口与边界

```text
Redis LaunchRunRequest
  -> worker ingress
  -> identity normalization + RunRepository claim
  -> FeatureCatalog[feature_key]
  -> create_deep_agent | official Swarm
  -> native state/checkpoint + GA RunRepository/workbench
  -> chat_messages/chat_events durable write
  -> Root Chat query boundary -> kokoro-bff/modules/chat Chat API/AG-UI
```

- 外部请求携带 `ExecutionIdentity`，不携带 caller namespace、thread、Agent、Skill、MCP 或 graph 配方；GA 内部按 `tenant_ref + subject` 派生稳定 `RuntimeNamespace`；actor/assertion 只用于授权、审计和计费。
- DeepAgents 的 native state 与 official `SwarmState` 都由框架拥有；GA 不定义自己的 State 包装。
- Agent 声明的 Skill 由 Capability public contract 解析；GA 仅把获准包体暴露为当前 Run 的只读 backend route，用户/项目/会话 Skill CRUD 仍属于 Capability。
- `chat_events` 是 GA 安全事件事实和 replay 游标；它不写入 BFF 已有的
  browser-live stream，因为两者的 generated envelope 和 seq owner 不同。不创建
  `conversation_messages`、持久 `run_events` 或独立 `event_outbox`。
- 模型按 provider accepted invocation 次数计费；Billing 通过 `invocation_id` 幂等结算，非 token 计费。

## 子仓文档

- [GA 当前边界](docs/agent/current-boundary.md)
- [API/AIP 契约摘录](docs/agent/api-contract.md)
- [GA 技术方案](docs/agent/technical-plan.md)

## 运行

```bash
uv sync
# Worker 使用已配置的真实 provider；模型凭据只通过环境变量或 secret 注入：
KOKORO_REDIS_URL=redis://127.0.0.1:6379/10 \
  KOKORO_AGENT_DATABASE_URL=postgresql://localhost/postgres KOKORO_AGENT_DATABASE_SCHEMA=kokoro_agent \
  ANTHROPIC_API_KEY=... uv run kokoro-agent-worker
```

BFF business ingress 与 worker 分进程运行；两者都只使用本仓自己的 PostgreSQL/Redis：

```bash
KOKORO_REDIS_URL=redis://127.0.0.1:6379/10 \
  KOKORO_AGENT_DATABASE_URL=postgresql://localhost/postgres KOKORO_AGENT_DATABASE_SCHEMA=kokoro_agent \
  KOKORO_INTERNAL_SECRET_AGENT=... KOKORO_AGENT_HTTP_PORT=4401 \
uv run kokoro-agent-http
```

Agent 是可选执行 profile，不是 Web/BFF 的启动前置条件。最小本地 profile 只启动 Web、BFF
和它们的 PostgreSQL/Redis；此时 BFF readiness 仍可通过，Chat/调度执行路由返回稳定的
`agent_not_configured`。启用完整执行 profile 时必须同时运行 `kokoro-agent-http`（BFF 的
durable ingress）和 `kokoro-agent-worker`（实际执行 loop）；只运行 HTTP 进程只能完成 admission，
不会执行任务。

LiteLLM 同样是可选的外置 OpenAI-compatible gateway。Agent 不包含 LiteLLM Python 包，也不
启动 LiteLLM 进程；只有在 `KOKORO_LITELLM_ENABLED=1` 且同时配置
`KOKORO_LITELLM_BASE_URL`、`KOKORO_LITELLM_API_KEY` 时，Model 的 `litellm` transport 才会
被 Agent 使用。没有 LiteLLM 时可使用直接的 OpenAI-compatible 或 Anthropic provider；Model
目录服务也不会因为 LiteLLM 未部署而启动失败。

HTTP ingress 不执行 Agent loop，也不直接暴露 Redis stream。当前 v1 业务入口是：

- `POST /v1/runs`：launch，先写 durable dispatch admission，再投递给 `kokoro-agent-worker`；
- `POST /v1/runs/{run_id}/control`：cancel/resume/steer；
- `GET /v1/runs/{run_id}/events`：Run evidence；
- `GET /v1/sessions`：按 trusted identity 查询持久化 session list，支持 `project_ref`、`limit`、`cursor`；
- `GET /v1/sessions/{session_id}/messages`：安全 session history；
- `GET /v1/sessions/{session_id}/events`：安全 session replay。

BFF 只通过这些版本化 HTTP 入口访问 Agent，不读取 Agent PostgreSQL/Redis、checkpoint、
RunRepository 或内部 Python 类型。除 `/healthz` 外的请求始终要求配置可信的
`KOKORO_INTERNAL_SECRET_AGENT`，并必须带标准 `Authorization: Bearer <secret>`；未配置 secret
时请求返回 `503 service_auth_not_configured`，认证缺失或错误时返回 `401 service_auth_failed`。
control 还必须带 `Idempotency-Key`；history/replay 还要带
受信的 tenant/subject/actor/identity-assertion headers。响应统一为
`{data, meta:{request_id}}` 或 `{error:{code,message}, meta:{request_id}}`（health endpoint
保留轻量 status payload）；launch 以不可变 `sha256` fence 对同一 `run_id` 幂等，body 漂移
返回 `409 run_identity_conflict`，control 由 PostgreSQL command ledger 按 `command_id`/request digest
去重并返回 `pending`/`succeeded`/`failed` 与 `replayed`，worker 使用 resume fingerprint
去重/恢复。

Agent ingress 不提供 BFF 的 session detail、title、share、delete、public snapshot 或
浏览器 SSE/AG-UI；这些仍是 BFF 自己的业务边界，不应通过直读 Agent PG/Redis 实现。生产环境
应分别配置健康检查和滚动停机，不把 HTTP ingress 和 worker 合并成一个容器进程。

部署时：

- Compose/Kubernetes 只注入 PostgreSQL、Redis、GA checkpoint/RunRepository、sandbox/workbench、模型和可选 public-client handle；不定义 Feature 或 Agent 组合。
- Feature 目录在 worker 启动时加载；Agent 声明的 Skill 在构造时解析，并由 DeepAgents 原生 SkillsMiddleware 渐进读取。
- `music` 与真实 provider/model 仍是本地骨架；provider 由 `model/factory.py` 统一适配。LiteLLM
  仅是显式开启时使用的外置路由，不是 Agent 或 Model 的必需进程。
- 没有 provider 凭证的离线循环只在测试中使用 `tests/support/local_fake.py`；它不属于正式包，也不提供 worker 运行时开关。
- MCP 连接的 egress 策略在 worker 启动时从 `KOKORO_MCP_EGRESS_MODE` 解析一次（默认 strict）；连接层不再读取进程环境。
- `ExecutionIdentity` 由 BFF Chat/IAM 提供，GA 自己派生 `RuntimeNamespace`；不在 wire 中传 `namespace`、用户 ID 或 workspace ID。

## 能力与验证

GA 的能力通过 `Feature -> Agent(s)` 组织：Music Agent 可单独作为 `music` Feature，也可被其他 Feature
复用；只有确有 peer handoff 价值时才使用 official Swarm。未来可视化 Builder 只生成同一套 Feature/Agent
声明，不引入第二套 runtime。

开发期可检查当前受管能力面；输出只包含声明元数据，不包含 prompt、secret、namespace、thread
或运行状态：

```bash
uv run kokoro-agent inspect
uv run kokoro-agent inspect music_chat --json
```

## 门禁与验证

```bash
uv run ruff check .
uv run pyright
uv run pytest
uv run pytest -o addopts='' tests/unit tests/contract tests/integration tests/e2e tests/acceptance
uv run kokoro-agent inspect --json
```

`uv run pytest` 是默认快速门禁，包含 unit 与 contract，排除需要真实服务的
integration、e2e 和 HTTP acceptance。最后一条 pytest 命令是完整 Agent 服务门禁，必须在
PostgreSQL + Redis fixture 可达时运行；缺少服务会由 fixture fail-loud，而不是静默得到假绿。
HTTP owner 的接口、fixture 要求和验收证据见 [`ACCEPTANCE.md`](ACCEPTANCE.md)。

跨仓联调由 Root 的 contract/goal2 门禁负责；本仓不依赖 Root 的旧验证脚本，也不把其它仓库的
源码、数据库或测试实现复制进来。

## 关键不变量

- wire 词汇 = Root `contract/` 生成物；GA ProductEvent 写入 `chat_events`，由 `chat_event_id + seq` 保证幂等与顺序；
  契约 optional 字段缺席=省略（exclude_none），null 永不上 wire。
- 请求流 XREADGROUP 消费、parse 后即 XACK；崩溃恢复权在 RunStateStore TTL 租约，
  HITL 暂停置哨兵永不被重拾重跑，其 control 监听由存活 worker 心跳收养。
- claim-before-emit：cancel/自然完成/异常三路共用同一原子认领键，恰好一个终态事件。
- HITL 帧构造唯一在 `execution/approvals.py`：resume 按 tool_id fail-loud 对齐，
  `tool.awaiting_approval` 携带 `pending_tool_ids`（同帧凑齐才提交的契约依据）。
- 第三方类型豁免锁死于 `tests/contract/test_boundary_pragmas.py` allowlist，
  行内 `type: ignore` 全仓为零（同测执法）。
- 异常 → `run.failed` 终态 fail-loud，worker 存活（单消息隔离，不崩调度循环）。

> 注：本仓走 aliyun 镜像，`uv run` 后 `uv.lock` 可能被改写——非依赖变更时 `git checkout uv.lock`；
> 真依赖变更用 `UV_NO_CONFIG=1 uv lock`。
