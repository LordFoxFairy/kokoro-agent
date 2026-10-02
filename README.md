# kokoro-agent

Kokoro 的 GA 执行底座：直接使用 DeepAgents 的 agent loop、state、checkpoint 和 interrupt；需要多个
peer 接手会话时使用官方 `langgraph-swarm`。Agent HTTP ingress 接收本仓 v1 Run 请求，worker 从 Redis
接收本仓 `protocol/control.py` 定义的严格内部命令，按 `feature_key` 取得 Feature，执行一个或多个 Agent，
并将用户可见结果写入 `chat_messages/chat_events`。

本仓自己维护 [Agent v1 API](docs/agent/api-contract.md)、`src/kokoro_agent/protocol/` 和 contract tests。
Root 不生成或发布 Agent 协议。BFF 只通过版本化 HTTP 查询历史、提交 control 和 replay，再由 BFF 自己投影
AG-UI/SSE；本仓只提供执行事实和恢复边界，不复制 BFF、Capability 或 Storage 的数据库模型。

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
├── protocol/        Agent-owned command/event/Redis wire；不包含其他 owner 的模型
├── domain/          领域模型、规则与按 context 放置的 repository port
├── application/     用例编排、DTO 与 schema operator boundary
├── infrastructure/  PostgreSQL、Redis、checkpoint 与外部技术 adapter
├── interfaces/      HTTP/RPC/event 传输映射
├── execution/       Run、control、HITL、事件投影与终态
├── worker/          Redis ingress、共享服务、claim、recovery、drain
├── tools/           GA 固定工具、每次运行的工具集合与 middleware
├── skills/          Capability Skill 只读 backend adapter
├── clients/         Capability/Storage 窄 client（Skill、MCP、Artifact 交付）
├── generated/       固定 owner Proto 派生的只读 Python Protobuf/Connect 客户端
├── sandbox/         Workbench 与 S3-compatible Workspace adapter
├── mcp/             MCP 连接、工具与部署配置适配
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
  -> Agent Chat HTTP query boundary -> kokoro-bff Chat API/AG-UI
```

- 外部请求携带 `ExecutionIdentity`，只通过 selected_skill_source_refs 携带冻结 exact Skill 选择，不携带 caller namespace、thread、Agent、MCP 或 graph 配方；GA 内部按 `tenant_ref + subject` 派生稳定 `RuntimeNamespace`；actor/assertion 只用于授权、审计和计费。
- DeepAgents 的 native state 与 official `SwarmState` 都由框架拥有；GA 不定义自己的 State 包装。
- Run 显式选择的 Skill exact refs 由 Platform current authorization 解析；每次文件读取重验批准引用与 Storage signed GET/ZIP，再暴露只读 backend。Agent 不声明静态Skill名称，空选择无Skill依赖；安装/启用仍由Platform拥有。
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
KOKORO_REDIS_URL=redis://127.0.0.1:56380/9 \
  KOKORO_AGENT_DATABASE_URL=postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent \
  KOKORO_AGENT_DATABASE_SCHEMA=kokoro_agent \
  KOKORO_SYSTEM_BASE_URL=http://127.0.0.1:4240 \
  KOKORO_INTERNAL_SECRET_AGENT=... \
  KOKORO_LITELLM_ENABLED=1 KOKORO_LITELLM_BASE_URL=http://127.0.0.1:4000/v1 \
  KOKORO_LITELLM_API_KEY=... uv run kokoro-agent-worker
```

BFF business ingress 与 worker 分进程运行；两者都只使用本仓自己的 PostgreSQL/Redis：

```bash
KOKORO_REDIS_URL=redis://127.0.0.1:56380/9 \
  KOKORO_AGENT_DATABASE_URL=postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent \
  KOKORO_AGENT_DATABASE_SCHEMA=kokoro_agent \
  KOKORO_INTERNAL_SECRET_AGENT=... KOKORO_AGENT_HTTP_PORT=4401 \
uv run kokoro-agent-http
```

以上是本地 profile 的共享依赖基线：复用 `127.0.0.1:55433` 的 PostgreSQL，并只使用 Agent 自己的
`kokoro_worker_agent` database；复用 `127.0.0.1:56380` 的 Redis logical DB `9`。启动前先探测现有实例，
不要为 Agent 重复启动 PostgreSQL 或 Redis。CI 继续使用 workflow 显式注入的 service 端口，不继承本地默认值。

Agent 是可选执行 profile，不是 Web/BFF 的启动前置条件。最小本地 profile 只启动 Web、BFF
和它们的 PostgreSQL/Redis；此时 BFF readiness 仍可通过，Chat/调度执行路由返回稳定的
`agent_not_configured`。启用完整执行 profile 时必须同时运行 `kokoro-agent-http`（BFF 的
durable ingress）和 `kokoro-agent-worker`（实际执行 loop）；只运行 HTTP 进程只能完成 admission，
不会执行任务。

启用 worker 时，必须配置 System 模型解析及 LiteLLM 网关：`KOKORO_SYSTEM_BASE_URL`、
`KOKORO_INTERNAL_SECRET_AGENT`、`KOKORO_LITELLM_ENABLED=1`、`KOKORO_LITELLM_BASE_URL`、
`KOKORO_LITELLM_API_KEY`。System 当前 executable transport 仅 litellm；缺配置启动失败，
每次构造模型先解析可信 tenant/feature/label，拒绝本地名称或默认 provider 兜底。
Agent 不包含 LiteLLM Python 包、不启动网关、不接收底层 provider 凭据。单独的 Agent HTTP ingress
仍可接收 durable admission；模型解析失败只终止对应 Run，不让假路由进入执行。

HTTP ingress 不执行 Agent loop，也不直接暴露 Redis stream。当前 v1 业务入口是：

- `POST /v1/runs`：launch，先写 durable dispatch admission，再投递给 `kokoro-agent-worker`；
- `POST /v1/runs/{run_id}/control`：cancel/resume/steer；
- `GET /v1/runs/{run_id}/events`：Run evidence；
- `GET /v1/sessions`：按 trusted identity 查询持久化 session list，支持 `project_ref`、`limit`、`cursor`；
- `GET /v1/sessions/{session_id}/messages`：安全 session history；
- `GET /v1/sessions/{session_id}/events`：安全 session replay；
- `GET|HEAD /v1/execution-proof/jwks`：匿名 internal-owner Ed25519 public ring（无 body/query/identity）。

BFF 只通过这些版本化 HTTP 入口访问 Agent，不读取 Agent PostgreSQL/Redis、checkpoint、
RunRepository 或内部 Python 类型。除 `/healthz` 与 exact JWKS GET/HEAD 外的请求始终要求配置可信的
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

跨仓联调由 Root 的 loopback E2E 编排验证；本仓的 API、protocol、类型、测试与构建在本仓闭环，不依赖 Root 的生成器，也不复制其它仓库的源码、数据库或测试实现。

## 关键不变量

- Agent HTTP4 保持 `/v1`；机器契约与 typed protocol 一致。Chat 事实以 `chat_event_id + seq` 保证幂等与顺序；
  字段省略/null 按 schema 区分，`pause_ref`、`action_result` 为必需 nullable 字段。
- Run/command 的 durable admission、lease/fence 与持久状态决定执行资格；Redis 投递和 ACK 不是执行或恢复事实。
  HITL waiting 保存完整 pending 集合，unknown 不自动重投。
- 终态由 `finalize_terminal` 在受信 authority 下同事务完成 Run、command、usage、terminal interaction、终态 Chat/outbox；
  终态重放不新增事实。
- HITL 使用正式 saver 与独立 reader 的持久证据，resume 必须匹配 pause revision/ref 和完整 item 集合；
  仅 `StartedResume` 允许一次 native 调用，HTTP ACK/普通活动不解除等待。
- 第三方类型豁免锁死于 `tests/contract/test_boundary_pragmas.py` allowlist，
  行内 `type: ignore` 全仓为零（同测执法）。
- 异常 → `run.failed` 终态 fail-loud，worker 存活（单消息隔离，不崩调度循环）。

> 注：本仓走 aliyun 镜像，`uv run` 后 `uv.lock` 可能被改写——非依赖变更时 `git checkout uv.lock`；
> 真依赖变更用 `UV_NO_CONFIG=1 uv lock`。

### Execution-proof public keys

`kokoro-agent-http` now uses the independent `kokoro_agent.interfaces.http.main:main` root. It can publish the anonymous internal-owner `GET|HEAD /v1/execution-proof/jwks` from an env-only public ring; an absent or invalid ring degrades JWKS/readiness while `/healthz` remains live. The worker does not load this public ring, and its private-key loader is not wired until the A2c statement-time supplier exists.

## Typed Skill package读取

固定Platform v4机器合同，`selected_skill_source_refs=[]`继续基础Chat；非空必须有worker Platform七项配置与
`KOKORO_STORAGE_OBJECT_ORIGIN`。此origin可单独用于GET，不要求Storage写secret；产物写入另需原URL+secret+origin。
当前包GET严格不带owner/用户凭据、不缓存包或授权。v4激活及用户安装/启用完整产品链仍待后续owner，见[当前实现](docs/CURRENT.md)。

## 配置参考

- `agent.example.full.yaml` 是安全的解析参考样本；通过 `KOKORO_AGENT_CONFIG` 指向它即可检查配置树映射。
- API key、服务凭据、Storage 签名材料与内部认证字段只从环境变量或 secret 注入，不写入样本；样本中的域名仅用于解析示例。
