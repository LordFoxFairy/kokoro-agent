# kokoro-agent 当前实现

状态日期：2026-09-27。本文件只记录当前代码、canonical schema、contract 和已执行证据；目标值与未来
设计分别见 `SLO.md`、`TECHNICAL_DESIGN.md` 和 ADR。

## 已落地

- Agent 使用 DeepAgents 原生 loop/state/checkpoint；GA 只有一个构造入口 `agent_factory.py`。
- Redis launch/control 是可重放通知；PostgreSQL 保存 dispatch intent、lease generation、receipt、
  outbox、chat facts、tool journal 和 cleanup intent。
- `database/schema.sql` 是唯一当前 DDL；没有 `database/migrations`、迁移 ledger、外键或跨仓 SQL。
- 数据库时间列使用 `TIMESTAMPTZ(3)`；PostgreSQL adapter 在数据库与内部 epoch-millisecond 边界间转换。
- HTTP ingress 先做 service bearer 与 trusted identity 校验，再打开 PostgreSQL/Redis；控制命令使用
  `Idempotency-Key` 和 request digest；Run 查询按 trusted tenant 与派生 namespace 双重 predicate 隔离，
  chat 查询只接受同一 identity 派生的 namespace。
- worker 具备 dispatch CAS、lease generation fencing、终态 claim、outbox republish、control reapply、
  sandbox cleanup retry 和 graceful drain。
- Skill/MCP/Storage 目前只有窄 client port；Agent 不读取 Platform/Storage 私库。Platform owner 已在
  `apps/kokoro-capability` 物理仓 main `ee25c1f4d6df08be183ca10f7f5e852e0b21f641` 发布 inactive
  `kokoro.platform.v1`。本仓现已 pin 两份只读 Proto 输入并生成 Python Protobuf/Connect async client，
  但尚未 pin execution-operation projector、实现业务 adapter 或接入 worker Connect transport。
- 生产发行包不包含本地 MCP/Skill fixture；缺少可选 Capability 时使用显式 `None`/unavailable
  状态，不组装伪 client。LangGraph checkpoint locator 使用受信 identity 派生 namespace 加 session
  id；本地 profile 默认复用 `127.0.0.1:55433/kokoro_worker_agent` 和 Redis
  `127.0.0.1:56380/9`，并设置连接/读写超时；CI 由 workflow 显式注入 service 地址。
- canonical schema 的 operator use case 位于 `application/schema.py`；`cli.py` 与 `worker/main.py` 从该稳定边界导入，
  不再让 CLI 依赖 worker transport。
- OpenAPI、protocol model、canonical database schema、contract test 和 provenance 已进入本仓。
- `createRun` 202 与 `replaySessionEvents` 200 已绑定 Agent owner OpenAPI 的
  `LaunchReceiptEnvelope`/`ReplayPageEnvelope`；两者分别引用既有 `LaunchReceipt`/`ReplayPage`，
  不再以泛型 `DataEnvelope.data={}` 描述。HTTP dispatch 的 202/200 实际字段、泛型回退拒绝及
  provenance digest 由 owner 测试与 checker 校验；未改运行时响应字段和数据库。
- 实际模型 `output_message.text=""` 现在仍发布一次 `message.completed(content="")`，并沿
  原有 `assistant.completed` Chat 投影持久化/replay；不发空 `message.delta`，没有模型终值
  且没有文本 delta 时也不虚构完成帧。真实 DeepAgents v3 离线模型（草稿+工具→空工具段→空最终段）
  经 dispatch claim、Run outbox/lease fence、Redis 及 PostgreSQL Chat 投影后按模型/工具因果顺序重放；
  HTTP replay 的 `seq` 与 owner event `index` 已对照。过期 lease 的空完成帧不会进入 Redis/Chat；
  四路完全独立的 FakeRunStream 不模拟上游 v3 跨投影时序，未将其工具相对顺序当作生产保证。
- Execution proof A1 已发布 Draft 2020-12 decoded-profile schema 与跨语言 canonical/negative/one-bit-tampered vectors；checker 以硬编码
  有序 owner inventory、逐 artifact digest、aggregate digest、strict duplicate/token parser、expected schema pointer/keyword、RFC 8785、
  canonical unpadded base64url/JTI、16 KiB 上限和单差异负向语义校验防止漂移。A2a 独立 runtime exact profile 与
  Ed25519 signer 已通过 SPEC/QUALITY 与 Root 验证，并以 A1 positive vector 和第二个 RFC 8032 KAT
  固定数学签名；A2b 已实现 HTTP route、private loader 与 JWKS snapshot。A2c 已实现 standalone proof 专用 statement-time lease reader 与 immutable run-scoped supplier；IAM owner verifier 已另仓发布，但本仓 worker gate、Platform client 与真实传输仍未实现。

## 当前证据

以最近一次主工作区验证为准，提交前重新执行：

```bash
uv run ruff check src tests
uv run pyright
uv run pytest -q
uv run kokoro-agent-contract-check
uv build --wheel --sdist
```

真实 PostgreSQL/Redis 验收必须显式提供 `KOKORO_AGENT_DATABASE_URL` 和 `KOKORO_REDIS_URL`，不能用内存
替身代替 integration/acceptance。

2026-09-23 本轮 Agent contract 切片已执行 `uv lock --check`、`uv run ruff format --check .`、
`uv run ruff check src tests`、`uv run pyright`、`uv run pytest -q`（1093 passed、6 skipped、
163 deselected）、`uv run kokoro-agent-contract-check` 与 `uv build --wheel --sdist`，均通过。
本轮 HTTP 202/200 验证为进程内 fake ports 的真实 dispatcher 路由，不冒称 PostgreSQL/Redis
acceptance；真实依赖验收留给 Root 隔离组合切片。

2026-09-24 空最终 segment 修复：TDD 先见 2 个旧行为失败、实现后对应 4 个聚焦用例通过；
随后“无模型终值且无文本”负向用例先失败，再增加不虚构完成帧的 guard。当前切片
`uv lock --check`、`uv sync --frozen`、`uv run ruff format --check .`（223 files）、
`uv run ruff check src tests`、`uv run pyright`（0 errors）、`uv run pytest -q`
（1100 passed、6 skipped、164 deselected）、`uv run kokoro-agent-contract-check`、
`uv build --wheel --sdist` 均通过。复用本地一个 PostgreSQL/Redis 实例、各次独立 schema 与
Redis DB 14 的 3 个针对性真实 acceptance 通过；测试后 DB 14 key 数及临时 schema 数均为 0。
这只证明 Agent owner 的空完成与持久 replay；BFF 对该事件的最终文本消费仍须由其 owner 验收。

## 仍需收敛的工程项

1. 历史包目录中的部分执行编排仍较大，需按 use case、repository adapter、outbox 和 supervisor 生命周期
   语义拆分，不能按行号机械切割。
2. `domain/`、`application/`、`infrastructure/`、`interfaces/` 是当前目标架构边界；叶子运行模块按真实职责保留，
   新代码不得恢复顶层 `repositories/`、`services/` 或 `http/` 重复入口。
3. Platform/Storage 真实 client 的生产装配仍需落地；已声明 Platform 能力缺配置或 owner 不可用时
   必须 fail closed，不创建伪实现或以空列表/部署 YAML 作为授权。无外部声明的基础 Run 不要求
   Platform 调用。Storage 自身仍按其 owner contract 决定可选路径。
4. CI/release 的 action SHA、镜像 digest、SBOM、provenance、签名和候选镜像 health gate 需要全部落地。
5. 内部 HTTP DTO 的时间字段仍是 epoch milliseconds；对外 BFF/AG-UI 投影必须转换为 RFC 3339 UTC，
   并在协议升级切片中删除重复时间语义。
6. Agent execution proof A2a 与 A2b 已通过既定门；A2c 已包含 proof 专用 direct PostgreSQL statement-time reader 和 run-scoped supplier。
   private loader 仍未装配进 worker，生产 Skills/MCP client 也没有 supplier；
   production signer call site 仅 standalone supplier 一个；runtime/client transport consumer/composition 为零。
   因此 standalone reader/supplier 通过不等于 proof 已传输，不得跳过真实 client 接线门。官方 Connect Beta 在取消时把
   `CancelledError` 包成 `ConnectError(canceled)`；下一片正式 adapter 必须恢复 asyncio 取消语义并以测试锁定，
   本 generated-only 切片不宣称已满足该运行时约束。
7. IAM owner main `a4c2b61467f1fc1772d6b6d8e98f081c090289fb` 已发布 execution verifier 与 Platform
   workload token ingress/OpenAPI SDK `0.6.0`；上文 2026-09-12 的“IAM verifier 待实现”只作历史阶段记录。
   Agent 代码仍未消费它们。`Agent.skills`/`Agent.mcp`、`SkillClient.resolve`/`McpClient.resolve` 当前只传字符串名称；
   Platform `ResolveVisibleSkill` 只接受 typed `SkillSourceRef`，MCP 资源按 typed connector/connection ID。
   声明来源尚不能供给这些 ID；`DiscoverVisibleSkills.query` 不是名称到精确 ref 的替代解析器。
8. `agent_factory.resolve_declared_skills` 在已声明 Skill 的 owner 读取失败时静默回空列表；
   `tools/toolset.py` 在 MCP client 缺失/失败时保留部署配置路径。这是待删除的旧 Capability 行为，
   不算 Platform current authorization。没有声明外部能力的基础 Run 不受此缺口影响；已声明能力在目标态必须 fail closed。

这些条目是代码工作的清单，不以文档声明替代实现或验证。

## System 模型解析消费切片（2026-09-08，消费者切片静态/单元已验）

System HTTP client已注入真实worker/AgentFactory，先解析可信tenant/feature/可选label，再构造DeepAgents模型；
移除select_model_label与anthropic/claude兜底。CLI需System服务凭据及LiteLLM配置，嵌入部署可显式注入ModelResolver。
System revision/digest/generation记结构化日志；本仓DB与公开Run wire未变，源契约pin见contract/provenance.json。
尚未运行live System/网关smoke，不声称完整执行链路通过；本轮验证见ACCEPTANCE，commit随Root交付记录。

## Agent execution proof 设计门（2026-09-11，已验收）

目标边界已记录在 TECHNICAL_DESIGN §7、API_CONTRACT、DATA_MODEL、SECURITY 与 ADR-004：Agent 将拥有 exact v1 proof
schema/canonical bytes、每次调用前的 database-clock lease gate、Ed25519 signer、分离的 worker private-key/HTTP public-ring 配置和
`GET|HEAD /v1/execution-proof/jwks`。数据库与 Redis 均不变化；Skills/MCP typed opaque identity 仍属于 Platform，proof 不携带资源 ID。

IAM `bf160be173ef473bebe8e4a93b74ec52c230f180` 已对齐 owner、proof/JWKS 方向、TTL/skew 与 current authorization，但仍只写正整数
`lease_generation`，尚未纳入本 R2 的 safe-integer/token 拒绝矩阵，且交付段尚未拆成下面六个独立门。该差异属于第 2 步的显式输入；
在 IAM 文档、verifier、OpenAPI/SDK 和 consumer tests 固定消费 Agent artifact 前，不记录为跨仓契约已对齐。

2026-09-12 的 A1 切片已新增 owner machine schema/vectors、contract test、checker/provenance gate，并直接声明
`jsonschema>=4.26.0` 与 `rfc8785>=0.1.4`；未修改 OpenAPI route、数据库、Redis、signer/key/JWKS/lease 或
Platform/IAM/Capability。后续必须串行推进：

2026-09-12 的 A2a runtime profile 与 signer 已通过 SPEC/QUALITY 与 Root 验证；直接声明
`PyJWT>=2.14.0`、`cryptography>=50.0.1`，lock 为 `2.14.0`/`50.0.1`；没有读取或装配 private key、发布 JWKS、
查询 lease、修改 wire 或调用 Platform。

1. Agent machine artifact/signer/JWKS/run-scoped supplier 独立验收；supplier 单元/fake-client 证据不称为真实 Platform call 接线；
2. IAM ADR/API/安全设计先对齐 exact profile、数字矩阵与六段依赖，再实现 verifier/OpenAPI/generated SDK；
3. Platform owner 发布最终 compact-proof wire/request-binding/generated helper，不发布临时 wire 或 fallback；
4. Agent 固定 Platform artifact，在真实 Skills/MCP client 每个 owner call 边界完成 supplier 接线与 transmitted proof 验证；
5. Platform consumer 切 IAM SDK、删除旧手写 wire 并完成 fresh/completed-replay receipt 闭环；
6. Root 真实 Agent -> IAM -> Platform -> PostgreSQL receipt sandbox。

实现验收必须覆盖 canonical header/payload/signing-input/vector、malformed key/file权限/symlink、worker/HTTP descriptor 与各自 key 不一致、
JSON safe integer矩阵（2^53-1、2^53、2^53+1、0、负数、bool、float、无coercion）、多副本descriptor/ring正常与紧急rotation、
stale/paused/terminal/owner或generation变化、数据库连接排队跨 expiry、签后 lease race、clock skew、nonce唯一、
unknown kid/禁止URL header、无敏感日志、JWKS GET/HEAD/400/404/405/503/no-store、OpenAPI/provenance drift，以及真实 IAM verifier消费。
此设计门通过不等于上述能力已经实现。

## Execution proof A2b committed implementation and A2c standalone component (2026-09-12)

The former label `A2b current candidate` is retained here only as a historical test anchor; it is not the current status.

The working implementation now contains the isolated private Ed25519 loader, anchored strict public-ring/JWKS snapshot, HTTP-only configuration root, anonymous exact JWKS GET/HEAD handling, OpenAPI `1.1.0`, and direct HTTP provenance pin. `kokoro-agent-http` points only to `interfaces.http.main:main`; the legacy worker HTTP entry is removed. Invalid or missing public material degrades JWKS and authenticated readiness before dependencies while health stays 200.

A2a remains the accepted pure signer, and A2c provides the standalone statement-time reader and run-scoped supplier. IAM verifier and the inactive Platform owner contract have since been released in their owner repositories; worker private-loader composition, Agent Platform call sites, and real Agent→Platform→IAM transport remain incomplete.

production signer call site 仅 standalone supplier 一个；runtime/client transport consumer/composition 为零。
