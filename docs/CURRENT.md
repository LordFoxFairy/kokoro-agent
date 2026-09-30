# kokoro-agent 当前实现

## W3 typed Skill source 设计门（2026-09-29；本仓文档目标，代码未动）

基线本仓 `main 7dfcfa936d0b51244683ffd66d16ea937fe510a6`。已落地的是 Run-scoped worker IAM token/fresh proof/六 RPC sender 和 Platform v3 `5b6eb2c` 的固定生成客户端；**未落地**的是 Agent/BFF launch 中的 typed Skill 选择、持久 Run fence、Source 业务 adapter、Storage signed GET 原字节/ZIP 校验与每次 `/.skills/` 访问的当前授权。因此 `EDGE-AGENT-CAPABILITY` 未激活，成功发送测试 RPC 不等于正式 Skill 在 Chat 可执行。现有 `agents/music.py` 仍声明 name `music`，`clients/skills.py` 与 `skills/backend.py` 仍保留旧 Capability name/scope/hash 与包缓存；本设计门不改它们。

目标已在 [TECHNICAL_DESIGN](TECHNICAL_DESIGN.md)、[API_CONTRACT](API_CONTRACT.md)、[DATA_MODEL](DATA_MODEL.md) 与 [ACCEPTANCE](ACCEPTANCE.md) 对齐：由 BFF 当前 IAM 用户选择 exact ref，Agent admission 冻结最多 16 个/4 KiB 到既有两张 Run 表 `request_json`，claim/resume 只读持久集合；已声明 ref 走 fresh Platform Resolve/Get、Storage signed GET 与 v4 ZIP/manifest identity，再由 DeepAgents 只读 backend 渐进读取。撤权、安装移除、感染、未知 scan、过期 URL/lease、owner 不可用均失败关闭；签名/包不落库，不建立跨 owner SQL。`music` name fallback 在后续代码片删除，不能在这份文档门把它标成已切换。

**独立 owner 前置与顺序：** Platform 当前 `main 6a09913a96c686b316bfe707b823d039e625607a` 的 [`platform_runtime.proto`](../../kokoro-capability/contract/proto/kokoro/platform/v1/platform_runtime.proto) 将旧裸 `read_reference` tag 4/name reserve，tag 5 返回完整 `transfer_reference`；v4 `manifest.json` 仍 `inactive/routable=false`。Agent 必须先精确 pin v4 原字节/生成再接 Source/GET，不能在 v3 sender 上臆造签名 URL。Platform `InstallSkill` owner RPC/当前 installed+enabled gate 与 BFF 用户公开安装/启用/选择不是同一个完成项：个人 Publish ACTIVE/本人列表不自动安装，当前 BFF public Chat→Agent launch 也没有 selected ref。先 Agent owner 发布 launch 契约，BFF 再接用户选择与安装产品 API/当前 IAM，Web 后续消费；Platform/Storage 生命周期与最终激活仍由各 owner 单独验收。当前不能凭个人 ACTIVE 列表声称 Resolve 可用。

本片只提交 Agent 四设计文档及验收矩阵；未运行代码构建、数据库、IAM/Platform/Storage 真组合或浏览器。后续代码片由 Root 放行后单一 Agent writer 执行 RED→GREEN，未决项见 [ACCEPTANCE](ACCEPTANCE.md)。下文旧 W1E/候选时间线为历史阶段记录，不覆盖此当前态。

## W3-AGENT-PLATFORM-V3-PIN（2026-09-29；本仓候选已通过直接门，待 Root 验收）

旧 Agent `cbb2719` 曾固定 Platform `ee25c1f` 的 Proto 与 execution-operations v1、
`binding_version=1.0.0`；当前本仓候选已固定 Platform owner `5b6eb2c` 唯一运行时 v3/3.0.0，
Proto SHA-256 为 `282bf886ea9648f7ce5208abd36ab47d879b2002a036d90aada2af59e74b4020`，
v3 aggregate 为 `324e749da1bc66c1ff03de74e7299716f798f5f5bb5fa19556033b79fa09ff8d`。
已用 owner 原字节替换 Agent 的 v1 vendor，固定完整 17 文件 v3 artifact/provenance，
重新生成唯一 Python Proto/Connect client 与 24-request projector，验证六个 Agent RPC 的正反向量。
本仓 `ruff format/check`、Pyright、contract check、生成 drift、聚焦 pytest 132 pass/1 deselected、
默认 pytest 1310 pass/6 skip/172 deselected、wheel/sdist build 均已在候选工作树通过；
Root 独立审查和提交尚未发生。六 RPC sender 尚未被 Skill/MCP 产品 adapter 调用。
manifest 仍为 inactive/routable=false；typed Skill/MCP 选择、Storage 包读取、MCP 凭据和真实 IAM→Platform
互操作仍为后续门，本片不提升 `EDGE-AGENT-CAPABILITY` 为 active。


## W2-REAL-MODEL-AGENT-WRITE 候选（2026-09-29，待 Root 提交/真组合）

`GENERAL_AGENT` 已显式启用隔离 state 工作区写入，全局 Permissions 和 Music 仍为只读，
`/.skills/` 仍由只读能力包 backend 拒写。只改静态声明，未增加请求放权参数或 host backend。
真实 AgentFactory/DeepAgents v3 组件测试先 RED：原生 write_file permission denied，随后
read_file/deliver 均 file_not_found、Storage facade 调用 0；修正声明后同一 write→read→deliver
链 GREEN，原字节/hash/Run identity/lease/tool_call_id 均一致。默认 Agent 写拒、Skill 写拒和
Run wire 额外权限字段拒绝亦通过。该测试的模型、System/Storage/Run/checkpoint 为明确测试
替身，真实 System+Ollama+IAM/浏览器/PG/Redis/Storage 组合仍待 Root 独立执行。

候选工作树已执行 `uv lock --check`、Ruff format/check（245 files）、Pyright（0 errors）、
contract checker、默认 pytest（1307 passed、6 skipped、172 deselected）及 wheel/sdist build；
默认测试保留上游 v3 beta/asyncio deprecation 警告。构建临时 `build/` 清理后重跑格式/检查通过，
不把生成临时副本格式差异混入源代码切片。真实依赖门未运行；本候选尚无交付 commit。

状态日期：2026-09-29。本文件只记录当前代码、canonical schema、contract 和已执行证据；目标值与未来
设计分别见 `SLO.md`、`TECHNICAL_DESIGN.md` 和 ADR。

## W2-F2-S4 Agent→Storage 作品种类：单仓代码门已验，增强真纵切待验

Storage `main` `d5cfc442c675e32363ae767f5ec662a9e0d9eaea` 已发布 v2 机器源与
final+CLEAN Artifact owner 代码。Agent S2 运行基线 `96dafec038ab6a0397ce58bc638bb576c59f1328`
已固定 Proto/生成 Python Connect client，正式 worker 装配独立 Storage secret、RPC URL 与受控对象源；
默认 Chat Agent 声明 `deliver`。工具按可信 Run/lease 与 conversation `session_id` 调用 Storage，
冻结 tool journal 意图和稳定命令。Storage FINAL 而 workspace 已消失时可按原 owner 命令/回执恢复；
`delivery.created` 使用稳定 ID 的 critical outbox、Chat 投影和终态屏障。取消路径在 Agent 同库事务内
原子写 control ledger、applied receipt、cancelled terminal、fence 与 cleanup intent；queued 帧按序补发。

Agent 单仓门：Root 独立执行 `uv lock --check`、Ruff format/check、Pyright（0 error）、contract
checker、默认 pytest（1267 passed、6 skipped）、自建临时 PostgreSQL/Redis integration（130 passed、
1 skipped）及 wheel/sdist build；测试资源已清理。Root W2-F2-S3 真纵切
`5d3b29c0f66aef366820f0eb` 使用生产 Storage client、已 claim Run、真实 PostgreSQL/Redis/MinIO/ClamAV，
验证 FINAL 作品、生产 `DeliverResult` journal、critical `delivery.created`/Chat/terminal 顺序、
重放不双发及本人签名 GET 原字节；另验证过期 lease 无出站、EICAR 不 Finalize、跨 conversation/tenant
不签发引用。该纵切**未**运行完整 worker/LLM，也未证明 BFF 当前用户私有授权、Product Library 或 Web 展示。

本仓 S4 代码把 `CreateArtifactResponse.kind` 的经校验值转换为严格必填
`artifact_kind`（document/code/image/audio/video/data/archive/other），传至 `DeliveryReceipt`、
`DeliverResult`/journal、critical `delivery.created` 与 Chat 投影；同命令恢复保留同值。
未知/0/缺失/不一致值失败关闭，`other` 只对应 Storage 显式 OTHER，不由 BFF 按 MIME 猜。
Agent event-protocol 源变更已同步本仓 `contract/provenance.json` 的组合摘要。
Root 独立 `uv lock --check`、Ruff format/check、Pyright 0 error、contract checker、
默认 pytest（1299 passed、6 skipped、172 deselected）、真实 PostgreSQL/Redis integration（130 passed、
1 skipped）与 wheel/sdist build 均通过；测试自有数据库和 Redis keys 已清理。
增强的真 S3 纵切仍须断言 kind 贯通；旧 S3 证据
尚未覆盖该字段，亦不证明 BFF/Web 已消费。

## W1E authenticated transport 候选（基线 `cf3d9ef`，待 Root 审查/提交）

- `worker/main.py` 在进程 context 内启动/关闭 `worker/platform.py` 资源，私钥只在 worker 加载。
  `WorkerPlatformRuntime.for_run(LeasedRun)` 提供固定 tenant/fence 的 sender；六个技术设计批准 RPC
  经 generated Connect client 真正发送，不手写 wire。无声明基础 Run 不取 IAM token、不签 proof、不发 Platform RPC。
- `clients/platform_credentials.py` 严格读取 owner-only regular JSON；`platform_tokens.py` 按 tenant/generation
  singleflight，轮换前后核对、无 stale-on-error、最后 waiter/关闭时收割任务；`platform_transport.py`
  每次 snapshot→token→唯一 projector→fresh lease/proof→tag100/Bearer，禁止重定向，限制响应 1 MiB。
- 已声明 Skill 无 client/reader 或 resolve 失败直接失败；MCP 不再 fallback 到部署 YAML；所有 Feature peers
  在任何 sandbox/model/provider 之前完成能力预检。`music`/`music_chat` 在缺 Skill client 时不再静默基础降级。
- owned loopback OAuth/Connect 网络门验证真实 binary send、签名/binding/fresh JTI、取消/deadline/302/1 MiB；
  另以自建临时 PostgreSQL 数据库完成真实 claim→worker signer/lease reader→两次 send→pause 拒绝，临时库已清理。
  Loopback 的 OAuth/Connect 服务是测试 fixture，**不是 IAM/Platform production authorization 的互操作证据**。
- 尚未闭环：真正 IAM→Platform 当前授权组合；typed Skill/MCP selection 的持久 Run fence 与业务 adapter；
  Storage v2 package bytes；MCP credential delivery；BFF/Web 选择传递。旧 name ports 未冒充 typed adapter，
  因此 worker 虽持有 transport factory，现有产品声明尚不调用它，Agent→Platform inventory 继续 broken。
  Platform downstream IAM 429 仍折叠为 Unavailable；caller-client-id rotation readback 仍是 owner gap。

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
- Skill/MCP 业务选择目前仍有窄 client port；Storage 真作品客户端已接线。Agent 不读取 Platform/Storage 私库。Platform owner 已在
  `apps/kokoro-capability` 物理仓 main `ee25c1f4d6df08be183ca10f7f5e852e0b21f641` 发布 inactive
  `kokoro.platform.v1`。本仓现已 pin 两份只读 Proto 输入并生成 Python Protobuf/Connect async client，
  并已固定 Platform 原始 execution-operation artifact、生成 24 tenant request 的 offline typed projector；
  owner manifest 仍是 inactive/routable=false，业务 adapter 尚未实现；worker Connect transport 与逐 call proof 已由上节候选接线，库存未激活。
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

2026-09-27 Platform binding projector 切片：首轮 TDD RED 为 7 failed/1 passed；审查加固的
exact message class、independent trust anchor 及 ECMAScript trim 用例 RED 为 71 failed/17 passed，
runtime artifact literal 架构门 RED 为 2 failed/24 deselected；GREEN 综合聚焦门为 119 passed。
`uv lock --check`、frozen sync、Ruff format/check、Pyright、完整 pytest（1197 passed、6 skipped、
165 deselected）、contract check、隔离再生成 drift、wheel/sdist 与 fresh Python 3.11 wheel projector
import 均通过。此证据仅覆盖 pinned inactive artifact 与离线 projector；未运行 PostgreSQL/Redis，
也未接 proof supplier、RPC transport、worker 或 current authorization。

## 仍需收敛的工程项

1. 历史包目录中的部分执行编排仍较大，需按 use case、repository adapter、outbox 和 supervisor 生命周期
   语义拆分，不能按行号机械切割。
2. `domain/`、`application/`、`infrastructure/`、`interfaces/` 是当前目标架构边界；叶子运行模块按真实职责保留，
   新代码不得恢复顶层 `repositories/`、`services/` 或 `http/` 重复入口。
3. Platform typed Skill/MCP 业务 adapter 与真实 owner 授权组合仍需落地；已声明 Platform 能力缺配置或 owner 不可用时
   必须 fail closed，不创建伪实现或以空列表/部署 YAML 作为授权。无外部声明的基础 Run 不要求
   Platform 调用。Storage 作品客户端已装配，但 `artifact_kind` 贯通和 BFF/Web 消费仍待后续切片。
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
