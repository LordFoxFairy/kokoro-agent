# kokoro-agent API 契约

## AGENT-PROFILE-P1：内部纯配方候选，HTTP3不变（2026-10-01）

在 D0 已提交基线75701413上，P1实现纯canonical profile/摘要、显式包来源descriptor与现build共用选择plan。
完整字段形状只属于内部配方，见TECHNICAL_DESIGN的P1段落；ValueError是内部失败，不新增wire错误码。
HTTP OpenAPI/provenance/generated、RunRequest/Redis/required字段、202/409、failure tuple和当前preflight顺序均未改。
没有digest网络传输/持久freeze/授权gate；完整生产manifest与worker装配仍待后继。不把P1交付称4.0 artifact发布或激活。
Root尚须独立复验；完整scope/native/retention与协调consumer发布门维持D0裁决。

## AGENT4-D0：当前 contract 与 P1 实施子门（2026-10-01）

当前 Agent main `224d0f19ff2199c38b95f621015ea7856f589454` 的唯一机器源仍为 HTTP 3.0.0。
现 `finalize_terminal` 已原子保存 usage/outbox/Chat/cleanup，durable RunRequest consume 已统一；这些已提交事实
不是 scope/retry/profile/native 4.0 实现。BFF `e7a325ce` 已验内部 Chat terminal FIFO 与非法 post-terminal source 阻断，
不代表 Agent scope、Scheduled 同 session 或组合激活。

P1 仅按 TECHNICAL_DESIGN 同名放置表实现纯 profile 编码与真实 build 共用选择计划；不编辑机器源、provenance、
generated、RunRequest、HTTP/Redis envelope、错误码或 202/409 语义。不会新增 3.0 `run_scope_busy`，不将纯 digest
变成持久或网络授权 gate。独立 P1 文档子门由 Root 放行；Conversation 删除/expiry 决定只阻最终引用释放及完整发布，
不阻 P1。当前完整 4.0 门仍未通过，required parent/runtime/schema/真实验收必须协调完成。

正式 4.0 的字段、错误、normal/retry 身份与发布顺序仍见下方目标；每片实现授权与 artifact 发布/服务激活分开。
当前 canonical fence、首次 Redis publish 和 recovery republish 的 `exclude_none=True` 必须在正式 wire 切片共同修正，
保留显式 `retry_of_run_id=null`；P1 不先更改这三处，不向现 3.0 sender 偷加 required 字段。

## AGENT-DURABLE-INGRESS-P0：3.0 wire 不变的 worker 内部门（2026-10-01）

本片不编辑 OpenAPI、generated、provenance 或错误码。`POST /v1/runs` 仍先持久
canonical dispatch intent，Redis `RunRequest` 只是通知。worker 的所有 `RunRequest` 入口都必须
回读 `get_pending_dispatch(run_id)` 并仅用返回的 canonical request 调用 `claim_dispatch`；
缺失/already-claimed intent 的通知为内部 no-op，不产生新 HTTP 响应或公开失败事件。
resume/steer/cancel 契约不变。本片不是 session FIFO、queued/steer 新语义或 Agent 4.0。

## AGENT-TERMINAL-ATOMIC/P0：HTTP3.0 wire不变的内部一致性门（2026-10-01）

基线dd5afc3528fe3a835756bc3ff55dfacaa8ca76d3。本片不改OpenAPI/provenance/generated/公开字段、
失败tuple、LaunchReceipt、Run cursor或Chat envelope，不发布4.0。typed outcome/result只是内部port。
原有completed/failed/cancelled用唯一finalize事务产生terminal outbox与Chat terminal session seq，HTTP在commit后
已可读；Redis随后发布，丢失ACK按固定事实重放，不二次分配seq或重写胜者payload。正常callback已观测最终usage
同事务入账；paused段与resume段沿现generation身份累加。取消无可靠新段时不伪造usage、既有wire形状不变。
Run outbox index/durable_seq只per-run，Chat event/message seq不同kind；本片没有新增scope锁，不声称同会话FIFO。
BFF仍只读Agent HTTP Chat replay；live保留reserve→fenced Chat→无锁publish，durable post-terminal live拒绝，
Redis迟到字节不是新的HTTP事实。terminal generic emit路径删除，nonterminal critical保留原恢复语义。
NACK由同一finalize的quarantined disposition终止：同txn核持久rejected receipt/fence，terminal/cleanup/superseded
私有audit原子写，不新增公开Chat、不恢复Redis、不推进毒化consumer；delivery不阻断隔离终止，重放核receipt+Run+audit。
三文档局部门已由Root批准进入源码；原retry4 required字段/全owner发布/生命周期整体门仍未通过。源码候选已实施，本片 Root 验证门已通过，最终实测证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。

NACK仅信任同txn与offending outbox匹配的receipt(run_id,seq,event_id)，保留fence并原子supersede毒化open帧；不返回可发布frame。terminal后usage不接受首次新段，既存精确重放只读，不改变wire或赢家。

normal/cancel 不越过同锁已确认的合法 rejected receipt；终态先赢则后到 NACK 不覆写。新增内部只读 verify_terminal_frame 仅验证已提交事实，缺失/漂移不发布、不修补 terminal Chat。retained started 的 Chat 缺口在同终态事务按原 identity/时间/index 补齐后再分配 terminal seq；不是新增 wire、不是另一终态路径。

职责收敛：postgres_run_leases 保持唯一 finalize 同连接事务编排；postgres_run_events 承接 delivery_ready_on_cursor、terminal_chat_on_cursor 与只读 verify_terminal_frame，façade 的只读验证指向 events。events 不 import leases，不复制 SQL、不新增模块，也不放宽现 800 行门。

本片 delivery GC 收敛：reconcile 先取得同一 Run 锁；active Run 的已 ACK delivery.created 保留原 event_id/index/time/payload，consumed watermark 仍推进，非 delivery 正常 GC。terminal 后按最终 consumed 水位重扫而不依赖本轮推进，避免 ensure 重建第二 delivery/Chat。没有新表/API/ledger、没有永久跳过 Run purge、没有退回本地时钟或弱化 barrier。真实测试同时覆盖 natural/cancel、持真实 Run 行锁的 GC↔ensure 与 GC↔finalize、最终 ACK GC 后 replay 不重建；本片上述矩阵已由 Root 执行，最终实测证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。

最后两处边界：add_usage（含 pause 段）先锁 Run，再通过 context.database_now 读取数据库时钟校验 active expiry；terminal 只允许已存精确 segment 重放，不接受新段。quarantined replay 同事务严格核 private audit kind/payload、NULL index、timestamp=terminal_at、durable_seq=Run counter 且越过 rejected fence；身份/内容漂移返回 lost，不补写、不公开、不换 winner。Root 已跑 true PG RED（usage 一例/private audit 六例），本片上述矩阵已由 Root 执行，最终实测证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。

## AGENT-RETRY-DESIGN：目标 launch 4.0（2026-09-30，文档候选）

当前基线 `f3be3b97dd67df69ed3c6cb88c59f3bc2db97703` 的机器 artifact 仍为 **3.0.0**，
本门未修改机器源或 generated。目标是同一 internal-owner `POST /v1/runs` 的 coordinated clean-slate
**4.0.0**，不新建 retry URL/service；本文并非已发布字段。唯一机器事实仍是
`contract/openapi/v1/openapi.json`，实现放行前须与运行时、DATA_MODEL 一起验证。

- `LaunchRequest`、`LaunchBody`、`RunRequest` 增 **required** `retry_of_run_id: null | non-empty string`。
  normal 明确 null；retry 指向最近失败 attempt；缺失、空串、错类型、自引用为
  400 `invalid_launch_request`。不提供省略 default、trace lineage 或旧 envelope fallback。
- canonical dispatch fence 包含该字段和当前 ExecutionIdentity；null 的 canonical 表达固定由唯一实现产生，
  HTTP→dispatch→Redis→Run 使用同对象，不靠 `exclude_none` 丢掉其语义；首次publish与supervisor_recovery
  republish均保留显式null，不能只修HTTP解析。`run_id` 是新 attempt/幂等身份，
  `request_id` 保持该 admission envelope 稳定；同 run_id 改 parent 或任何原 canonical 字段为
  409 `run_identity_conflict`。重复请求先完成当前 caller 授权，再返回原 receipt，不因原 parent 已非 latest 而制造新 attempt。
- retry 的原 message_id/content、feature_key、requested_model_label（含 null）、有序 selected_skill_source_refs、
  tenant/resource subject/session/派生 namespace 全部与 origin 冻结事实一致；actor/assertion 是当前新授权身份，
  不复制旧 credential。MCP 当前不在 launch schema，按 Agent 原执行 profile 固定并在 worker 重验；不得捏造已发布选择字段。
- 父必须是 scope 最新 attempt 且其 logical origin 仍最新、持久 4.0 延续的 strict Failure 合法 tuple 且 retryable=true、terminal outbox
  与Chat terminal事实已同事务持久，无 active dispatch/Run/HITL。非 latest/非可重试/存在活动 attempt 为 409 `run_retry_conflict`；
  同租户可见父的冻结业务输入漂移为 409 `run_identity_conflict`。不可见或不存在父统一
  404 `run_not_found`；错误不回显其他 scope、原 payload 或 provider 内容。
- normal 与 retry 共用 scope admission；有活动 dispatch/Run/HITL 时 normal 为 409 `run_scope_busy`。
  native locator/profile 损坏、缺失或未来版本不兼容失败关闭：接纳前为 409 `run_retry_conflict`，
  已接纳后走现 `contract_incompatible/false` 终态，不复用 latest checkpoint 或把 retryable 人为改 false 规避重试实现。
- 202 的既有 LaunchReceipt/查询/replay envelope 不变；新 attempt 与旧 attempt 的 Run evidence 独立，
  failure code/retryable tuple、Run cursor=-1、Chat seq 从 1 均保持。`ChatMessage(role=user).run_id`
  明确定义为 **origin Run**，其内容/seq/time 恒定；assistant/system/tool 仍属于其 immutable attempt。
  BFF 通过原消息关系投影，不要求 user.run_id 等于新 assistant.run_id。

权限语义不变：受信服务身份与当前 IAM session 先验证；业务 body 不自报 tenant/actor。重试不是旧 Run resume，
不接收旧 approval decisions；平台能力、System route/current health、Storage 当前授权在新 Run 重验。
执行profile按TECHNICAL_DESIGN版本1白名单canonical JSON/SHA256在任何可重试外部preflight前冻结；
原子继承的digest不是新wire字段，不包含secret/动态route/当前授权。typed terminal outcome在同scope事务
持久最终usage段、delivery barrier（delivery Chat事实已持久）、terminal/outbox/Chat terminal身份与session seq、
cleanup、成功head晋升与active释放；同连接原子提交后仅Redis发布，不允许release后补分terminal Chat seq。
当前 finalize_terminal 已消除先terminal CAS后emit窗口；4.0追加scope/head/active原子性。其他nonterminal critical按原outbox/Chat恢复；
固定terminal重放返回原Chat identity/seq，live可丢并依现Chat/native恢复，
Agent保留reserve index→独立事务fenced Chat append→无锁live publish；terminal先赢拒durable append，
live先提交则HTTP Chat seq先live后terminal。BFF正式AG-UI只消费HTTP Chat replay，不读取Agent Redis。
BFF e7a325ce 已验非法 post-terminal source 显式 block，不静默丢、不新增 schema；其 Chat FIFO 不等于 Agent scope/native fence。
terminal重放核原generation/固定event identity，不重分Chat seq；不保证终态后绝无迟到Redis字节，不新增全帧ledger。
scope/head/retry/profile/native 仍为目标实现；现 terminal 原子性已提交。lifecycle/retention 决定仍阻最终释放/完整4.0发布，独立P1实施子门不受其阻断。

retryable 只是用户可尝试的失败属性，不保证成功、无副作用或免费；既有 tool journal/artifact 不被撤销或隐藏。

**发布顺序：** Agent D0 四文档审查 → 独立 P1 纯代码及正式执行各片 RED/GREEN（不激活） → native atomicity/lifecycle 门 → owner machine/runtime/schema/fresh-install
同片验收，固定 4.0.0 commit/direct/aggregate digest 并 **只发布 artifact、不激活服务**（failure model 随 source digest
再生） → BFF 同一个消费切片原字节 repin/生成，全部 normal Chat、Scheduler producer/outbox/parser 显式 null，
retry 显式 parent 非空，并接原 user/新 assistant/outbox → 核查并更新所有其他 sender（含直接 worker caller）
→ Web 固定 BFF → Root 自有 fresh/组合验收后一次协调切换。required 字段不允许先激活 Agent4 再补普通发送方，
也不让 BFF4 向旧 Agent3 热发。旧 3.0 JSON、旧 Run baseline 缺失或旧 trace 不补默认；现受管组不半热混用。
Proof schema/vectors/独立 digest 不变；owner inventory 因实现文件变动应按实际再生，不以手改摘要遮 drift。
所有消费者明确固定新 artifact 后再切，不将机器/SQL尚未编辑的本次文档候选称为已通过三文档门。

## AGENT-FAILURE3-GRANULARITY API 不变门（2026-09-30）

当前 owner HTTP artifact 3.0 已提交于 `da056b0103cced10188cdc1f5baef841d8333889`，Root
真实 HTTP acceptance 22/22 通过；BFF/Web 尚未固定 3.0，当前受管 3310 仍是旧 2.0。该发布状态
与本次内部 Python 职责拆分必须分开记录。

目标拆分不编辑 OpenAPI、provenance、生成 failure model、proof schema 或 vectors。Run failure 仍是
严格 `{code,retryable}`，Chat failure 仍是严格 `{status:"failed",code,retryable}`；闭集、合法
retryable tuple、未知 owner code、初次/恢复一致性及 Run-only evidence cursor 全部不变。不新增 URL、
字段、alias、默认值、兼容读取或第二枚举。

execution-proof 的 31 个具名 negative 名称、顺序、stage、component、error_kind、optional metadata、
difference、payload bytes 和期望错误均不变。`validate_named_negative_metadata` 是本仓 checker 的内部
Python 协作者，不是 wire API；唯一公开给 checker 的 `json_exact` 同移至 existing negative-spec
policy，checker 无别名直接导入调用而不复制实现。两者继续类型精确比较 JSON，尤其拒绝以整数 `1`
替代布尔 `true`。机器 artifact 任何字节变化均超出本片。

## Run evidence 初始 cursor 实现候选（2026-09-30）

原 `/v1/runs/{run_id}/events` 错误复用默认/下限 0 的 AfterSeq，exclusive index 过滤
漏掉合法首帧 index=0。当前只该 Run operation 引用具名 EvidenceAfterSeq：query after_seq
为 integer/int64，minimum=-1、maximum=9223372036854775807、default=-1，省略等同 -1；
非整数、低于 -1 或高于 int64 上界返回 400。
它表示最后已见 Run index，事件始终满足 index > after_seq；after_seq=0 仍排除 index=0
而读取 index=1。EvidencePage.next_seq 范围为 -1..9223372036854775807，有事件返回本页最后 index，
无事件回显输入 cursor（包括 -1），terminal 仍按本页真实终态事件计算。
因此只有 index=0 的 run.failed 初始页必须返回该帧且 terminal=True，不伪造 START。

Session/Chat operation 继续引用原 AfterSeq（integer/int64、minimum/default=0），
Run event index 起点、Chat seq、水位和 wire safe failure profile 不变。本修复纳入尚未发布
HTTP artifact 3.0.0，不增加 URL 版本或兼容分支；OpenAPI 为唯一机器源，failure header 的
全文 source digest 和 provenance 已由唯一 generator 再生。纯测试已由既有 RED 转 GREEN；
真实 owner fixture 仍待 Root 独立复验，BFF/Web 不消费在途。


## AGENT-FAILURE-CONTRACT 候选契约（2026-09-30，尚未协调发布）

基线 `58b59cf7` 原机器为 `2.0.0`；当前工作树已实现本节及生成/strict runtime 校验，
HTTP artifact `3.0.0`（URL 保持 `/v1`）是 pre-launch coordinated breaking：新增 required retryable、
闭集扩展、删除 RunFailure 的 raw error_kind/message；不接受旧形状或以缺字段默认值兼容。

唯一可编辑事实源为现 `contract/openapi/v1/openapi.json` 的基础 Failure schema：code 闭集和合法
code/retryable tuple 只定义一次；RunFailure 引用基础，ChatFailure 组合引用基础并加 status const failed。
两 profile 严格拒绝额外字段；JSON bool 不接受字符串、数字或 null。组合 schema 须正确封闭最终对象，
不能用错误的 additionalProperties/allOf 组合拒绝合法 status。只允许 model_unavailable、dependency_unavailable
配 true；其余 code 必须 false。生成模型必须执行同一 tuple 约束，不以静态类型提示代替 runtime 校验。

`RunFailure={code,retryable}` 是 Redis run.failed 的 payload；`ChatFailure={status:"failed",code,retryable}`
是 HTTP replay 中 event_type=run.failed 的 decoded payload_json。外层 payload_json 仍为 string，
OpenAPI 通过具名 x-kokoro 映射明确该 discriminator 对应 ChatFailure；不把其他文本/工具事件套上 failure contentSchema。
脚本单向生成 protocol/run_failure_generated.py；不得在 protocol/events 或消费者另写可编辑枚举。

### System client 与归码矩阵（候选已实现）

先验证 strict error envelope 和正式 HTTP/code/retryable tuple，再分类；删除现
`status >= 500 and error.retryable` 掩码，不把矛盾响应修成合法 false。下表 HTTP 约束仅适用于 owner 响应，
本地 typed 错误没有 HTTP status；System 正常 200 route 验证及大小/deadline/取消/拒重定向边界不变。

| 来源/code | 合法 HTTP / retryable | Agent code / retryable |
| --- | --- | --- |
| owner MODEL_UNAVAILABLE | 503 / strict true 或 false（现 owner 正常事实为 true） | model_unavailable / 原 bool |
| owner SYSTEM_UNAVAILABLE | 503 / strict true 或 false | dependency_unavailable / 原 bool |
| 本地 MODEL_RESOLUTION_UNAVAILABLE | 无 HTTP / client 明确产生 true | dependency_unavailable / true |
| owner POLICY_DENIED、FORBIDDEN | 403 / false | model_access_denied / false |
| owner ROUTE_NOT_FOUND | 404 / false | assembly_failed / false |
| owner INVALID_ARGUMENT | 400 / false | assembly_failed / false |
| owner service_auth_failed | 403 / false | assembly_failed / false |
| 本地 MODEL_RESOLVER_NOT_CONFIGURED、MODEL_REQUEST_INVALID | 无 HTTP / false | assembly_failed / false |
| 本地 MODEL_RESPONSE_INVALID、MODEL_RESPONSE_TOO_LARGE | 无 HTTP / false | contract_incompatible / false |
| 未知 owner code → MODEL_RESOLUTION_FAILED | 严格 envelope，HTTP 为操作已声明的 400/403/404/503；不信任未知 code 的 retryable 语义 | internal_error / false |
| 其他装配异常 | 无 owner tuple | assembly_failed / false |
| 其他执行异常 | 无 owner tuple | internal_error / false |

已知 owner code 的错 HTTP、不可重试 code 携 true、非 bool 或坏 envelope 均产生 MODEL_RESPONSE_INVALID，
对外 contract_incompatible/false；3xx 不跟随，其他未声明 HTTP status 也归响应合同错误。未知 code 仍须先通过 envelope 严格类型校验。
本次按 Agent provenance 核固定 System `f5702068d4416ad90b1bd02af57d2825c32be916` / `2.0.0`：
当前 owner OpenAPI 原 bytes SHA-256 与 pin `f9ea76f107e1ea0fc19df20ee7c59032c0fbac66e640e9a16a1b770ab27c1f37`
完全相同。该 resolve operation 声明 200/400/403/404/503，OwnerError 是 strict envelope，但 code 为非空 string，
机器未枚举具体 code；上表是结合本仓既有 System 消费契约与 Root 获批分类的窄 adapter 规则，
不冒称固定 OpenAPI 已编码全部 code/HTTP 对照。未修改 System pin 或复制 owner schema。
旧 client allowlist 中的 service_auth_not_configured 在该固定机器 artifact 中没有发布依据，
不猜其 status；未发布 code 按未知 owner 路径处理，本地 resolver 未配置使用既有本地 typed code。

既有 token_budget_exceeded、recursion_limit_exceeded、enqueue_failed、dispatch_exhausted、
contract_incompatible、internal_error、assembly_failed 保持 code，retryable 均为 false。
assembly_failed 表示本轮装配失败，不断言一定是用户空间配置错误。Cancel 继续传播而非生成可重试失败。
不按 exception message/类名归码，不将 owner message、secret、URL、stack 写入安全 failure wire。
retryable 不触发自动重试、不恢复旧 Run、不变更计费或幂等，不承诺再次请求免费。

### 生成、版本与消费者门

`uv run python scripts/generate_failure_models.py --check` 核完整再生 bytes/header/source hash；
`uv run kokoro-agent-contract-check` 核 schema/profile/tuple/decoded 映射、generated direct digest、HTTP direct digest
与完整 owner inventory/aggregate。当前候选 checker 已通过 3.0.0，本仓纯门记录见 CURRENT；
这不证明 BFF/Web 已 repin 或当前服务已切换。
Proof schema/vector/独立 digest 不动。Agent 3.0.0 固定发布后 BFF 才严格 repin，保留 code/retryable 并按
safe code 生成标准 AG-UI RUN_ERROR.message；Web 固定消费 BFF，不直接消费 Agent 私有 wire。
旧 retained JSON 的一次性自有 fixture 切换与资源授权见 [DATA_MODEL](DATA_MODEL.md)，无 old fallback。

## W3 OAuth consumer 扩展规则（2026-09-30）

IAM `POST /iam/oauth2/token` 成功响应遵循
[RFC 6749 §5.1](https://www.rfc-editor.org/rfc/rfc6749#section-5.1)：未知成员忽略且丢弃（包含 `expires_at`）。
已知字段类型、Bearer/token 语法、`expires_in` 整数且 >5、返回 scope 的精确匹配保持严格；
省略 scope 仍沿既有请求 scope 语义。缓存仅采用请求开始时刻 + expires_in，不读取扩展有效期。
错误脱敏、1 MiB、timeout、拒重定向、取消与凭据轮换契约不变；本片不修改机器契约或 SQL。

## W3 typed Skill reader 消费当前态（2026-09-29）

Agent launch仍为既有OpenAPI2.0.0，不改HTTP/Redis字段。BFF571b51de/Web1dc211bb已消费exact refs/[]，旧“未同步”属于历史。
当前原字节pin是Platform6a09913 v4（21JSON/34catalog/24binding，bindingVersion3.0.0）与既有Storage16a6c1c。
`ResolveVisibleSkill`/`GetApprovedSkillPackageReference`已通过Run-bound sender接线；保留全部六允许RPC，不扩充可调用操作。
原read_reference tag4/name reserved，transfer_reference tag5；每次读取验证当前响应身份与Storage签名GET/ZIP，不接受裸URL fallback。
Storage固定v2明确GET required_headers为空；不自创非空header支持。worker ObjectStore origin与写secret解耦，但allowlist不放宽。
读取错误/取消向既有Run失败边界传播，无隐式重试/回空；无新增外部API。只读路径/资源限制见[技术设计](TECHNICAL_DESIGN.md)，
实际证据见[验收](ACCEPTANCE.md)。v4 inactive与安装/启用产品面尚未闭环，不将ACTIVE列表当execution grant。

## 历史：W3 launch 机器契约代码片（2026-09-29）

Agent-owned OpenAPI `2.0.0`（URL 仍 `/v1`）与 HTTP/Redis `RunRequest` 已要求显式 `selected_skill_source_refs`：exact `skill:<SkillId>`、最多 16 项/4 KiB、顺序与去重受检，`[]` 是无外部 Skill，缺失/错形状 400，同 `run_id` 选择漂移 409。字段由现有 canonical RunRequest/dispatch fence 承接；真实 PostgreSQL/Redis roundtrip 尚待隔离验证。下节 launch 两行是设计来源且已经代码化，Platform v4、Storage GET、ZIP、backend 其余各行仍是目标。BFF 当前 Chat durable outbox 与 Scheduler 发送方未同步，旧 payload 将在 Agent 入口 400；需 BFF 精确 repin 后显式发送 `[]` 或选择，不从 `trace.pinned_skills` fallback。

## W3 typed Skill source 设计起点与剩余目标（基线 `7dfcfa9`）

当前 Agent `main 7dfcfa936d0b51244683ffd66d16ea937fe510a6` 的 [`contract/openapi/v1/openapi.json`](../contract/openapi/v1/openapi.json)、`protocol/control.py` 与 `interfaces/http/ingress.py` **没有** Skill 选择；`Agent.skills`/`SkillClient.resolve` 仍为名称字符串。下面是待 RED→GREEN 发布的 Agent-owned **目标**，不是现有 HTTP/Redis 可用字段。BFF public Chat 选择字段也尚未发布，不得让 Web 直接改 Agent wire。

| 边界 | 目标形状、身份与错误 |
| --- | --- |
| BFF→Agent `POST /v1/runs` | 增加 `selected_skill_source_refs`：有序、去重、严格 `SkillSourceRef.value` 字符串数组，空数组表示无外部 Skill；最多 16 个、选择 JSON 编码最多 4 KiB，精确上限写入本仓 OpenAPI/Pydantic，`extra=forbid`。BFF 的用户身份仍由受信 `X-Kokoro-*` headers/assertion 给出，不由 body 自报。语法错误/重复/超限为 400 `invalid_launch_request`；同 `run_id` 但选择变化为既有 409 `run_identity_conflict`。 |
| Agent HTTP→Redis→worker | 同一字段进入本仓 `RunRequest` 唯一 canonical JSON、dispatch fence 和 Run claim；不要在 HTTP body/Redis/Run JSON 之间设不同默认或 alias。已持久化选项为重放/lease takeover 的唯一资源选择；不能以 query/display name 或 Feature 当前字符串重新解释。 |
| Agent→Platform Resolve | `kokoro.platform.v1.SkillSourceService/ResolveVisibleSkill`，`ResolveVisibleSkillRequest.source_ref` 为 **message** `SkillSourceRef{value}`，带 owner 固定 `request_id` 和 tag100 `execution_proof`；tenant/subject/actor 来自当前 Run + IAM token/proof，绝不从 ref 自身取得。核响应 `SkillSource.source_ref`、exact `skill_id/revision`、`package_asset_ref`、`content_digest`、`manifest_identity`。 |
| Agent→Platform 包引用 | `GetApprovedSkillPackageReference` 输入同一个 typed ref 与 fresh proof；响应核 `asset_ref/content_digest/manifest_identity` 与 Resolve 完全一致，`transfer_reference` 必须包含 `url/method/required_headers/expires_at`。该读取重新做 Platform 当前授权/安装和 Storage CLEAN；旧 Proto 裸 `read_reference` 已 reserved，不接受。 |
| Agent→ObjectStore | 仅按上述批准的短期 `GET` 与安全 headers 获取原始 ZIP bytes；URL 不等于 authorization，禁 redirect、凭据、签名持久化、任意 host。响应须 200、无压缩编码变化、长度有界、SHA-256 等于 `content_digest`；ZIP/manifest 身份再按 Platform v4 profile 核验。 |

`SkillSourceRef` 的 owner 格式为 exact `skill:` 加合法 `SkillId`，总长 7..197 ASCII bytes、区分大小写，不 trim、不允许重复 prefix、不以 `series_id`、`installation_id`、name 或 BFF 列表 item 代替。唯一权威是 Platform `6a09913a96c686b316bfe707b823d039e625607a` 的 [`contract/proto/kokoro/platform/v1/platform_runtime.proto`](../../kokoro-capability/contract/proto/kokoro/platform/v1/platform_runtime.proto)（SHA-256 `8ccab4aee4efdfd8210f2e5f02ae8ec85c2c470e90451915209406e16621289a`）、[`contract/execution-operations/v4/manifest.json`](../../kokoro-capability/contract/execution-operations/v4/manifest.json)（`7058e2d2e11c885765ceb4e813e1f4f208e170c97a9b27e8d5f0178fcffc8d5d`）、[`zip-profile-v1.json`](../../kokoro-capability/contract/execution-operations/v4/zip-profile-v1.json)（`18ef03a3d7ea84f0e2a38c6e625146f607b96689957210052116b557008024fc`）和 Storage `16a6c1ce95832df6dc839e0d50e957405c5c7005` 的 [`contract/proto/kokoro/storage/v2/storage.proto`](../../kokoro-storage/contract/proto/kokoro/storage/v2/storage.proto)（`02266d53b8bfb7fdd6991b17d59ebb17399d4f189b6a3135fedcb69b6b364276`）；这些链接为只读来源，Agent 不编辑 owner Proto。Agent 当前 vendored/generated 仍来自 Platform v3 `5b6eb2c`，v4 `inactive/routable=false`；v3 的六个证明绑定 operation 仍为 `3.0.0`，但 v4 的 GET transfer 与 ZIP profile 不可由 v3 旧字段猜出。下一机器片必须固定 exact owner commit/direct digests、生成物与 drift gate 后才使用。

Platform Resolve/包引用的 `NOT_FOUND`/`PERMISSION_DENIED`/`UNAUTHENTICATED`、过期/撤权/未安装/感染/未知扫描、5xx/限流/timeout 都不得映射成空 Skill 或旧缓存命中；在已声明选择的 Run 中 fail closed。相同逻辑读取重试保留 ref/request_id，但重新获取当次 bearer/DB-clock lease/proof/JTI/reference；服务器完成与网络 ACK 未知只重试无 mutation 的读取，不发第二种选择或绕过当前授权。对外错误用本仓稳定 Run failure/evidence code，不能回显签名 URL、headers、token、包内容、owner 内部 message。每次 `/.skills/` 再读都须新获当前授权；缓存仅在当次授权通过且资产/hash/manifest 未漂移时复用同 Run 已校验字节。

**先行契约依赖：** Agent 发布本仓 launch OpenAPI/Redis 字段并约束数量；BFF 再发布用户显式选择的 Product Chat 字段、IAM session 准入和传递，Web 最后固定消费。Platform/BFF 还须提供个人已发布 Skill 的 install/enable/选择链；现有 personal ACTIVE 列表只是浏览结果，`ResolveVisibleSkill` 当前要求 installed+enabled。没有这些 owner 契约时，Agent 不接受 name fallback 或将未安装 Skill 执行成功。实现/验收范围见 [TECHNICAL_DESIGN](TECHNICAL_DESIGN.md) 与 [ACCEPTANCE](ACCEPTANCE.md)。

## W3-AGENT-PLATFORM-V3-PIN：owner 机器来源（2026-09-29；本仓候选已验）

旧 Agent pin 的 Platform Proto 来自 owner `ee25c1f`，SHA-256 `7c55fcadf5ba0753ca5d1bb304ccb96bf0318a4fb5c37aae4f96781c4ea53466`，
execution artifact 为 v1/1.0.0。当前候选只读来源是 owner `5b6eb2c1532b23b9747bc4bf6ac99f69ad453de0` 的
Proto SHA-256 `282bf886ea9648f7ce5208abd36ab47d879b2002a036d90aada2af59e74b4020` 与完整
v3/3.0.0 aggregate `324e749da1bc66c1ff03de74e7299716f798f5f5bb5fa19556033b79fa09ff8d`。
本切片已按 owner provenance 固定每个原始文件及直接 SHA，不在 Agent 创建可编辑 RPC 契约；
generated client/projector 只由固定输入再生。24 个 tenant-execution binding 中，Agent 仍只发送
`ResolveVisibleSkill`、`GetApprovedSkillPackageReference`、`GetMcpConnector`、`GetMcpConnection`、
`ListMcpConnectorCapabilities`、`AuthorizeMcpTool` 六项，每次 fresh proof 的 digest 必须按 v3 计算。
Proto package 仍为 `kokoro.platform.v1`，无 Agent public HTTP/Redis wire 变化。v3 manifest 的
inactive/routable=false 是发布标记而非运行开关；本仓直接门已通过，Root 独立验收待执行；
本片只证明离线契约消费，不称产品激活。
下文 W1E 的 v1 来源和“worker transport 尚未接线”陈述为历史阶段，不作为当前验收事实。


## W2-F2-S4 Storage consumer 与 `artifact_kind` 事件契约（2026-09-28）

Storage `d5cfc442c675e32363ae767f5ec662a9e0d9eaea` 的
[`storage.proto`](../../kokoro-storage/contract/proto/kokoro/storage/v2/storage.proto) 和
[`common.proto`](../../kokoro-storage/contract/proto/kokoro/common/v1/common.proto) 是唯一 owner
机器源；Agent 已将其只读 pin 于 `contract/storage/v2/` 并生成 Python Connect client。
两份 owner Proto 原字节 SHA-256 分别为
`5a5dcaec2e1fd0d5eed369b8f79477fd0f8f653b32f9ebe14a8c339f4eb713ac`、
`4604725ec7d5896c9d74b53c6f06d19b20ee758d5ab9e1cb90177ede95bba9fd`；owner provenance
combined SHA-256 为 `8317e644d45c8db310b44f114afa22892a6a40d6ee7d0c1c4a37a8203e79f427`。
Agent 只消费 `CreateUpload`、`CompleteUpload`、`AbortUpload`、`GetUploadStatus`、`GetScanStatus`、
`CreateArtifact`、`FinalizeArtifact`；不因作品种类字段增加 Asset/List/下载资格。受信
`tenant/subject/conversation scope` 来自已认领 Run/lease，而非工具参数；Storage 继续验证
caller×operation×scope/purpose。`CreateArtifactResponse.kind` 是种类的权威返回值，
`FinalizeArtifactResponse` 不包含种类字段。

**当前 Agent 事件（`96dafec`）。** 生产 client 已验证 CreateArtifact 返回的 kind 与出站请求相同，
但 `DeliveryReceipt`、`DeliverResult`、`DeliveryCreatedPayload` 和 Chat `event_type=delivery`
均不含 kind。`delivery.created` 已是稳定 ID 的 critical outbox 事件，按序投影并在终态前落账；
不是旧设计中的 best-effort 普通帧。Root S3 已用真 Storage/PG/Redis/MinIO/ClamAV 验证此链的
资源 ID、原字节、顺序及重放，但**未**验证 kind 贯通，也未运行完整 worker/LLM 或 BFF/Web。

**下一代码门的 Agent-owned event-protocol。** 在 `DeliveryReceipt`、`DeliverResult`、
`DeliveryCreatedPayload` 与 Chat `delivery` payload 增加同名必填字段 `artifact_kind`，精确字符串集合：
`document | code | image | audio | video | data | archive | other`。映射固定为 Storage owner
`DOCUMENT=1`、`CODE=2`、`IMAGE=3`、`AUDIO=4`、`VIDEO=5`、`DATA=6`、`ARCHIVE=7`、
`OTHER=8`；`UNSPECIFIED=0`、未知数值、缺失/非集合字符串、与冻结请求不一致均失败关闭。
`OTHER` 不是未知值 fallback。MIME 只决定 CreateArtifact 出站请求，不是下游推断来源。
成功回执必须把**经校验的 owner response kind** 传到工具结果、journal 成功 JSON、critical
`delivery.created`、Agent Chat；同 tool-call 恢复仍使用原 Storage command/回执和同一字段值。
已存的缺种类/损坏成功 journal 不得静默补猜后发布成功事件；无兼容双字段或旧事件 fallback。

新增必填事件字段是 Agent→BFF consumer 的有意契约变化；BFF 后续以此构建自己的 Product/AG-UI
投影，不能在 Agent 文档门宣称已消费或已完成用户私有授权。Agent 的
`src/kokoro_agent/protocol/events.py` 变更时，现有 `contract/provenance.json` 的
`source_files` 必须继续包含该文件，`combined_sha256` 必须重算，并通过
`kokoro-agent-contract-check`；文档门不改机器源。
代码门以严格解析、八枚举映射、未知失败、恢复同值、stable critical event/Chat 顺序及 Root 真纵切
复验为验收条件。无 Agent HTTP/Redis launch wire、Storage Proto、canonical SQL 或浏览器 API 变更。

## W1E authenticated transport 更新（2026-09-28）

`worker/platform.py` 现装配 worker-only signer、tenant token provider 与 run/fence factory；六个首批 RPC
由 pinned generated Connect sender 发送。每次请求 snapshot 后保留 logical request_id，重新计算 binding、
读取 lease、签 fresh JTI 并填 tag100；Bearer 与 proof 分离，错误不暴露上游 message/details。
OAuth 仅 `POST /iam/oauth2/token`、Basic、form `grant_type=client_credentials/resource/scope`，不传 tenant body，
无 refresh 或自动 retry；缓存从 exchange 起始时刻计时，仅余期 >5 秒复用。IAM 429 分类只针对直接 token endpoint；
Platform downstream IAM 429 仍由 owner 映射 Unavailable。本地 HTTP loopback 只证明网络/wire，不证明真实 owner 当前授权。
Agent 公共 HTTP/Redis schema 与 pinned Proto 不变；typed Skill/MCP selection/持久 fence、Storage 包体与真实三 owner 组合仍待后续。

## W1E Agent 消费 Platform owner RPC（2026-09-27；generated consumer/projector 已固定、runtime 尚未接线）

物理仓 `apps/kokoro-capability` 的 Platform owner main
`ee25c1f4d6df08be183ca10f7f5e852e0b21f641` 已发布唯一 Proto package
`kokoro.platform.v1` 与 inactive `platform-execution-operations` artifact `1.0.0`：31 exact RPC、
24 tenant-execution request binding、15 command digest。本仓现于 `contract/platform/v1/` 固定该提交的
两份只读 Proto 输入及 direct SHA，并在 `src/kokoro_agent/generated/` 保存 Python Protobuf/Connect
async client；同时逐字固定 execution-operation 原始 13 payload + provenance、14 个 direct SHA 与
aggregate，并在同一生成入口产生 24 个 tenant request 的 typed projector。代码内 trust anchor
固定 owner repository/commit 与 exact 14 条 consumer path/owner path/direct SHA（含 provenance
raw SHA）。隔离再生成 drift、31 RPC、24 双向 operation/method 描述及 wheel import 由本仓门
锁定；24 positive/134 projected-JSON binding negative/7 raw-parser negative 是 build-time artifact
checker 证据，不是 runtime Proto constructability 证据。`AuthorizeMcpTool` owner vector 没有 typed
arguments 的原始 preimage，因此只有 23/24
可从 owner typed preimage 重建；第 24 项使用派生 raw-byte 测试证明 SHA-256 取原始 Proto bytes，未虚报
owner preimage parity。generated consumer 仍无业务 adapter、proof supplier call、worker transport 或
current authorization，不能宣称 Agent→Platform 可用。

Set 值域精确使用 ECMAScript `trim()` 的 WhiteSpace + LineTerminator 码点集拒绝空串与
全空白（包含 U+FEFF，不包含 U+001C），再按 UTF-8 bytes 排序；owner artifact checker
的静态描述只写 `length>0`，因此这是已记录的 runtime consumer 收紧，不冒充 artifact
projected-JSON validator 有同样空白规则。

生成器 `buf-bin==1.73.0`（使用其内置 WKT）、`protoc-gen-py==0.1.1`、
`protoc-gen-connectrpc==0.11.1` 位于临时 Python 3.11 环境；应用环境只固定
`connectrpc==0.12.1` 与 `protobuf-py>=0.3.0,<0.4.0`。这是必要隔离：Connect generator 的依赖锁死
`protobuf-py==0.1.1`，与 runtime 要求冲突。官方 Beta 的取消包装也尚未在 adapter 恢复；下一片必须将
`ConnectError(canceled)` 还原为 asyncio 取消语义并覆盖 deadline 与 caller cancellation。

已核原始字节：`contract/proto/kokoro/platform/v1/platform_runtime.proto` SHA-256
`7c55fcadf5ba0753ca5d1bb304ccb96bf0318a4fb5c37aae4f96781c4ea53466`；
`contract/execution-operations/v1/manifest.json` SHA-256
`187bbeeceb1e082c15a8df3fc1451f89322575fa13276abe161a2c72fedad9c9`；
`request-bindings.json` SHA-256
`050291cf6f56052453ddb841f1e7cc624b2d3da28e5d9afd6abb9108349371fd`。
完整文件/aggregate provenance 由 owner `provenance.json` 定义，本仓同时 pin
`kokoro.common.v1` dependency Proto；上述机器门只声明离线生成/投影一致，不声明运行 transport。

| Agent 真实调用点                                  | Platform exact method / operation                                                                                                                                                                                            | 当前 shape 与目标                                                                                                                                          |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `agent_factory.resolve_declared_skills`           | `SkillSourceService/ResolveVisibleSkill` / `skill.resolve_visible_skill`                                                                                                                                                     | 当前 name selector；目标必须是 `SkillSourceRef`，source 当前可见且 active。                                                                                |
| `skills/backend.py` 的 `SkillReader.load_package` | `SkillSourceService/GetApprovedSkillPackageReference` / `skill.get_approved_package_reference`                                                                                                                               | 当前 `(scope,name,content_hash)`；目标以同一 typed source ref 取经 Platform/Storage gate 的 asset/read reference，并核内容摘要。                           |
| `tools/toolset.py` 的 MCP 组装                    | `McpConnectorService/GetMcpConnector` / `mcp.get_connector`；`McpConnectionService/GetMcpConnection` / `mcp.get_connection`；按需 `McpAuthorizationService/ListMcpConnectorCapabilities` / `mcp.list_connector_capabilities` | 当前 name+deployment；目标需 typed connector/connection ID 与 owner-current 可见性。                                                                       |
| `mcp/tools.py` 的 `mcp_call`                      | `McpAuthorizationService/AuthorizeMcpTool` / `mcp.authorize_tool`                                                                                                                                                            | 每次执行前以 typed connector、tool selector、原始 typed arguments bytes、approval ref presence、idempotency key 获取短期 grant；不把启动期快照当实时授权。 |

上述四组均属 `tenant-execution`、`proof=required`、`platform:execute`；request 的
`execution_proof` 是 tag 100 的 1..16384-byte ASCII compact JWS，不是 bearer。Connect HTTP
transport bearer 另由 IAM 的 `kokoro-agent` tenant-machine client 取得（audience
`https://kokoro.dev/resources/platform-internal`，scope `platform:execution.invoke`），
tenant 从 canonical Run 的 trusted identity 选择。`request_id` 在 logical retry 中固定，
但每次真实 send 用 owner typed projector 重新计算 binding、数据库时钟检查 lease 并 fresh sign。
Platform ingress 先校验 bearer；execution verifier 再按 operation/binding/当前 IAM 决策验 proof；
拒绝时 owner 数据库/Storage/provider 零副作用，completed receipt replay 也重新验当前权限/资源。

**形状缺口及上游顺序：** Platform `ResolveVisibleSkillRequest.source_ref` 已删除旧
`source_selector`，没有 name→ref 精确 RPC；`DiscoverVisibleSkills.query` 是查询而非解析。
按 owner ADR-002 §3.3/§13，Agent 声明必须保存 typed `SkillSourceRef`，由 BFF/Agent
声明来源先完成精确 ref 选择/传递。MCP task 使用 active `McpConnectorId`，policy 用
`McpConnectionId`，`McpServerId`/tool selector/URL/name 均不得互换。当前
`McpClient.resolve(selectors, identity, namespace, deployment)` 不携带这些 ID，也没有已证明
的 server/credential 映射，须先由声明与 Platform owner 精确字段闭环；不足时提出 owner-first
具名 gap 并拒绝该调用，不增临时 RPC 或兼容 wire。本仓不消费六个 BFF catalog
workload-only 或 System global-reserved 方法，亦不把 Agent HTTP/Redis wire 改为 Platform Proto。

IAM owner main `a4c2b61467f1fc1772d6b6d8e98f081c090289fb` 已发布 OpenAPI/SDK
`0.6.0` 的 Platform workload token introspection 与 execution verifier；这是 Platform server 的
下游，不是 Agent 直接调用的替代验证器。原文后面 2026-09-12 的“verifier 未实现”是历史阶段，
对当前 owner release 已过期；Agent 自己的 runtime transport/worker composition 仍未实现。

## 机器事实

- HTTP：[`contract/openapi/v1/openapi.json`](../contract/openapi/v1/openapi.json)
- Redis control/event：[`src/kokoro_agent/protocol/control.py`](../src/kokoro_agent/protocol/control.py)、
  [`events.py`](../src/kokoro_agent/protocol/events.py)、[`streams.py`](../src/kokoro_agent/protocol/streams.py)
- 契约 owner/provenance：[`contract/README.md`](../contract/README.md)

## HTTP 边界

Agent HTTP 是 `internal-owner`，不是浏览器 API，也不提供 AG-UI/SSE。路由只有 health/readiness、Run
admission/control/evidence、identity-scoped session list/history/replay 和 exact `/v1/execution-proof/jwks` GET/HEAD。
除下述匿名例外，所有 `/v1/*` 请求使用 service bearer、tenant/subject/actor/assertion trusted context；身份不从 body 推导。
匿名例外只有 exact `GET /healthz` and exact `GET|HEAD /v1/execution-proof/jwks`；其他 method/path 均先执行
service bearer 校验。

成功响应为 `{data, meta:{request_id}}`，错误为
`{error:{code,message},meta:{request_id}}`。错误 code 稳定且不暴露 SQL、堆栈、provider 原文或 secret。
`POST /v1/runs` 用 `run_id` 与不可变 request fence 幂等；control 必须带 `Idempotency-Key`，保存 digest。
列表 limit 有上限，session 使用 opaque cursor；run evidence 使用单调 `after_seq` 读取内部证据。

两个 Chat 启动/恢复消费入口的成功 `data` 已绑定机器类型：`createRun` 202 使用
`LaunchReceiptEnvelope.data -> LaunchReceipt {run_id, session_id, replayed}`；
`replaySessionEvents` 200 使用
`ReplayPageEnvelope.data -> ReplayPage {events, next_seq, watermark}`。`meta.request_id`
保持传输关联；`ChatEvent.chat_message_id` 在无消息关联的执行事件上序列化为 `null`，
非空时为非空字符串。重放为空时 `events=[]`、游标与 watermark 仍为数值。其他 operation 的泛型
`DataEnvelope` 不作为这两条响应的回退，BFF 应固定消费 Agent owner OpenAPI 版本与 digest。

执行 wire 的 `message.completed.payload.content` 允许空字符串：有真实模型终值时，空值仍是
该 `segment_id` 的完整最终快照；不以缺失事件或空 `message.delta` 表达。BFF 消费安全
`assistant.completed` replay 时按 `seq`/segment 更新最终内容，不能把较早的非空草稿当作
`run.completed` 的最终文本。没有模型终值、也没有文本 delta 的空投影不产生完成事件。
这是既有 `content: str` 契约的运行时修正，不新增 wire 字段、schema 或版本。

当前 Agent 内部事件和 chat view 的时间字段以 UTC epoch milliseconds 表达，这是现有 BFF adapter 的内部
传输约定；任何公开 Product API/AG-UI 输出必须在 BFF 边界转换为 RFC 3339 UTC。时间约定升级时同时修改
machine contract、consumer contract、实现、测试和文档。

## Redis 语义

- `kokoro:requests`：launch notification；完整请求以 PostgreSQL canonical intent 为准。
- `kokoro:run:{run_id}:control`：control command。
- `kokoro:run:{run_id}:events`：执行事件传输；持久顺序由 Agent ledger/receipt 保证。

stream 名称、maxlen、consumer group 和字段编码只在 `protocol/`/`streams/` owner 内定义；BFF 不直接消费
这些内部流。

## System owner 消费契约（2026-09-08，已接线、live smoke待验）

固定 owner `kokoro-system` commit `f5702068d4416ad90b1bd02af57d2825c32be916`，
OpenAPI `2.0.0` / SHA-256 `f9ea76f107e1ea0fc19df20ee7c59032c0fbac66e640e9a16a1b770ab27c1f37`。
本仓只保存依赖来源 pin，不维护可编辑 System OpenAPI 副本；字段事实源在 owner。

`POST /v1/system/model-catalog/resolve` 接收 `feature_key` 与可选 `label_key`；
服务身份 `kokoro-agent`，Bearer 为专用服务凭据，`x-kokoro-tenant-id` 来自 Run 的可信 ExecutionIdentity，
`x-request-id` 为安全 opaque 关联 ID（缺省生成）。不发送 provider 凭据、伪造权限或 body tenant。
System 成功仅 `{data}`，错误 `{error:{code,message,retryable}}`，每个响应带 `x-request-id`。
这描述新消费边界，不声称上文本仓旧 HTTP envelope 已同步改造。

路由必须匹配请求 feature 和显式 label；只允许 litellm transport，拒绝未知/缺失字段、无效版本/摘要和超限响应。
当前候选按本文 AGENT-FAILURE-CONTRACT 表验证 HTTP/code/retryable tuple，
替换了旧 status 掩码与已知模型错误的统一 assembly_failed，
只保留已验证的可用性 bool；取消与无自动重试保持，Run/Chat 只发布安全字段。

## Execution proof machine contract 与 JWKS 当前态（2026-09-12）

Agent A1 artifact 与 A2a signer 已提交，A2b 已提交：owner machine fact `contract/execution-proof/v1/schema.json` version=`1.0.0`、
canonical/negative/tampered-signature vectors、immutable Ed25519 signer、隔离 key loader、public-ring snapshot 与 HTTP JWKS projection
已分别验收。A2c standalone owner-internal component 现提供 statement-time lease reader 与 run-scoped supplier；A2c 不新增 wire，也未装入 worker
或真实 client。IAM verifier 与 inactive Platform proof wire 已由各自 owner 发布；本仓 production transport 仍待实现。后续仍按 TECHNICAL_DESIGN §7.5 串行消费；supplier
独立测试、Platform 最终 contract、Agent 真实 client 接线与 Platform server cutover 是不同验收门，不能互相冒充。
旧陈述“A2b 当前是待复审候选；A2c/consumer、IAM verifier、Platform proof wire 均待实现”不再描述 current fact。

IAM `bf160be173ef473bebe8e4a93b74ec52c230f180` 是当前 owner/方向基线，不是本候选已经发布的消费者契约：该提交仍只写正整数
`lease_generation`，尚无本节 safe-integer/token 矩阵，也未把六段交付全部展开。第 2 步必须在 IAM ADR、API、verifier、OpenAPI/SDK 与
测试中消费 Agent 固定 artifact 和同一拒绝向量；不得由 IAM 复制一份可独立漂移的 claims schema。

1. Agent A1 machine artifact、A2a signer、A2b key/JWKS 与 A2c standalone supplier 已在 owner 内实现；production consumer 尚待实现；
2. IAM 已发布 verifier/OpenAPI/generated SDK，并消费同一 strict profile、数字矩阵与 canonical vectors；
3. Platform owner 已发布 inactive compact-proof wire、request-binding 与 generated helper；
4. Agent 真实 Platform client 逐 call 接入 supplier；
5. Platform server 已删除旧 wire并实现 fresh/completed-replay receipt gate；代码发布不等于跨仓 activation；
6. Root 真实 Agent -> IAM -> Platform -> PostgreSQL receipt sandbox。

该顺序不产生临时 wire、fallback、alias 或双协议；第 1 步的 supplier unit/fake-client 证据不称为第 4 步的真实接线。

### Proof profile

proof 是最大 16 KiB 的 compact JWS/JWT。decoded protected header 和 claims 都是 strict object；字段重复、缺失或多余均非法：

| 部分             | 精确字段                                                                                                                                                                                     |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| protected header | `typ="kokoro-agent-execution+jwt"`、`alg="EdDSA"`、`kid`                                                                                                                                     |
| claims           | `contract_version="1.0.0"`、`iss`、`aud`、`tenant_ref`、`actor`、`subject`、`run_id`、`execution_session_id`、`lease_generation`、`operation`、`request_binding_sha256`、`iat`、`exp`、`jti` |

`kid`、`iss`、`tenant_ref`、`actor/subject.opaque_ref`、`run_id` 与 `execution_session_id` 只约束为非空字符串，不附加
pattern/maxLength，也不收窄当前 canonical Run/identity 可表达的 Unicode、空格、slash、URN 或长度。`actor`/`subject` 各自为
`{kind:"user|project|service",opaque_ref}`；kind 必须保留。`aud` 精确为
`https://kokoro.dev/resources/iam-execution-authorization`；`iss` 是部署固定、由 IAM 配置 exact match 的 Agent issuer。
`lease_generation` 必须是 JSON safe integer `1..9007199254740991`；`iat`/`exp` 必须是 JSON safe integer
`0..9007199254740991`。三者拒绝 boolean、float、负数与越界值，不截断、不 round、不转 string；`iat`/`exp` 仍要求
`0 < exp - iat <= 60`，且 `exp` 不晚于当前 lease expiry 的整秒下界。`operation` 匹配 `^[a-z][a-z0-9_.]{0,127}$`；binding 只接受
lowercase 64-hex。IAM verifier 的 5 秒 skew 语义固定为
`iat <= verifier_now + 5s` 且 `verifier_now < exp + 5s`；它不改变 minted `exp-iat`，但最坏接受时间可到 `iat+65s`。`jti` 是每次
Platform outbound call 新生成的 128-bit CSPRNG value，以匹配 `^[A-Za-z0-9_-]{21}[AQgw]$` 的 canonical 22-character
unpadded base64url 表达；解码必须恰好 16 bytes并重新编码相等，trailing pad-bit alias 也拒绝。V1 不允许 `nbf`、
`identity_assertion_ref`、permission/scope、Platform resource ID 或其他 claim。

header 与 payload 分别按 RFC 8785/JCS 产生 UTF-8 bytes，再用 unpadded base64url 组成 signing input；Ed25519 signature 同样用
unpadded base64url。字符串不做隐式 Unicode normalization，JSON number 不使用 float，解析拒绝 duplicate member。固定 test vector 是
canonical bytes 的验收事实，不以不同 JSON serialization 只要能验签为兼容。

机器 schema/runtime/canonical vector 的数字矩阵固定为：

| 输入                          | `lease_generation` | `iat`/`exp` type/range 层 | 后续时间语义                                      |
| ----------------------------- | ------------------ | ------------------------- | ------------------------------------------------- |
| `9007199254740991` (`2^53-1`) | 接受               | 接受                      | `iat/exp` 仍检查 pair/clock/TTL                   |
| `9007199254740992` (`2^53`)   | 拒绝               | 拒绝                      | 不进入 crypto                                     |
| `9007199254740993` (`2^53+1`) | 拒绝               | 拒绝                      | 不进入 crypto                                     |
| `0`                           | 拒绝               | 接受 range                | 只有满足 `exp>iat` 和 current-time 的 pair 可继续 |
| 负数                          | 拒绝               | 拒绝                      | 不进入 crypto                                     |
| JSON `true`/`false`           | 拒绝               | 拒绝                      | 不把 bool 当 int                                  |
| `1.0` 或其他 float token      | 拒绝               | 拒绝                      | 不 coerce 为 integer                              |

JSON Schema 的 `type/minimum/maximum` 与 strict runtime type guard 共同执行该矩阵；由于 JSON Schema 按数学值可能把 `1.0` 视为
integer，当前 schema 对三字段标记 `x-kokoro-require-integer-token=true`，由 contract-check、canonical-byte equality 与
pre-coercion parser 从原始 JSON token 拒绝 float。

`operation` 与 `request_binding_sha256` 来自 Platform owner 的固定 generated contract/helper。Agent adapter 在 Platform call boundary
完成 enum/shape 与 binding 校验后传给 proof supplier；signer 不维护 IAM 的 5 Skill/16 MCP permission catalog，也不解析
`series_id/skill_id/installation_id` 或 `connector_id/server_id/connection_id/authorization_id/invocation_grant`。这些 typed opaque
reference 只被 Platform canonical request binding 覆盖，不作为 proof 独立 claim。该约束只借鉴 Root/IAM 已核验的 Manus v2
list-first/reference-by-ID 原则，不复制 Manus wire、ID 或 owner。

### JWKS HTTP projection

```http
GET /v1/execution-proof/jwks
HEAD /v1/execution-proof/jwks
```

- visibility 为 `internal-owner`；仅通过内部网络暴露。
- public key 不是 secret，因此该路径是不要求 service bearer 的具名例外；也不接受 bearer 身份作为业务输入。
- 不接受 tenant/actor/subject/assertion header、query 或 body；带 query/body 的请求返回 400 execution_proof_jwks_invalid_request。
- GET 200 返回标准 JWKS `{ "keys": [...] }`；HEAD 200 返回相同 status 与 representation headers（`Content-Length` 为 GET 表示长度）但不写 body。
- key 仅含 `kty="OKP"`、`crv="Ed25519"`、`use="sig"`、`alg="EdDSA"`、唯一 `kid`、32-byte unpadded-base64url `x`；
  禁止 private `d`、`x5c`、`x5u`、`jku`、`jwk` 与额外字段。
- known path 的非 GET/HEAD 为 405，带 `Allow: GET, HEAD`；unknown v1 path 为 404。
- 200/400/404/405/503 都发送 `Cache-Control: no-store`；不发送 ETag，不支持 304。IAM 自己的 30 秒 fresh snapshot 是唯一 cache。
- route 不重定向，JWKS representation body 不超过 64 KiB（不含 HTTP headers）；兼容 IAM 对 response body 的 2 秒总 timeout、64 KiB limit、禁止 redirect、per-issuer
  single-flight、known-key 30 秒 freshness和 fresh unknown-kid最多一次强制 refresh策略。
- public ring 缺失、格式错误、重复 kid、HTTP active descriptor 缺失或其 kid/thumbprint 不在当前 ring 时 readiness 为 503，JWKS route 也为 503，
  不发布部分 key set。

该 route 已加入 `contract/openapi/v1/openapi.json` 的 HTTP `1.1.0`；OpenAPI 只描述 HTTP method/response/JWK shape并引用 Agent-owned proof artifact，
operation治理元数据固定 `x-kokoro-permission=none`，不在 OpenAPI 重建 claims schema。`contract/provenance.json` 同时纳入 proof schema；
当前 `kokoro-agent-contract-check` 已校验 schema、source digest、canonical/signature/JWK vectors、单 bit tampered signature fixture、
duplicate/token/schema/JCS/base64url/TTL 负向语义；A2a 已实现并验证 Ed25519 数学签名、自验与 tampered rejection。

### 调用与错误边界

run-scoped supplier 必须在每次 Skills/MCP owner HTTP/RPC call 前使用 PostgreSQL database clock确认 same
`run_id + lease owner + generation`、unexpired、not paused、not terminal，再即时签发；build-time proof 与 proof cache 均非法。无法证明 lease
current、应用 clock 与 database clock 超过 5 秒、key/config/canonicalization失败时，本次 Platform 调用 fail closed且不发送请求。

签发后 lease 被接管或终态化与网络在途并发无法由 compact proof 原子撤销；旧 proof 的 claim TTL 不超过 60 秒且 `exp` 不晚于原
lease expiry，IAM 的验证端 skew 仍可能把最坏接受时间延到 `exp+5s`。V1 不提供 introspection/revocation endpoint。IAM 仍对每次
Platform action/replay 重验其当前事实，Platform 仍执行自己的
resource/provider/receipt policy；任一依赖失败都不得复用旧 allow/proof。

正常 rotation 的可观察组合固定为：`ring{old}/http old/worker old` -> `ring{old,new}/http old/worker old` ->
`ring{old,new}/http old/worker new` -> `ring{old,new}/http new/worker new` -> 等待最后一次 old 签名至少 70 秒 ->
`ring{new}/http new/worker new`。每阶段覆盖全部 replica；有落后、readiness 失败或 thumbprint 不一致就停止，不带病进入下一阶段。

### HTTP 1.1.0 JWKS current wire (A2b)

Exact anonymous `GET|HEAD /v1/execution-proof/jwks` returns the precomputed RFC 8785 JWK set as `application/jwk-set+json`; GET and HEAD share representation status/type/cache/length and HEAD writes no body. Query markers, transfer/expect framing, identity-header presence, and any content length other than one exact OWS-trimmed `0` are `400 execution_proof_jwks_invalid_request`. Other methods are `405 execution_proof_jwks_method_not_allowed` with `Allow: GET, HEAD`; missing/invalid ring is `503 execution_proof_jwks_unavailable`. All responses are `no-store`, without ETag, 304, redirect, bearer, tenant, or database dependency.

Launch identity remains trusted `X-Kokoro-*` headers. It is not accepted from the launch body. The standalone A2c supplier adds no HTTP wire; IAM verifier, Platform proof wire, production composition, and real transport are not part of HTTP `1.1.0`.
