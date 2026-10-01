# kokoro-agent 数据模型

## AGENT-PROFILE-P3A-R25：Run静态绑定已编码、真PG待验（2026-10-01）

基线main a37e8f1。现候选canonical schema在kokoro_agent_run新增assembly_recipe_bytes BYTEA与
assembly_recipe_fingerprint TEXT，成对NULL/非NULL、1..8388608 bytes/小写64hex具名CHECK；无新表/索引/外键/迁移。
RunProfilePort由新PostgresRunProfiles执行同Run行锁/锁后DB clock/原TEXT身份与lease generation校验；首次bind同事务两列写，
已有binding严格重算与逐byte比较、matched不更新；错误/取消rollback，失去旧authority不冒新generation终态。
真正fresh install/catalog精确约束drift与43个PG用例仅collect待Root执行，尚不宣称数据库门已通过。
profile与Run同生命周期，不另设TTL。当前没有scope/head/origin/effective第二阶段字段；下方完整4引用保护/两阶段继承与
最终release仍为后继硬门，不用static SHA冒充完整profile。HTTP机器源保持3.0，未改任何协议/生成物。

## 历史 AGENT-PROFILE-P3-D0：下一片Run静态绑定字段与最终两阶段（2026-10-01）

当前main `7e902c08296cacdacfe810ccbb4a6233d1b2ca7b`，P2已验；canonical `database/schema.sql` 仍无profile字段。
以下为目标，D0没有改SQL。下一最短P3A仅现Run两列，完整scope/dispatch lineage/native字段依下方正式4目标另片实施。

| P3A目标列/约束 | 精确语义 |
| --- | --- |
| assembly_recipe_bytes BYTEA NULL | P2同plan的canonical UTF8原bytes，含domain_tag=kokoro-agent:assembly-recipe:1；上限8MiB，拒空；不存对象/secret/URL。NULL仅fresh Run尚未freeze。 |
| assembly_recipe_fingerprint TEXT NULL | bytes的SHA256小写64hex；两列必须同NULL或同非NULL，首次绑定后不可修改，verify无updated_at变更。不是runtime_profile_digest。 |
| ck_kokoro_agent_run_static_recipe | 显式约束两个NULL或两个非NULL，后者octet_length 1..8388608且digest符合^[0-9a-f]{64}$；NULL三值逻辑用IS NULL/IS NOT NULL表达。哈希/canonical/domain匹配由typed adapter锁内验证。 |

不新建表/索引/外键/版本号默认值；以现tenant+run_id定位、PK加锁，不为两个不可查询摘要列建索引。
父表就是Run，无第二生命周期、孤儿reconciliation或跨owner关系。P3A purge随Run现有事务同删，不另设profile TTL；
完整4的origin/latest/committed/native引用保护与最终release必须后继补齐，现TTL不声称满足未来retention。

内部port与事务以TECH P3A为唯一编排定义：完整trusted request+lease+binding入参；同连接Run行锁，锁后DB clock
重新验证tenant/canonical请求/owner/generation/nonterminal/active expiry；第一绑定或严格逐byte/摘要比较；零网络持锁。
初始NULL且无执行事实可以freeze，包含尚未执行即崩溃重claim的Run；已有event/outbox/usage/sandbox事实却NULL拒绝，
部分NULL/未知tag/坏hash亦拒绝。该检查不从计数器推断外部安全，真正零外部调用由factory先commit的执行顺序保证。
lease失效/terminal竞争零写，DB取消或statement故障全rollback；ACK丢失重连verify，不重写冻结值。
当前只Run fence；完整4必须scope→有序dispatch→Run同锁序升级，绝不加advisory scope旁路或冒称P3A已有active/head。

唯一请求身份序列化式为 `request.model_dump_json().encode("utf-8")`（不传任何dump参数），
与现 `postgres_run_dispatch.py:43,76,97` 写入/claim所用 `request.model_dump_json()` 完全同源。
锁内读取的原 `request_json` TEXT 直接 `.encode("utf-8")` 后逐byte比较；不先parse再dump、
不使用profile的canonical_json，不按dict/Pydantic对象相等或JSONB等价授权，也不新增helper。
字段重排、额外whitespace、等价JSON转义/表示、显式默认与省略默认差异、未知字段，即便解析后对象等价仍typed拒绝；
缺失/非TEXT/非法UTF8同样拒绝。recipe自身的canonical编码与请求原TEXT身份是不同边界，不能混用。

真PG负向矩阵逐例改stored原TEXT的字段顺序、whitespace、等价转义/表示、显式默认/省略默认、未知字段，
验证typed拒绝且binding零写；原字节正例通过。正常首次claim的RUNNING不是执行事实，不能因此拒首次freeze；
以实际durable/event counters、usage、sandbox及执行记录判断，保持原generation错误收口。

fresh install只目标空owner schema；现verify仅缺表不足，P3A必须加这两列的类型/null/default及具名CHECK catalog drift验证，
缺列或错误DDL的旧schema直接拒绝，不自动ALTER/migrate/default补值。其余既有schema全域drift完善仍属完整4门，不声称本片已完成。
真实PG两连接/锁等待后expiry/取消/ACK lost/漂移矩阵见TECH，不能只用schema文本匹配或fake Repo通过。

### P3B/完整4的字段和继承语义（不是本P3A提前添加空列）

未来Run再增加effective_native_policy_bytes/digest成对字段，canonical版本含独立domain tag；
完整profile身份=(static版本/bytes/digest,effective版本/bytes/digest)，两阶段都存实际值并验证，不用静态recipe代替最终policy。
第二阶段包含所有peer及其GP/catalog的实际prompt、工具覆盖/exclusion/schema/顺序和middleware source/安全选项，
在route后仅本地构造、所有sandbox/provider执行前一次绑定；字段不包含route revision/health/凭据或运行对象。
normal首Run可处于static已冻/effective未绑的明确前执行窗口；重claim可以继续首次绑定，已有native/HITL执行而NULL视为损坏。
retry新Run不能补原父缺失值：scope事务内验两阶段完整，从origin复制原bytes/digest再由worker严格比较。
后续合法takeover/resume只比较既有值，不重算覆盖；旧generation即便相同bytes仍无写权。

完整4原表格的runtime_profile_digest须理解为完整两阶段组合身份，不把P3A列替换/默认成它。
完整scope lock、baseline/head/native DAG、terminal committed晋升/active释放与bounded引用GC目标全部保留。
Conversation最终释放通知/幂等/drain/privacy决定仍待BFF/Agent/产品，不能以永久保留代替GC闭环，也不阻P3A实施。

## 历史 AGENT-PROFILE-P2-R24：装配候选仍零持久化（2026-10-01）

基线`9dcaa34a3664668c3ad2da6adcc71f271ea96224`。生产SourceManifest/RuntimeAssemblyPolicy、单次PreparedFeaturePlan
及其bytes/fingerprint已接真实factory；显式资源只读，绑定工具实例与native registry身份仅进程内比较，绝不序列化。
没有SQL/索引/事务/Redis key/Run identity/profile列变化，没有用内存registry seal冒充Run scope/generation fence。
recipe包含源码/白名单政策，排除运行凭据/URL/绑定对象；RuntimeAssemblyPolicy与真正worker设置漂移时拒绝装配。
完整Run profile依然需要两阶段持久身份；effective-native实际结果和缺阶段retry资格在后继SQL/事务门处理，不补NULL、
不把P2 fingerprint当完整冻结。Conversation生命周期最终引用释放仍独立未决；没有永久免GC或新持久状态机。
26路径范围及离线验证见TECH/CURRENT；本轮未执行数据库/Redis或provider验收，未更新canonical schema或机器契约。

## 历史 AGENT-P2-D0-R24：只读包来源与单次装配内存（2026-10-01；仅设计）

当前基线`ec65d04f9915580eb57629126fffffc20f4c4033`已含Root验收P1；SQL仍无scope/profile持久freeze。
P2拟对象为进程不可变RuntimeAssemblyPolicy/显式SourceManifest，以及单次build的PreparedFeaturePlan；
包source字节只读，无新SQL/表/列/索引/事务/Redis key/保留策略。计划不跨worker持久、不复用旧Run计划，
不为NULL digest补值，不代替Run/lease/native head身份。每次当前授权照常重验。

`assembly_recipe_fingerprint`是批准静态recipe/源码/政策集合的内存证明，**不是**Run.runtime_profile_digest；
R24已裁决完整profile由pre-System static recipe envelope冻结及route后有效native政策绑定两阶段组成。
第二阶段`effective_native_policy_digest`覆盖main+全部peer实际prompt/tool override/exclusion/GP/middleware source，
仅本地model构造后、全部sandbox/provider执行前以scope/lease/generation fence持久绑定；retry/resume/takeover严格
比较继承身份，缺值不等于匹配、不以当前值补写绕过。route revision/health/凭据不纳入。此SQL/事务属独立后继，
本P2既不新增该列，也不把内存fingerprint视为已持久冻结；完整Run profile目标没有降级为静态recipe。
P2不把动态route/凭据/namespace/workspace实例/client/checkpointer对象存入配方；backend仅显式无secret政策。
完整source闭包与源码/wheel一致的验收不等于真实PG事务、恢复或跨owner授权证据。

未来正式scope freeze仍须scope-first、锁后DBclock、normal首次冻结/retry继承比较、HITL占active、
terminal原子推进committed/latest、native引用DAG与bounded GC；当前finalize_terminal不可被第二终态器替代。
Conversation删除/expiry仍只阻最终取消/保留/释放语义及完整发布，不阻P2。不得永久免GC假装闭环。
生产plugin批准集合默认空；pure metadata gate在lazy bootstrap/load/call前拒unknown/重复/同key冲突，登记清单
显式有序，late registry mutation拒绝；runtime-only middleware不静态执行。该进程封闭source身份不是持久授权缓存。
精确25文件、plan同源与RED矩阵以TECHNICAL_DESIGN P2为准；本D0不改canonical schema或机器字节。

## AGENT-PROFILE-P1：纯内存值与零持久化变更（2026-10-01）

D0基线75701413之后的P1仅产不可变选择计划、白名单metadata、canonical bytes与SHA256。
显式包资源读取只计算代码来源，不访问数据库/Redis/owner网络，不写Run/checkpoint或业务文件。
Toolbox记录无secret业务选项，profile投影拒缺metadata/实际挂载漂移；旧内部构造仍可按现行为build，
这不是持久旧数据fallback，也不为未来retry补NULL digest。预算0关闭语义保留，不改运行限额。
canonical SQL/列/索引/事务/retention及机器源无变化；生产scope/profile冻结/native fence均未由P1实现。
源码资源fixture与wheel证据只证明纯配方/包装，不代替真实PG/Redis或生命周期释放验收。

## AGENT4-D0：当前 SQL、P1 零持久化边界与完整发布门（2026-10-01）

当前 Agent main `224d0f19ff2199c38b95f621015ea7856f589454` canonical SQL 尚无 scope、lineage、baseline/head、
runtime_profile_digest。Run lease_generation 是每 Run 的 fence，不是 session 全局 generation。
当前唯一 finalize_terminal 已将 usage/terminal/outbox/Chat identity/seq/cleanup 同连接提交；未来在此事务增加成功
committed head/user_seq 晋升与 active 释放，不重建第二终态机制。BFF e7a325ce 的 Chat terminal FIFO 已验收，
不代替本仓持久 scope，也不证明 Scheduled 同 session 或 native DAG。

P1 只产纯选择计划/编码 bytes/digest，供现 build 同源选择及后继正式 profile 装配使用；不持久、无 DB I/O，
不改 canonical SQL/表/列/约束/索引/事务/Redis namespace/RunRequest/retention。fixture 的完整 profile 不冒充生产已冻结。
本轮 D0 与下一 P1 不添加占位 scope/nullable fallback；精确文件与 RED 矩阵见 TECHNICAL_DESIGN。

scope/native 的后继实现子门与完整生命周期发布门分开：用户未决 Conversation 删除/expiry 阻断最终引用释放，
不阻独立 P1。活跃/latest/committed 引用保护必须随正式引用字段实现；bounded purge/reachability 必须真实验，
不得把永久保留当 GC 完成。最终取消/drain、通知/重放、释放时机、privacy 删除与 session 复用语义确定后才发布完整4.0。
本轮不创造 DELETE/tombstone/保留时长。schema 验证须覆盖列/类型/default/约束/索引，现 `verify_agent_schema`
只检查表存在，不足以宣称完整 drift；新增字段/约束应在正式 schema 切片补真实 catalog 负向测试。

## AGENT-DURABLE-INGRESS-P0：现 dispatch 事实的唯一入口（2026-10-01）

本片不新增表、列、索引或 migration。`kokoro_agent_run_dispatch` 仍是 Run 启动前的
durable admission 事实；worker 只能将其现有 `pending` 行与 `kokoro_agent_run` lease 在
`claim_dispatch` 的单事务中收敛为 claimed。Redis frame 中除 `run_id` 外的字段不是该转换的
持久事实源；无 pending dispatch 时禁止直接插入 Run。现 terminal、lease、control、purge 数据语义
不变；本片不建立 scope/head/lineage 事实，不处理 retention 决策。

## AGENT-TERMINAL-ATOMIC/P0：现表原子终态切片（2026-10-01）

基线dd5afc3528fe3a835756bc3ff55dfacaa8ca76d3，canonical schema原字节不变，无新字段/表/索引、
无migration/retention改写。现Run/usage_segment/outbox/Chat event/message/sequence/control/cleanup足以承接
局部原子收口；scope/head/userseq晋升是后续4.0真实实现，本片不增空占位。
唯一postgres_run_leases.finalize_terminal协调同连接：Run锁/DB clock重验→receipt/delivery核验→usage→前置及terminal Chat身份与seq→outbox/
control/cleanup→Run终态；相应子表writer先Run锁，避免反序。调用现Chat唯一append_on_cursor primitive，不复制SQL。
完整projection在锁内确定index/timestamp/usage后调用纯project_chat_fact；网络全部在commit之后。
最终usage使用现(run_id,lease_generation) PK，同数字重放只读、不同数字全rollback；pause段独立提交，resume
adopt新generation；终态段与terminal事实同txn。终态后add_usage首次新段拒绝、aggregate不变；既存同段精确重放只读、漂移conflict；取消缺席段不补零、不丢旧总数。
delivery snapshot锁内重读，并确认每个成功delivery的Chat identity/source_index/content已持久；queued outbox
不足以通过barrier。cancel command/receipt与terminal同txn，旧generation回执不得篡改赢家。
Chat/outbox/usage/cleanup/Run全部rollback或全部commit，随后只发布Redis；固定terminal重放复用原Chat seq。
现receipt GC可删已消费outbox，重放按Run+Chat与现manifest/receipt核对，只重发retained queued帧，不复活已消费行。
本片不承诺历史旧孤儿terminal的自动修复，也不清理用户数据；新事务路径无先terminal后Chat/outbox窗口。
真实矩阵见TECHNICAL_DESIGN本片：Chat后故障rollback、ACK lost新连接恢复、delivery race、过期lease、usage多段、
live/terminal两序、HTTP已见而Redis失败。Chat message/event计数器不得跨kind比较；当前无scope门不证明并发FIFO。
NACK为同一事务协调器内quarantined disposition：同锁核持久rejected receipt/fence，Run terminal/cleanup及
superseded私有terminal audit同commit，不新增Chat/Redis，保留原fence；delivery缺失不阻断。重放依据receipt+Run+audit，
不要求Chat。负向覆盖伪receipt/漂移fence/重复命令/无公开帧与不推进毒化consumer；完整4.0数据门/retention仍未通过。

NACK receipt须同事务按(run_id,seq,event_id)匹配offending outbox；reconcile只报告候选不先写fence。隔离commit同时supersede被拒及fence后open帧、写稳定私有audit，retained_frames恒空；失败零mutation，与natural/cancel单赢家且不覆写。

normal/cancel 同锁核有效 rejected receipt 优先，错误 event_id 不参与阻挡；terminal 的 typed winner 已提交时后到 NACK 不覆写。delivery Chat 必须同时匹配 request tenant/session/namespace/run/index、canonical chat_event_id 和 payload。terminal replay/recovery 额外核固定 fence/outbox identity 与 Chat session/source/index/time；缺失 fail-closed，不新增事实。retained started 缺 Chat 在同事务按 outbox index 先投影再 terminal；全部事务回滚时其 seq 也回滚。

职责收敛：postgres_run_leases 保持唯一 finalize 同连接事务编排；postgres_run_events 承接 delivery_ready_on_cursor、terminal_chat_on_cursor 与只读 verify_terminal_frame，façade 的只读验证指向 events。events 不 import leases，不复制 SQL、不新增模块，也不放宽现 800 行门。

本片 delivery GC 收敛：reconcile 先取得同一 Run 锁；active Run 的已 ACK delivery.created 保留原 event_id/index/time/payload，consumed watermark 仍推进，非 delivery 正常 GC。terminal 后按最终 consumed 水位重扫而不依赖本轮推进，避免 ensure 重建第二 delivery/Chat。没有新表/API/ledger、没有永久跳过 Run purge、没有退回本地时钟或弱化 barrier。真实测试同时覆盖 natural/cancel、持真实 Run 行锁的 GC↔ensure 与 GC↔finalize、最终 ACK GC 后 replay 不重建；本片上述矩阵已由 Root 执行，最终实测证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。

最后两处边界：add_usage（含 pause 段）先锁 Run，再通过 context.database_now 读取数据库时钟校验 active expiry；terminal 只允许已存精确 segment 重放，不接受新段。quarantined replay 同事务严格核 private audit kind/payload、NULL index、timestamp=terminal_at、durable_seq=Run counter 且越过 rejected fence；身份/内容漂移返回 lost，不补写、不公开、不换 winner。Root 已跑 true PG RED（usage 一例/private audit 六例），本片上述矩阵已由 Root 执行，最终实测证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。

## AGENT-RETRY-DESIGN：目标 lineage 与 native baseline（2026-09-30，未实施）

基线 `f3be3b97dd67df69ed3c6cb88c59f3bc2db97703` 的 `database/schema.sql` 本门原字节不变。
下表是 SQL-first 后继切片的确切目标；只在 Agent owner schema 内，fresh install，无 FK/migration/跨 owner JOIN。
`RunScope.scoped_thread_id` 继续由可信 tenant/subject namespace 与 session 派生，native root ns=`""`。

| 存放 | 目标字段/NULL/default 与查询理由 |
| --- | --- |
| 新 `kokoro_agent_run_scope` | PK `(tenant_id TEXT, namespace TEXT, session_id TEXT)` 均 NOT NULL；`latest_origin_run_id TEXT`、`latest_attempt_run_id TEXT`、`active_run_id TEXT` 可空，初建 NULL；`committed_checkpoint_id TEXT` 初建 NULL，仅初始化 genesis 期间允许；`committed_user_seq BIGINT NOT NULL DEFAULT 0`；`created_at/updated_at TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)`。根 thread/ns 可由 scope 唯一推导不重复存。该行只由 run admission/native/terminal 协调写，不复用展示 chat_session。 |
| 现 `kokoro_agent_run_dispatch` | 新 `origin_run_id TEXT NOT NULL`（无默认，normal=self）、`retry_of_run_id TEXT NULL`（normal NULL）；`baseline_kind TEXT NOT NULL`（empty/native）、`baseline_checkpoint_id TEXT NOT NULL`（empty 亦是已持久 genesis ID）；`baseline_user_seq BIGINT NOT NULL`、`input_user_seq BIGINT NOT NULL`。两 seq 冻结原 native 缺失 user 的读取窗口，retry 从 origin 原样复制。既有 tenant/namespace/session 确定 root locator；request_json/fence 也承接 typed parent，row 与 canonical 比较一致。 |
| 现 `kokoro_agent_run` | 新 `native_head_checkpoint_id TEXT NULL`（尚未写本 attempt checkpoint 时 NULL，绝不表示可取 latest）；`runtime_profile_digest` 仍指完整Run profile身份目标，不以静态recipe单独充当；后继须给static recipe envelope与 `effective_native_policy_digest` 的两阶段持久表示（前者preflight前冻结、后者route后全peer实际政策绑定，未绑定不是相等/空政策）。确切列/类型/约束、完整身份组合、继承/比较与缺阶段retry资格须在独立SQL门精化，P2不加列。Run PK/tenant/lease/terminal/usage 均保留；claim 同事务读 dispatch 固定 baseline，避免第二套 request。 |
| 原 user/message | 不改 Chat PK `(tenant_id,chat_message_id)` 或 seq 唯一键。`role=user.run_id=origin_run_id`，retry 查回并验证同 tenant/namespace/session/message/content/status，返回原记录，不 UPDATE run_id/seq/time。非 user 的完整 immutable identity 原样保留。 |
| 原 native 三表 | `checkpoints/checkpoint_blobs/checkpoint_writes` 不复制为 payload 快照表、不更改上游格式。使用完整 native parent/checkpoint/channel versions/namespace，delta/子图/pending writes 不手工剪裁。旧分支只作证据。 |

必要约束/索引：scope PK 已支持唯一串行入口，不叠加等价索引；scope 具名 CHECK 保证
`committed_user_seq>=0`，latest 两字段同空/同非空（active 可空）；dispatch 具名 CHECK 保证
baseline_kind 二值、`0<=baseline_user_seq<input_user_seq`、normal 的 origin=self/retry parent 非 self，
同一 attempt 只一个业务父。新增 partial UNIQUE `(tenant_id,retry_of_run_id) WHERE retry_of_run_id IS NOT NULL`
保护一个失败 attempt 至多一个直接后继（同 key replay不新行）；不因查询 lineage 预建 origin 单列索引。
将 Chat 原 `(tenant,namespace,session,seq)` 唯一索引用于缺失 user 范围读取，role 在受限区间过滤，暂不加冗余索引。
新字段无“修旧数据”默认值；commit 前 schema check 与真实 EXPLAIN/并发测试核验 SQL 名称和必要性。

### 事务与恢复

所有相关写统一锁序：**scope（完整 key 字典序）→ dispatch（run_id 序）→ Run（run_id 序）→
chat identity/sequence → native → outbox/receipt/control/tool/cleanup 子行**；尾部不需要的组可跳过，
同组按完整 PK 排序，取得后禁止再取前组。run_id-only 入口先 **无锁预读** canonical locator
(tenant,namespace,session,run_id)，该结果仅寻锁不授权；scope-first 后在同 transaction 重读 dispatch/Run，
精确比较 locator、canonical identity、owner/generation、state 与该操作允许的 active/terminal fence；取得全部前置锁后单独读取数据库时钟重验 expiry，
不复用等待锁前计算的 Python now，也不把只含等待前谓词的 UPDATE 当作锁后到期验证。
预读后删除、scope不符或 generation变化即失败/无写；不凭预读结果继续。外部网络不放事务。
当前目标不额外增加scope_generation；active_run_id、Run owner/generation和expected native head共同授权。
cleanup_id-only入口先无锁读cleanup→run locator再同序锁后重读；历史terminal证据重放核原attempt，不冒领新active身份。

| 入口锁矩阵（现方法；必须同片统一） | scope-first 后的重验/写边界 |
| --- | --- |
| enqueue_dispatch、try_claim/claim_dispatch | 锁相关 origin/parent/new dispatch 与 Run（各按 run_id）；核 frozen identity/baseline/profile，再 admission占位或claim/createRun；不能先 UPDATE dispatch/Run再找scope |
| adopt、renew、pause | 精确旧 generation及 paused/active 条件重验；scope仍指本attempt才变更租约 |
| reclaim_expired | 无锁选候选 locator，按 scope key、run_id排序逐scope事务；锁后DB clock重验过期，不保留当前先批量UPDATE Run的路径 |
| finalize_terminal（execution/cancel/quarantine authority） | scope→dispatch/Run→Chat→native→outbox/usage/receipt/cleanup；typed outcome的最终usage段、delivery Chat barrier、terminal/outbox/Chat身份及session seq/cleanup、仅completed成功head晋升与active释放同事务；quarantine沿现私有audit不新增公开Chat。保留现无先CAS后emit窗口，禁止release后补Chat |
| stage_critical_frame、reserve/next_event_index、mark_critical_published | run_id定位后同序重读；emit需当前active lease；terminal重投仅原terminal fence/已持久event身份，不允许新active Run冒领 |
| reconcile_receipts、control admission/delivery/status | 同序重读后receipt/manifest/control CAS；允许终态补收口不代表跳过scope/原fence或重开Run |
| Chat save_message/append/append_fenced/ensure_session | 已知scope直接先锁scope；run关联投影先锁dispatch/Run再chat identity/sequence；user只normal创建，retry精确复用origin；纯展示写不反向锁Run |
| tool journal/result、steer、usage、sandbox bind/cleanup claim/complete/reschedule | 同序进入所属子行，重验相应lease或已持久cleanup身份；scope锁不覆盖外部tool/network，回执写重新验证；本片前execute_active_effect持锁await Redis的旧路径已删除，live现于PG提交后无锁发布；cleanup_id-only先无锁定位run/scope，锁后重读原cleanup身份，GC不能从子行反向寻scope锁 |
| native aput/aput_writes | 无锁locator→scope→dispatch/Run重读→native保存及head CAS；同连接/同commit；新attempt不得写origin baseline或失败兄弟分支 |
| purge_terminal/delete_run_rows、native DAG GC | 无锁候选→按scope排序锁→dispatch/Run按ID锁→重验age、cleanup和全部引用后删；不靠候选快照或仅terminal TTL删，生命周期释放未裁决则不可宣称整scope回收已完成 |

两条连接真实 RED→GREEN 矩阵（明确同步 barrier，不靠 sleep）：A 持 scope 后等待Run、B 从 run_id入口写；
目标 B 先等待scope而非持Run，释放A后两者完成且无deadlock/越权更新。逐组覆盖 claim/adopt/renew/pause/reclaim/
cancel/terminal/event/receipt/chat/tool/native/GC；另以 A 在 B 预读后推进generation或删除候选，B 锁后必须重读拒绝。
双scope反向输入必须同排序；GC-vs-retry、terminal-vs-native、reclaim-vs-renew、receipt-vs-terminal 均核胜者唯一、
失败事务零mutation、无孤儿baseline/Run/outbox，原查询超时/deadlock不得靠增大timeout掩盖。

- Admission 的同一事务创建/锁 scope、读取当前 caller 范围内 parent/origin、验证 receipt/终态/最新性，
  normal 仅一次持久 user，保存 baseline/input seq、dispatch 并占 active。重复 same fence 原 ACK；不同 key
  竞争按唯一约束/行锁冲突，无第二 assistant（BFF 自有事务负责）或 Run。launch通知是否已发布不释放active；仅完整terminal原子事务释放，不等待Redis terminal发布。
- 首 scope 在同事务以公共 native `empty_checkpoint()`/`AsyncPostgresSaver.aput` 建 genesis：
  metadata source=input/step=-2、channel/pending 空，scope committed locator 与 dispatch baseline 同 commit。
  native saver 使用相同 connection，失败回滚全部；empty kind 是业务显式标记，不用随机缺失 id触发 native empty fallback。
- 正式4 worker claim/createRun在scope、parent/origin/new dispatch/Run有序锁内原子继承冻结recipe与baseline/window；
  replay只读原事实，正常首次static recipe freeze在全部外部preflight/System前。R24另设route后全peer有效native政策
  绑定：仅本地model构造，任何sandbox/provider执行前在同scope/lease/generation fence核身份并持久第二阶段digest。
  两阶段共同构成完整profile，recipe相同不证明实际prompt/tools/GP/middleware相同。retry/resume/takeover必须核
  继承值相等，漂移拒绝不改选；不复制secret/route revision/health/token，当前授权仍重验。
  精确schema/阶段状态与“第一阶段已冻但第二阶段前失败”的retry资格/恢复由独立后继门审定，不以NULL匹配、
  自动补当前native值或随意改Failure.retryable绕过。必须覆盖全peer绑定前零sandbox/provider、双阶段漂移、缺阶段、
  generation失效、原子复制/绑定回滚、相同key replay及normal首次绑定；P2没有这些持久实现或事务验收。
- 原生 write adapter 用独占同连接 transaction，statement-time DB clock 验租约及 active Run，委托 saver 公共
  `aput/aput_writes`，root head CAS 同 commit；失效 lease/并发取消/错误 parent 全部 rollback。
  pending writes 只接受该 attempt 自有 checkpoint，绝不写 immutable baseline；若 installed native 在 delta/input
  路径必须写 baseline，需先完成原生隔离 fork 的 spike/设计修订，禁止静默放行跨 attempt baseline mutation。
- native 出错后保留 last attempt head 用于诊断/同 Run 恢复，不晋升 committed head。自然完成必须确认 native
  tasks/next/interrupt/pending writes 空、精确 head 未变，再随 terminal durable staging 晋升 committed head 和
  input_user_seq；失败/取消不晋升。typed outcome同连接单事务完成最终usage段幂等写、delivery barrier、
  terminal、固定outbox、Chat terminal事实（session seq/identity）、cleanup及active释放；barrier要求delivery
  Chat事实已持久，不仅queued outbox存在。Chat写后outbox/head失败全部rollback，禁止release后补terminal Chat。
  `postgres_run_leases.py`沿现finalize协调事务，`postgres_run_events.py`提供窄同cursor outbox协作；锁内取得固定index/时间/usage后通过现
  project_chat_fact构造projection，复用PostgresChatRepository已实现唯一SQL的package-internal append_on_cursor
  （承接_append_projection/_next_seq/_save_message），不公开cursor port、不复制SQL、不引入新API。
  terminal Chat取session seq须早于同事务release；outbox durable_seq/index/fence只有per-run语义，不能替代
  session排序。commit后仅Redis发送；重放核原generation/identity并返回相同Chat seq，不分配新seq。
  其他nonterminal critical可沿现outbox/Chat恢复；live Redis可丢，按现Chat/native恢复，不另建ledger。
  保留live reserve index与fenced Chat append两个事务；append锁后核active，terminal先赢拒绝，live先commit则
  HTTP Chat event seq先live后terminal。真实barrier证明old terminal Chat先commit再允许新user admission，
  同event流old terminal.seq < new run.started.seq；message/event是不同kind计数器，不跨计数器比较大小。
  BFF通过HTTP Chat replay消费而非Redis；e7a325ce已验非法post-terminal source显式block与Chat terminal FIFO，
  不代替Agent scope/native或Scheduled同session门。不保证终态后无迟到Redis字节，也不为live强并事务或另建ledger。
- 下一 normal 在 committed head 上补齐 seq 范围内所有尚未 committed 的稳定 user（含先前失败原问题），
  同 ID reducer只一次；retry 仍使用 origin 固定窗口，不包含未来 user。原 Chat row 是输入事实，failed native tail
  的 AI/tool/files/todos 不是可补输入。全 native state 由正确 baseline 还原，外部 workspace/store/artifact 不回滚。

无 FK 完整性由同 scope 事务验证 parent/origin/tenant/seq/locator、读回精确比较及 owner reconciliation 负责。
现 `purge_terminal` 仅 age/cleanup 判定将误删被 lineage/baseline 引用的 Run，必须按上表加入 active/引用保护；
origin/dispatch/user/native祖先及 blob/delta/子图仍被 active、latest retry候选或committed head引用时禁止TTL删。
**Retention 生命周期产品决定未决（非本轮 P1 编码切片）：** latest/committed 指针不会自行过期，只写“无引用才删”并不闭环。Root 已向用户询问
“上下文随 Conversation 保留，DELETE 会话由 BFF 可靠通知 Agent 清理，Run TTL 只清无引用数据”的方案；待明确
owner通知契约、幂等/重放、在途取消/drain、释放引用与privacy删除后才能授权整scope GC，本文不自行新增DELETE API。
暂时保护引用不是永久跳过purge的完成方案，也不承诺现Run TTL足以清全部数据；该未决阻断最终释放/完整生命周期发布，不阻独立P1实施子门。
Run purge必须有限batch无锁选候选，再按scope/dispatch/Run顺序锁后重读age、cleanup及引用；保留active、
latest候选、committed head及其origin/lineage/user依赖。native reachability沿精确root/child parent、channel版本、
blob/delta/pending writes闭包计算，不把“root不再最新”当垃圾。删除须与并发retry/terminal同锁重验，先证明
共享依赖无引用；该bounded purge/reachability是正式源码阶段与验收依赖，不宣称已实现或最终整scope GC闭环。
不新增DELETE API或tombstone字段；生命周期产品决定后再完成释放引用及最终回收。
本片不新增 Redis key；Redis只通知，不作head/授权事实源；严禁逐Run盲adelete_thread破坏同scope DAG。

**待验：** native saver 同连接 pipeline rollback/cancel、失效 generation 写入、子图/DeltaChannel 分支闭包、
normal-vs-retry/双 key 并发、ACK lost、首轮 empty、post-start/HITL恢复装配失败、失败后 normal补 Human、
fresh schema/drift/retention；对应命令和精确允许文件集见 TECHNICAL_DESIGN。旧 3.0 无 baseline 数据不兼容读取；
仅 Root 明确拥有的 fixture 可按协调发布一次 fresh，未授权共享/用户数据不清理。本门不是 SQL 已验收。

## AGENT-FAILURE3-GRANULARITY 数据不变门（2026-09-30）

基线 `da056b0103cced10188cdc1f5baef841d8333889` 的 canonical `database/schema.sql`、
Redis stream/key、outbox、Chat projection 与 proof artifact 均不变。本次目标只重分配 Python 代码
职责，不新增或迁移表、列、索引、JSON 字段、receipt、事务、缓存、retention 或跨 owner 数据。

`run_failed_payload` 移入 execution 内专用模块后仍在写入前产生同一 strict safe payload；
`run_id`、index、durable_seq、event_id、terminal CAS、lease generation、重放及 superseded audit row
不变。旧 retained JSON 继续遵守 3.0 已裁决的无兼容切换，不借内部移动补字段或重写历史数据。
negative-spec metadata 只用于离线验证 immutable proof vectors，不是持久事实；移动其校验函数不会写
proof、JTI、数据库或 Redis。Root 后继真实 22 HTTP 重跑是行为回归证据，不代表本片有 schema 变更。

## Run evidence cursor 数据不变量（2026-09-30，实现候选）

Run 对外 index 从 0 起，durable_seq 与 wire index 不混用：重复终态可保留 superseded、
index_value=NULL 的 outbox 审计行，而 published index=0 仍只有一个可见帧。
Chat source_index 指向同一 Run index，Chat 自有 seq 从 1 起；不改任一持久编号或单终态 fence。

初始 evidence cursor=-1 是只读“尚未见事件”哨兵，不写入事件 index、不新增数据列。
exclusive 查询空页 next_seq 回显输入，故初始空页仍为 -1，随后迟到 index=0 可被同 cursor
读取；after_seq=0 继续只读更大 index。Session/Chat AfterSeq 仍为 0。
本片无 DDL、数据迁移、Redis key/proof/事务/retention 变更；不通过补造 START 或改 index
避开真实首帧。数据库不动；机器只同步查询参数与响应下限，真实持久验收由 Root 使用自有资源重跑。


## AGENT-FAILURE-CONTRACT 数据边界（2026-09-30，候选映射已实现、真实切换未验）

基线 `58b59cf7`；canonical `database/schema.sql` 原样，未迁移或清理任何数据。
候选实现 safe RunFailure={code,retryable} 经过现 Run outbox/receipt/Redis 路径；Chat 投影持久保存
ChatFailure={status:failed,code,retryable}，HTTP replay 原样承接该安全事实，不在查询时猜测 retryable。
机器字段与合法 tuple 唯一来源见 [API_CONTRACT](API_CONTRACT.md)；数据文档不另建 schema。

| 数据/状态边界 | 目标不变量 |
| --- | --- |
| Run critical outbox | 同一 lease/terminal CAS、durable_seq/event_id 与重放幂等不变；失败 code/retryable 在写入前严格验证。 |
| Chat event | 现 payload_json 保存完整安全失败属性；不存原异常 error_kind/message、provider body/secret/URL；不改变 seq、水位或身份范围。 |
| Run 生命周期 | retryable 是失败事实，不是 queued/retry 状态，不自动创建新 Run/重跑；取消、lease takeover、usage/计费边界不变。 |
| Schema/owner | 不增删表、列、索引、事务、Redis key 类型；不访问 BFF/System 数据库，不复制路由或健康事实。 |
| 旧 retained JSON | 旧 Run payload 含被删除字段，旧 Run/Chat payload 缺 retryable；新 strict 模型拒绝旧形状，不默认补 false、不猜旧 assembly_failed 原因、不双读。 |

无 DDL 变化不等于无持久数据切换风险。Agent Redis/outbox/Chat 及 BFF durable 投影均须在 coordinated
cutover 中考虑；本阶段不声明任何历史迁移完成。仅 Root 明确确认归属的当前自有 fixture，可在消费者
全部固定新版后有序停止、清理、一次 fresh 重建；未经授权不改用户或共享数据。存在其他保留事实时，
由对应 owner 先审计并另行裁决切换，不能把隔离新数据验证冒充历史数据转换。

真实验收复用已有 PG/Redis：现 acceptance 创建随机 Agent schema，但 launch 使用固定 REQUESTS_STREAM，
必须由 Root 分配已确认空的测试 logical DB，不与活跃 Agent DB10 并发；仅清自有 schema/keys，不 flush。
在正常 PG/Redis 上验证 terminal outbox→安全 Chat→HTTP replay 持久一致后，仍须 BFF/Web owner 切换与
Root 跨 owner 验收；不存在以本仓 fake provider 结果替代真实 System failure 的放行。

## W3 typed Skill reader 数据当前态（2026-09-29）

本片不修改`database/schema.sql`、Run/dispatch request_json、事务、索引或Redis键；Agent仍仅拥有既有冻结refs与执行事实。
BFF571b51de/Web1dc211bb已消费选择；Root已验证真实Run roundtrip与基础worker/浏览器恢复，详见Root任务表。
Platform6a09913 v4是exact revision、安装/当前授权与包引用owner；Storage16a6c1c是bytes/scan/签名owner。
当前reader每次访问从当前lease发送fresh proof并重新获取Approved/GET，不保存包bytes或授权缓存，不持久asset/URL/header/token/proof。
ResolvedSkill只有本次Run的精确身份元数据；resume/takeover重新创建client，旧lease仍由statement-time reader拒绝。
本片未运行新的真实owner数据库组合；安装产品链/v4激活/退役仍待各owner，见[当前实现](CURRENT.md)。

## 历史：W3 typed Skill source Run fence 代码片（2026-09-29）

Agent 已将必填 `selected_skill_source_refs` 纳入唯一 `RunRequest` canonical JSON；HTTP、Redis 与既有 dispatch/Run `request_json` 机制使用该对象，重复 `run_id` 的选择变化由原 fence 409 拒绝。未增表/列/Redis key。单测验证顺序、空数组及漂移；真实 PostgreSQL/Redis roundtrip 尚未验收，不把代码路径等同数据库证据。Platform 当前授权、包资产、Storage GET/ZIP 等仍未接。下文是设计起点和其余目标，所述“当前无字段”仅指 `7dfcfa9` 基线。

## W3 typed Skill source Run fence：设计起点与剩余目标

当前唯一 canonical [`database/schema.sql`](../database/schema.sql) 的 `kokoro_agent_run_dispatch.request_json TEXT NOT NULL`、`kokoro_agent_run.request_json TEXT` 保存现有 `RunRequest`；该请求只含 `feature_key`/trusted identity/input/模型等，没有 Skill ref。`Agent.skills` 静态字符串没有进入持久 Run，因此部署变更或 lease 重领后无法证明同一 Run 使用同一 exact revision。现有 `request_json`、dispatch fence、Run claim 机制足以承载**有界**新字段；本目标不创建 Skill 表、安装投影、SQL 列、索引、Redis key 或 owner 联合事务。

目标 `selected_skill_source_refs` 是 BFF 显式用户选择的 exact typed `SkillSourceRef.value` 列表，不是 Platform 所拥有的 Skill/Installation row，也不是授权缓存；Agent admission 将原顺序、类型、去重结果与 trusted `ExecutionIdentity` 一起冻结到同一 RunRequest canonical JSON。`POST /v1/runs` 的既有 `sha256` fence/相同 `run_id` 冲突必须包含该字段；两张 Agent Run 表写入/claim/checkpoint/resume/replay 均读取同一冻结 JSON，不能用 Feature 当前配置、请求重试的不同 refs 或另一个 tenant/subject替代。空列表是明确的无外部 Skill，不能解释为“沿旧 music 名称自动加载”。单个 ref 的大小遵守 Platform 7..197 ASCII bytes，目标第一版最多 16 个、整个选择 JSON 最多 4 KiB（实现时由 Agent OpenAPI/HTTP/Redis 测试统一锁定）；缺字段与空数组不建立长期双义兼容，新代码/消费者应同片统一显式传空集合。

Platform `6a09913a96c686b316bfe707b823d039e625607a` 是 Skill exact revision、installation installed+enabled、current owner/tenant/subject 决策与包资产绑定的唯一事实源；其 v4 Proto/ZIP profile 当前 inactive，Agent 仍 pin v3。Storage `16a6c1ce95832df6dc839e0d50e957405c5c7005` 是 blob bytes、当前 scan CLEAN、对象健康与 GET 签名的唯一事实源。Agent 不复制 owner 表、不跨 schema JOIN、不持久 `asset_ref`/短期 `transfer_reference`、URL/headers、proof/JTI/bearer 或 ZIP bytes；只在本次 Run 的受控内存里保存已经校验的 exact package identity/bytes，生命周期不得超出 Run。RunRequest 的 refs 只说明“想用哪一个确切 revision”，不证明已发布、已安装、未撤权或仍 CLEAN。

| 时点 | 本仓持久事实 / 跨 owner 失败恢复 |
| --- | --- |
| Admission | Agent 的 dispatch `request_json` 与 fence 写入一次，重复 `run_id` 的 ref 漂移按原 `run_identity_conflict` 拒绝；不存在已接纳但在第二个表另选 Skill 的窗口。 |
| Claim / lease | 当前 Run 行保存相同 request JSON，worker 从该行和同 generation/owner 的租约创建 run-scoped sender；Platform RPC 前现有 statement-time PG clock reader 校验 lease。 |
| Resolve / Get | 这是只读外部调用，不在 Agent 事务内；每次新 IAM/Platform 当前决策与 fresh proof。RPC ACK 未知只按相同 frozen ref/request_id 有界再读；不能把错误转成“无 Skills”，也不凭旧 Resolve/包缓存越过撤权/感染。 |
| Signed GET / ZIP | GET URL、required headers 仅瞬时使用；超时/签名过期重新向 Platform 获取 fresh reference。上次已完整校验的 bytes 只有在本次重新获当前授权且 asset/digest/manifest 精确相同后才可用于同 Run 读，任何变化清空缓存/失败关闭；不写 Agent journal/Chat/outbox。 |
| Resume / takeover | 取原 Run refs 与新 lease generation，重新做 Resolve/Get/current gate；旧 generation 的内存缓存、proof、签名 URL 不跨 worker/lease 传递。取消/终态后不得发新的包读取；在途 proof 的有限窗口按 IAM/Platform 既有合同处理，非数据库原子撤销。 |

原 `kokoro_agent_run`/dispatch 的 tenant predicate、锁/唯一性、幂等、retention 规则不变；没有理由为 `source_ref` 建 SQL 索引，因为执行只按 `run_id` 获取整份请求。跨 owner 删除/撤权不靠 Agent orphan 扫描替代：每次读取重新请求 Platform，当前拒绝即停止此 Run；Root 真组合要覆盖安装移除、Skill 禁用/撤回、Storage 感染/未知与并发 lease takeover。若后续发现 canonical JSON/fence 实际不能覆盖新字段，代码片必须先改本仓 owner 设计/测试再实施，不能另起一份无 fence 的运行时选择表。

## W3-AGENT-PLATFORM-V3-PIN：数据边界（2026-09-29）

本片仅替换 Agent 的 Platform Proto/v1 execution vendor 为 owner v3 原字节，并更新无状态
Python request-binding projector；`database/schema.sql`、Run/lease/checkpoint/journal/Chat 表、
Redis key、事务与持久化 `RunRequest` 均不变化。Platform 独占 Skill/MCP 与业务 receipt，IAM 独占
token/current permission，Storage 独占 package bytes/scan；Agent 不保存 v3 proof、bearer、
package、授权缓存或跨 owner 表副本。六 RPC 的调用前置仍需后续 typed 选择与当前 lease proof；
本切片只有机器契约和离线投影证据，不作真实授权、撤权或跨 owner 恢复声明。


## W2-F2-S4 作品种类的持久事实与恢复（2026-09-28）

Storage `d5cfc442c675e32363ae767f5ec662a9e0d9eaea` 唯一写 Upload、Asset、Artifact、Scan
与阶段 command receipt；`CreateArtifactResponse.kind` 是作品种类权威回执。Agent 只写本仓 Run/lease、
`kokoro_agent_tool_journal`、`kokoro_agent_run_outbox` 与 Chat 投影，不复制 Storage Artifact 表，
不跨 schema JOIN，也不保存预签 URL、对象 bytes 或服务 secret。BFF 拥有用户对话与未来 Product
可见范围，本仓受信 `session_id` 只是 conversation scope，不是私有性授权证明。

**当前已落地。** Agent `96dafec` 在已有 journal JSON 冻结同一 `(run_id, tool_call_id)` 的交付
意图、原内容 SHA/size/MIME/path/title、受信 scope 和稳定阶段 command。外部写后可按同一
owner command/Upload 状态恢复，包括 Storage FINAL 而 workspace 不可读窗口；成功 JSON 保存
`artifact_id + asset_id + digest`，稳定 event ID 的 `delivery.created` 进入 critical outbox，
Chat `delivery` 投影与 terminal barrier 均在已有 Agent 事务/恢复机制内。取消终态、control ledger、
receipt、fence 和 cleanup intent 在同库事务提交；queued outbox 按序补投影/发布。
Root S3 真纵切证明此资源 ID/顺序/重放链，但没有验证种类字段。当前成功 JSON/outbox/Chat
**没有** `artifact_kind`，这是下一代码门，不得把文档写成已实现。

**目标字段与不变量。** 只扩现有 `DeliveryReceipt`、`DeliverResult` 成功 journal JSON、
`delivery.created` outbox payload 和 Chat `delivery` payload 的必填 `artifact_kind`；值域严格为
`document/code/image/audio/video/data/archive/other`，映射见 [API_CONTRACT](API_CONTRACT.md)。
MIME 可决定冻结的出站 `CreateArtifactRequest.kind`，但最终传播值必须取自与该请求一致的
Storage `CreateArtifactResponse.kind`。`UNSPECIFIED`、未知/缺失或不一致的 owner kind，及
缺 kind 的损坏成功 journal，一律阻断成功 receipt/event/Chat；`other` 只代表 owner 显式 `OTHER=8`。
FINAL→journal 崩溃后用原冻结意图与稳定 CreateArtifact command/owner 回执恢复同一 kind，
并与 Final Artifact ID/asset/digest 一同核对；不依赖 workspace 重读，也不为恢复另造作品。

持久化仍使用现有 journal `result` JSON、outbox payload JSON 和 Chat payload JSON；不新增
SQL 表、列、索引、事务边界、幂等键或第二种 kind 事实源。既有 event ID、durable_seq、
source_index、timestamp 在重放时保持，terminal fence 仍待 critical `delivery.created`/Chat
投影落账后封定。Agent 与 Storage 无跨 owner 原子事务，恢复靠同 command/owner receipt、
本仓 journal/outbox 去重及既有终态屏障。新增字段后的 provenance 与测试门见
[TECHNICAL_DESIGN](TECHNICAL_DESIGN.md)；本次只改文档，不修改 canonical
[`database/schema.sql`](../database/schema.sql)。

## W1E Platform consumer 数据/事务边界（2026-09-27，目标，尚未接线）

本片不改变唯一 `database/schema.sql`：Agent 仍只拥有 Run、lease、journal、chat/evidence 与恢复事实；
Platform 独占 Skill source/installation、MCP connector/connection、授权和业务 receipt，IAM 独占
token/current permission/audit，Storage 独占 package/Artifact bytes。Agent 不复制 Platform 资源表、
安装状态、invocation grant、proof nonce、OAuth access token 或 receipt；不跨 owner JOIN/SQL/事务。

每次 Platform RPC 的 proof 输入从已有持久 Run 的受信 `ExecutionIdentity` 与当前 lease 行派生，
必须沿 A2c 同一 PostgreSQL statement 的 database clock 读取 owner/generation/expiry/paused/terminal；
签发不持久化。`SkillSourceRef`/`McpConnectorId`/`McpConnectionId` 是 Platform typed opaque
reference，不等同于现有 Agent 声明中的名称，也不从 `RuntimeNamespace`、URL、provider key、
`SkillInstallationId` 推导。目标声明来源按 ADR-002 保存/传递 typed ref；本仓只保留执行所需
的精确引用，不造 owner 业务事实副本。BFF→Agent launch 的选择形状如需变更，先由上游 owner
冻结契约；本次文档不修改 Agent HTTP/Redis wire。

Platform request binding 中的 `tenant_ref` 来自受信 Run/token，`request_id` 是同一逻辑重试的
稳定关联值；fresh proof/JTI 不是新幂等身份。Agent 不在本仓新建 Platform command receipt：
当前首批 Skill 读取无 CommandIdentity，`AuthorizeMcpTool` 使用其 exact `idempotency_key`，
completed receipt replay 前的 IAM/current-state 重验由 Platform owner 执行。执行前失败不留下
新 Agent/Platform/Storage/provider 副作用；Run 失败仍走本仓既有终态/恢复机制，不以成功缓存代替授权。

## Canonical authority

唯一 DDL 是 [`../database/schema.sql`](../database/schema.sql)，仅支持空数据库 fresh install；不保留
migration chain、外键或 `REFERENCES`。所有表名/列名小写 snake_case，时间事实使用 `TIMESTAMPTZ(3)`。

## 表按事实分组

| 事实组                  | 表                                                                                                                |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Run admission/lifecycle | `kokoro_agent_run`、`kokoro_agent_run_dispatch`、`kokoro_agent_run_dlq`                                           |
| Event durability        | `kokoro_agent_run_outbox`、`kokoro_agent_run_receipt`、`kokoro_agent_run_receipt_manifest`                        |
| Control/usage           | `kokoro_agent_run_control_command`、`kokoro_agent_run_steer`、`kokoro_agent_run_usage_segment`                    |
| Sandbox/tool            | `kokoro_agent_sandbox_cleanup_intent`、`kokoro_agent_tool_result`、`kokoro_agent_tool_journal`                    |
| Chat projection         | `kokoro_agent_chat_session`、`kokoro_agent_chat_event`、`kokoro_agent_chat_message`、`kokoro_agent_chat_sequence` |
| Long-term memory        | `kokoro_agent_memory`                                                                                             |
| Native checkpoint       | `checkpoints`、`checkpoint_blobs`、`checkpoint_writes`                                                            |

Run/dispatch 是 tenant-scoped；namespace 是 Agent 从 trusted identity 派生的隔离键。Run ingress scoped
lookup 必须同时 predicate `tenant_id` 与派生 namespace，且 tenant-owned JOIN 必须把 tenant 条件写在
`ON` 与 `WHERE` 中。没有独立 `tenant_id` 列的 chat scope 只能接受由同一 trusted identity 派生的 namespace，
不得把 caller-provided namespace 当作权限。子表通过受保护的 application/repository 查询和固定事务路径关联，
不依赖数据库外键。跨 owner 的 tenant/resource ref 只保存 opaque reference，不做数据库 JOIN。

`createRun` 202 与 `replaySessionEvents` 200 的 typed HTTP envelope 只是既有 dispatch
receipt、chat event 查询结果的传输约束；本切片不增删表、列、索引或事务，也不把 BFF 的
Conversation/Message 投影写入 Agent schema。

模型最后一个 segment 的 `message.completed.content=""` 会进入现有 `chat_event` 的
`assistant.completed` 安全投影，并以同一 segment 派生的稳定 message ID 保存空内容
`chat_message`。空字符串是有效内容，不等于缺失事件；重放顺序仍由 `seq` 与 `source_index`
约束。本切片不改变 canonical DDL、事务边界或 retention。

## 字段与约束原则

- `id`/业务 identity 只在真正幂等或唯一不变量时建主键/唯一索引。
- 状态字段使用有限集合 `CHECK`；不使用互相矛盾的 boolean 代替状态机。
- append-only event/receipt/usage 不机械添加 `updated_at`。
- JSON/JSONB 只承载结构化 payload；可查询的 tenant、状态、排序字段使用普通列。
- 每个索引必须对应 tenant filter、due scan、稳定排序、唯一性或并发路径。
- 软删除、version、audit actor 只在有真实语义时增加。

## 时间与金额

数据库默认值为 `CURRENT_TIMESTAMP(3)`；应用内部可在 adapter 使用 epoch milliseconds 参与兼容的
checkpoint API，但不得把 Unix 秒写入数据库。金额如未来进入 Agent 只用最小货币单位整数和显式 currency。

## System 路由消费切片

2026-09-08 G6-Agent 不改变 canonical schema、Run输入或持久化写边界。模型目录、版本、provider、租户路由表
只属于System；Agent不复制、不JOIN。每次模型构造的解析revision/digest/generation记结构化日志；这不是持久化模型快照。
canonical schema 保持不变；目标 failure JSON 与 3.0.0 切换见本文顶部及 API_CONTRACT，
不能沿用旧阶段的 Run wire 不变结论；System 依赖 pin 仍见 API_CONTRACT。

## Execution proof 数据边界（2026-09-12，statement-time reader 已落地、无 schema 变更）

execution proof 是由 canonical Run/ExecutionIdentity 与当前 lease 派生的短期 signed artifact，不是新的 PostgreSQL/Redis durable fact。
本设计不修改 `database/schema.sql`，不新增 proof、nonce、key、delegation、receipt 或 outbox 表，也不新增 Redis key。私钥/public ring
是部署 key material，不写数据库；IAM 的 key cache/decision audit 与 Platform receipt 仍由各自 owner 保存。

签发输入只来自已有 canonical facts：

| proof 值                            | Agent 当前事实                                                                                                                    |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `tenant_ref`、typed `actor/subject` | persisted `RunRequest.execution_identity`；不接受调用时 body/header 重报                                                          |
| `run_id`                            | canonical `RunRequest.run_id`                                                                                                     |
| `execution_session_id`              | canonical `RunRequest.session_id`                                                                                                 |
| `lease_generation` 与 owner         | `kokoro_agent_run` 当前 claim；generation 必须是 `1..9007199254740991` JSON safe integer且owner精确匹配supplier捕获的`LeaseFence` |
| lease validity                      | 同一 PostgreSQL statement 的 `clock_timestamp()` 与 `lease_expires_at`，并要求 `terminal=false`、expiry 非空且未到期              |
| `operation`/binding                 | Platform owner contract 在 Agent client boundary 已校验的值；不写回 Run 表                                                        |

现有 `is_lease_current` 在取得数据库连接前读取应用 clock，不能为 proof freshness 提供证据。A2c proof 专用 statement-time reader 已落地：
它使用同 statement database instant 返回 checked time/expiry，只读现有 canonical Run 行，不写入 schema 或业务事务，不改变通用 lease API。proof `exp` 取 `min(iat+60s, floor(lease_expires_at))`，没有至少 1 秒正有效期时不签；IAM 5 秒 verifier skew 可能使
接受延到 `exp+5s`。query 与签名间仍存在 generation race，文档/API 明确最长在途窗口而不冒充实时撤销。

数据库 `lease_generation` 当前是整数事实，但跨 JSON/JWS 边界必须在 canonicalization 前显式检查 `1..9007199254740991`；不得因
Python integer 无精度上限而签出超出消费者安全范围的值。`iat`/`exp` 同样限制为 `0..9007199254740991` JSON integer，并继续检查
`exp>iat`、TTL、clock 与 lease expiry。边界层拒绝 `2^53`、`2^53+1`、负数、bool 和 float，不截断、round 或 stringify；
`lease_generation=0` 拒绝，时间字段 0 只通过 type/range 层，随后仍由正向/current-time 语义拒绝或接受。canonical 向量必须包含
`2^53-1` 的有效边界和上述所有拒绝样本；future schema 的 `x-kokoro-require-integer-token=true` 由 contract-check 在原始 JSON 上执行，
不让数学 integer 判定放过 `1.0`。

Skill 的 `series_id/skill_id/installation_id` 与 MCP 的
`connector_id/server_id/connection_id/authorization_id/invocation_grant` 继续只属于 Platform。Agent proof 不复制这些字段、不建立
跨 owner JOIN；Platform 只在自己的 canonical request-binding digest 中覆盖它们。`identity_assertion_ref` 继续是 Run ingress 的受信关联，
不是 execution proof claim，也不复制进 IAM audit payload。

## W1E authenticated transport（2026-09-28）

本片不改 canonical schema、不新增数据库事实。worker sender 只读既有 Run/lease statement-time observation，
所有 bearer、credential snapshot、proof/JTI 为进程/调用生命周期对象，不落库、不写 Redis。验证用独立随机数据库
安装同一 canonical schema，经真实 repository claim/pause 检验 send/拒绝，并清理自有库；它不替代真实 IAM/Platform
数据库当前授权矩阵。typed Skill/MCP 选择冻结尚未进入 Run 持久 fence，库存仍 broken。
