## R122 数据当前态（2026-10-02）

当前为Agent main `2653bcc723da5366fd877db73d701410e7bdc3a8` 之上的完整HTTP5/Todo、安全过程、安装资源与生命周期候选，待Root提交/发布；下方R104提案与R117 RED均为历史阶段，不覆盖本节。事实owner/模块、唯一canonical SQL与机器契约不变；distribution `2.0.0`、HTTP `5.0.0`、execution proof `1.0.0` 是不同身份。当前OpenAPI SHA256 `bca8e4f4fd613e4325f594266893d5b089168cf14f2ad7a7df03f3f116af85f2`；跨仓消费者必须随后固定正式owner commit/digest，不靠未发布源码或兼容协议。

Root E95实际1960纯节点全部通过/零skip、七静态门通过；E96当前源码新wheel/sdist与完整runtime安装、四布局64负向/219步骤通过；E97同包installed CLI真实PG首次安装/重复拒绝、canonical catalog与六漂移回滚通过；E98同包仓外安装态现HTTP acceptance实际36通过/0失败/0跳过（36 setup/call/teardown），151 loaded模块在collection/finish对应本次site-packages/RECORD。当前wheel SHA256 `4d0e7c8d1455fa395faaebc5e0de123f7131a31d02c413f04266ec67bad78fc3`；Root运行/manifest详见Root `docs/progress.md` E95–98。自有库精确回收/Redis测试区空/owned进程自然终态、371冻结路径保持；七固定测试工具按当前lock URL/hash单独供应，资源测试不出localhost。

这些是限定源码/安装/资源门，不是完整Agent或用户链闭环：真实console启动/SIGTERM、S3/Docker/E2B/custom/provider、完整retention、全部数据owner同库组合，以及正式BFF/Web过程消费与浏览器仍未验；T-Q03/T-R02继续开放。此四文档前缀只修正当前态，不改源码、SQL、协议、生成物、依赖或锁；发布前Root须重建当前最终文档集合的wheel/sdist并核与已测包的全部entry/RECORD一致性，差异则重新验证。

---

## R118：assembly registry 与未转交 handle 不新增持久化事实（2026-10-02）

**当前裁决：R119 GREEN 候选无 SQL/Schema/事务/Redis/checkpoint 变更，待 Root 最终复验。** Supervisor 的 assembly registry、late-handle cleanup、resume transferred 标志与 recovery probe finally 都是进程内生命周期状态。

- assembly task、run task、resource cleanup task 由 Supervisor 强持有并在同一 `drain_timeout_s` 绝对 deadline 下动态等待；这些 task、close 计数和 client 状态不写入 Run、interaction、outbox、Chat 或公开事件。
- resume replay/observed/非 Started 与 reader 异常/取消不改变既有 interaction 状态机；补 close 只收束未转交本地资源。成功 spawn 后仍由 run task 唯一关闭 handle，禁止第二 ownership 记录或双关。
- recovery probe 继续读取既有 unknown attempt、local-drained token 与 reconcile probe 事实；probe handle 永不成为 durable sandbox identity，读/记账任一出口都必须本地关闭。
- Docker 后半构造失败时，是否销毁 container 只按 `container_id != prior_sandbox_id` 判定。本次自建 S3 client 必须关闭；既有 sandbox id、cleanup intent、lease generation、backend kind 与 teardown ref 不被伪造、删除或提前完成。
- shutdown timeout 保留未完成本地 task 的强引用并使 worker 非零；不通过取消 thread、清表、改 terminal 或写一条“已清理”记录伪造成功。

因此 `database/schema.sql`、installer、表/列/索引/约束、canonical schema、SQL tests、wire、pins、provenance、generated 与 lock 全部保持未编辑。当前纯 GREEN 只验证进程内 owner/顺序，不能替代真实 S3、Docker、SIGTERM 与 durable sandbox recovery 门。

---

## R117：本地 client 与 durable sandbox cleanup 的数据边界（2026-10-02）

**当前裁决：无持久化模型变更；生产实现尚未授权。** 基线 Agent `2653bcc723da5366fd877db73d701410e7bdc3a8`；依据 Root R117 生命周期实施卡、同轮 TECHNICAL_DESIGN/API 与 [Python 后端工程规范](../../../docs/kokoro-handbook/standards/09-python-backend-engineering.md)。

- `S3Archiver` 自建 boto client 及其 `open/closing/closed`、in-flight operation 和 Supervisor cleanup task 都是单进程内资源状态，不写入 PostgreSQL、Redis、checkpoint、Run JSON 或公开事件。
- 现 sandbox id、cleanup intent、lease generation、backend kind 与 teardown ref 继续是 Docker/E2B/custom 等 durable 外部 sandbox 恢复事实；它们不证明本地 boto client 已 close。本地 close 也不得完成、删除或伪造 durable cleanup intent。
- Run terminal transaction、lease/fence、outbox、Chat、Delivery journal、tenant、幂等、UTC、purge 与 retention 语义全部保持。正常终态之后等待本地 close 是执行生命周期次序，不把数据库 terminal 回滚为 active，也不产生第二 terminal。
- connector/Factory 在 backend 或 handle 尚未成功返回时拥有已创建资源；partial peer、native constructor failure、CAS loser 或取消必须在当前构建 ownership 下真实收束，不能把无登记 task 留给 shutdown 猜测。成功返回后 ownership 才转给 Supervisor cleanup registry。
- shutdown 使用现 `drain_timeout_s` 同一 deadline 等待 run 与随后产生的 cleanup。timeout 返回 false 且 worker 非零；未完成 task 继续受强引用，不通过取消 thread、清表或 durable 状态改写伪造成功。

因此 `database/schema.sql`、installer、事务、表/列/索引/约束、production role、canonical schema 与 SQL 测试全部冻结；wire、pins、provenance 与 generated 当前也预期不变。runtime descriptor 摘要由现 codec 对源码字节自然计算，新 wheel RECORD 由 build 派生；出现任何实际机械生成文件差异时须停下交 Root 另行授权，不手填。后继 GREEN 的 unit double 只证明进程内 owner/顺序；真实 S3 与 durable sandbox 恢复仍是分开的资源验收门，不能互相替代。

---

## R104：安装 DDL 资产与数据验收边界 D0（2026-10-02）

**方案候选，待 Root 裁决与独立审查；本轮无DDL/代码/资源操作。** Agent基线 `17c73541ae5d9f123d85cf531a79503df5c463bd` 加冻结候选，Root `264de6b8`。安装checker真实RED与资源布局比较见同轮TECH/API；本节只界定安装资产与既有数据owner，不改变下方业务表、事务、lease、幂等、UTC或tenant模型。

### 唯一canonical与安装副本

- 唯一可编辑DDL仍是 `database/schema.sql`；无新表、列、索引、迁移链、数据库role或通用配置。pyproject现将其分发到 `share/kokoro-agent/schema.sql`；Root只读报告已确认当前wheel的这一资产与canonical字节相同，但尚未证明安装后fresh apply与catalog门。
- 建议方案B保持此单份安装DDL位置，不再向audit树复制一份DDL。contract audit树不是数据库schema容器，contract/provenance仍不承担DDL owner。wheel/sdist仅派生同一canonical文件；不手改share、副本不反向写回仓库。
- 当前 `canonical_schema_path()` 依次试源码相对路径与sys.prefix/share，不能据此宣称任意`--target`可定位。后继调整此函数使用同一 `distribution_assets.py` 的确定布局绑定，返回当前distribution记录的准确DDL；source模式来自明确源码归属，installed模式缺失即失败，不从checkout回退。SQL正文、原installer事务及verify语义不变；operator与installer的资源预检顺序另按下一项收口。
- 新helper只处理本发行资产的路径/身份，不导入数据库连接、worker或业务repo；schema → helper 的依赖不把contract/Platform checker带进数据库启动路径。R105审查确认当前 `src/kokoro_agent/application/schema.py` 在预检前连接，`src/kokoro_agent/infrastructure/schema.py` 在读DDL前调用 `ensure_schema`，原笼统承诺尚未实现。后继须同时纳入这两个现文件：`apply_database_schema` 先经同一canonical loader纯定位、读取并校验文件清单/来源/bytes，再调用 `connect_pg`；现 `apply_agent_schema` 自身在 `ensure_schema` 前取得已验SQL，保存为本次执行值，后续事务内不重读文件。不新installer/API。入口初始缺失/漂移为零连接、零schema创建；直接installer的连接由调用者提供，其预检失败只承诺零ensure_schema/DDL，不能据此推断零已有连接。

### 生命周期与失败恢复

| 边界 | 保持的规则 / 后继证明 |
|---|---|
| 安装文件生命周期 | 归属当前wheel及其安装/卸载；检查只读，不在启动时生成、下载、缓存修复SQL。缺资源、跨安装定位、非预期路径或digest不一致在资源preflight失败。 |
| 持久事实owner | Run/Chat等仍只写Agent schema；无跨owner SQL/JOIN、无共享ORM、无新production角色。Redis仍只是live/协调，不成为安装或durable数据权威。 |
| Fresh apply | 已安装CLI沿现apply_agent_schema(require_blank=True)安装Root明确分配的临时库/唯一schema；重复安装拒绝、catalog drift拒绝，不自动drop/重置他人数据。 |
| 事务/故障 | 预检提前不改变现ensure_schema与transaction的边界；事务内require_blank、SET LOCAL与DDL执行顺序保持，只执行此前捕获的已验SQL，catalog校验不变。失败保实际日志/退出码，Root外层精确回收自有fixture。源树成功不替代安装路径实际连接与验证。 |
| HTTP测试资源 | 仍用R102独占声明/Redis空与requests不存在预检、事前登记exact keys、handler drain及线程终止后清理；unknown残留失败保留，不SCAN认领、不FLUSH。 |

### 安装后 DDL 与 HTTP 门（本轮均未运行）

1. Root先绑定新wheel SHA、实际已安装module/DDL来源与canonical输入digest；在仓外cwd、Python `-I`、无editable/PYTHONPATH源码回退环境验证正常venv与`--target`布局。仅测试harness可提供输入，不能手动复制checkout SQL到安装目录补洞。
2. 先在现 `tests/unit/test_cli.py`（已定位的 `test_db_apply_schema_uses_the_configured_empty_namespace` 目前只测CLI委派，后继扩现文件测试真实application入口）与 `tests/contract/test_canonical_database_schema.py` 固化缺失/漂移负例：operator入口connect调用0、schema创建0；直接installer ensure_schema及DDL调用0。合法输入control保原事务顺序；若入口预检后资产再变化，installer二次预检也须在ensure_schema前失败，此时不声称connect调用0。无设施资源门通过后，Root以显式 `KOKORO_AGENT_DATABASE_URL`、schema和独占资源启动已装 `kokoro-agent-db-apply-schema`（或同一安装的 `kokoro-agent db:apply-schema`）；记录fresh成功、require_blank重复拒绝、目录/列/类型/默认值/约束/索引catalog验证，以及库/schema准确清理证据。相邻规范断言仍在现 `tests/contract/test_canonical_database_schema.py`，不修改canonical SQL迎合测试。
3. 同一wheel的HTTP server/RunEmitter沿正式enqueue/claim→有序Unicode Todo→[]→fresh PG→session HTTP limit1/replay。对照PG原identity/seq/payload UTF-8 bytes/created_at毫秒与next_seq/watermark，跨tenant/subject只返回空集合，原scope与水位不变。已装模块来源检查必须先于PG/Redis/HTTP写入；原源码HTTP36不挪称本门通过。
4. worker不自行build/install/启动服务；Root先授权精确安装harness文件及资源，再运行。平台/锁/依赖版本不顺手升级，安装失败不fallback源码；记录原r104b RED及新产物不同SHA，不覆盖旧证据。

### retention 的已证范围与未完成边界

Root PG65与HTTP36已包含：terminal cleanup intent未完成阻止purge、完成后允许；interaction terminal吸收、purge后late command不复活、两种锁序无孤儿；Delivery ACK GC在active保mapping、terminal后按watermark回收。保留这些证据，不重写为“retention全未测”。
但现 `postgres_run_context.purge_terminal` 无LIMIT/bounded batch，也未校验新增scope/head/baseline/native checkpoint可达引用；Chat无统一retention入口。Run purge、outbox GC、Chat/native完整引用生命周期不是同一结论。完整bounded/reference-aware retention、active引用保留、age边界与跨Chat/native删除规则仍需后继已批准政策/独立真实门；未决保留天数不编造，不借安装修复增加DELETE或清用户数据。修复安装资源可独立推进，完整Agent发布/正式数据处置与BFF-Web整链仍不得宣称完成。

---

## R90-W03：安全过程 durable Chat 数据 D0 闭集（2026-10-02）

Agent main基线17c73541ae5d9f123d85cf531a79503df5c463bd；替换未提交R87前缀，原HEAD全文保留。当前HTTP4、唯一canonical database/schema.sql、SQL/installer/source/生成物均不改；HTTP5是后继目标，不新表/列/数据库/FK/进程/依赖。
Owner：Agent唯一写执行/安全Chat；BFF唯一写durable AG-UI与同水位compact snapshot；Web不写持久业务事实。跨owner只用发布契约，不跨库查询。

### 已核事实与既有位置

| 现事实 | 当前约束 / 目标承接 |
|---|---|
| kokoro_agent_chat_event | PK=(tenant_id,namespace,run_id,source_index)；tenant+chat_event_id及tenant/namespace/session/seq唯一；event_type/payload_json为TEXT，无event_type闭集SQL CHECK。现表容纳safe activity/Todo，不因枚举新增DDL。 |
| Chat seq/ID | 原session计数器/namespace-run-source_index ID不改；同identity比较全部immutable字段，含时间/payload。公开activity/preflight digest仅归组，不替代授权/PK/seq。 |
| 非critical发射 | RunEmitter先reserve index再append_fenced再live；Run active锁重验原owner/generation/expiry/非terminal；reserve与Chat append分离，可留下index间隙，不能声称原子提交。 |
| run_outbox | stage_critical_frame已有Run锁及queued/published/superseded事实；run.started尚无按kind幂等查询，invoke仍以index==0判定。目标在同方法/锁内补查询，沿既有签名与StagedFrame。 |
| Chat message/terminal | Todo/activities仅Chat event，不假造assistant Message；原完整HITL、成功Delivery、唯一terminal writer与事务权限不变。 |

采用扩现infrastructure/postgres_run_events.py和原RunEmitter，不创建progress/todo表或Factory直写/Redis真源；domain/run/repositories.py与postgres_run_repository.py现port/转发无需为查询新增方法。postgres_chat_repository.py的append_fenced/immutable检查维持；postgres_run_context.py是purge事实基线。
现schema结构容纳目标不等于运行正确性已验；后继若发现确需新DDL/索引，先Root另准三面与精确路径，本D0不授权新建兼容表。

### started唯一锁内判定

1. 只对kind='run.started'：进入既有stage_critical_frame事务，先lock_active_lease持有该Run锁并验证原lease；失败返None，零查回冒充成功/零分配。terminal仍只能走原finalize_terminal。
2. 同锁内、分配durable_counter/event_index_counter之前，参数化查询原RUN_OUTBOX_TABLE：WHERE run_id=%s AND kind='run.started' AND status IN ('queued','published') ORDER BY durable_seq ASC LIMIT 2；精确同Run有界查询，不锁外先SELECT再emit，也不拉全部outbox到内存过滤。
3. 0行才走现计数分配/insert；1行校验index_value非null与固定run.started payload一致，返回原event_id、durable_seq、index_value、occurred_at→timestamp、published=(status='published')，newly_staged=false。已有StagedFrame不含payload字段，持久行payload/时间保持原值，调用方不可重写；2行视为不变量破坏，失败封闭，不任取一个掩盖重复。
4. queued重拾沿现outbox补Chat/发布，published不新分配或重发；重复调用输入的新timestamp/event_id不替换已持久值。same source投影必须用返回的原index/timestamp；原payload一致才复用。durable_seq不要求=1，preflight已占index也不影响started资格。
5. build成功后的initial invoke调用此入口，包括initial crash重拾；resume模式不分配新started，恢复probe不自行invoke；模式来自supervisor现resume上下文，不以next_index==0/内存bool/随机ID推断。build失败零started。
6. 并发由现Run行锁串行化，非新锁/新表；真实两连接竞争、提交ACK丢失、reclaim/new generation重拾和terminal后拒写由Root PG门证明，内存fake/unit不冒称此证明。

### progress轮次与故障窗口

1. ordinary progress走同原lease RunEmitter按Run串行：首resolving reserve source_index→按API的C/H派生spf_ID→append_fenced确认→才允许Skill resolve；所有Platform/Storage I/O在DB事务外，禁止持Run锁等待网络。
2. 后续phase复用首durable resolving anchor，工具/subagent用原真实调用/segment身份；API已锁完整身份元组、域分隔SHA256、64小写hex及68字符前缀ID。identity不来自模型文本，source_refs只来自canonical request。
3. reserve后append前crash：没有durable阶段，不做Skill I/O、不补伪ready；恢复重新授权/包读必须新resolving anchor。append ACK不确定也不继续I/O；重读原提交事件是replay，重新真实调用是新轮次，不能reuse旧ready。
4. 同已提交Chat事实replay保source_index/created_at/payload；漂移冲突不覆盖。单纯再emit取得新index不称旧事实重放。append成功/live失败只由durable replay补展示，不重做provider/tool、不补第二terminal。
5. 回调需显式durable确认；现静默None不算确认。持久化/fence错误独立传播，停止后续I/O，不落入Skill业务异常/assembly_failed/invoke终态转换；initial/control/recovery全部catch同步覆盖。Skill网络失败才用API闭集failed code。
6. 旧generation迟回调零新增progress，不借新lease；terminal后普通进度零写。HITL/Delivery原具名恢复权限不扩大/删除；private诊断继续保留在私有边界。
7. Todo以最新已提交主Run完整表替换，不混tenant/Run/子agent；0..100项、content 1..1024码点及整个{"todos":[...]}的C编码≤65536 bytes，孤立surrogate/缺todos/超界整表拒绝。API唯一编码口径在持久化前执行，不二次转义算budget、不截断。

### retention与clean-slate发布门

现Run purge清理Run/outbox/receipt/tool等但不清Chat event/message；现outbox直到Run purge才删除，Run生命周期内started查回沿此事实，不能据此宣称Chat GC已完成。Redis TTL不等于durable Chat retention。
Agent5仅在明确fresh批准测试边界或正式已批准数据处置cutover启用；本轮不迁移/删除既有用户数据，不双读旧raw、不把旧raw payload重标safe。保留缺口是正式发布/整链门，不阻挡不接资源的纯RED。
Chat/BFF后继保留策略须对齐durable消费checkpoint、快照与cursor窗口；active/waiting/恢复所需事实及未被下游持久承接事实不得提前删。具体天数未获批准，不编造；现Run purge后Chat残留作为已知风险保留。
BFF本仓一致快照中的Todo/compact activities/消息/HITL/作品来自同event_watermark；快照之后只replay更晚事件，过期/历史缺失显式重取受信snapshot，不推断空表/成功。Agent不写BFF快照，后继BFF owner裁其保留实现。
仅安全白名单字段进新Chat过程payload；raw name/args/result/error/路径/stack及private thinking/output不公开。HITL/成功Delivery原授权事实不受普通activity禁字段规则误删；不宣称任意自然语言通用脱敏。

### 后继验证与范围

Root精确PG路径：tests/integration/database/test_run_outbox_filter.py、test_run_interaction_transactions.py、test_delivery_outbox.py；覆盖两连接started、丢ACK/reclaim、fence、回滚/重复漂移、tenant/session排序/终态拒写。新增Chat PG测试另准路径；不reset他人数据、不自行起服务。
先现tests/unit/chat/test_projection.py、test_emitter.py及factory/invoke/supervisor行为RED；tests/support/fakes.py模拟新started语义但不代替PG。tests/contract/test_canonical_database_schema.py保持canonical无漂移；tenant/time与machine/HTTP门及准确caller/生成路径见TECH。
后继命令uv run pytest tests/unit/chat tests/contract、uv run kokoro-agent-contract-check及Ruff/Pyright/build；PG/HTTP组合仅Root隔离资源。当前没有DDL，不执行schema安装、不声称fresh install或恢复已通过。
R80 attempt/evidence/usage outbox仍是独立后继数据目标；不复用本片identity/计数当付款事实、不以Todo/tool/Skill事件计费。同Agent writer先冻结本过程，再续usage/v1，usage SQL另审，不占HTTP5或阻塞本片。
Root闭集已落定，剩余为代码/机器/真实PG证明、正式历史数据处置、Chat-BFF retention与消费者发布；本轮只文档保护检查，不把D0或旧绿色作为本轮业务验证。

---

## R80-W03：逐模型 attempt 事实、usage证据与结算恢复 D0（2026-10-02）

跨仓依据：[ADR-033：逐实际调用用量与 Billing 单一定价 owner](../../../docs/kokoro-handbook/decisions/ADR-033-actual-usage-and-pricing-ownership.md)。按 Root 已接受 owner 裁决同步；ADR独立审查不作为本仓实现验收。

基线 main `444684d32473c96ddbb70247081b1d1cdb8558f1`。本轮不改 `database/schema.sql`；以下是后继SQL设计要求，当前schema无这些新事实。Agent是唯一writer，Billing独占价格/金额/hold/ledger，System独占planned技术binding；全程无跨owner SQL、FK或数据库事务。

### 1. 当前事实与目标关系

当前Run保存request_json、lease、token_total、usage_input_total/output_total；`kokoro_agent_run_usage_segment`以(run_id,lease_generation)封存两项计数。它能避免同generation重复累加，却不表达一段内多个实际attempt、classification、unknown、actual provider或Billing admission。RunRequest当前只有可信execution_identity及Agent durable dispatch admission；不将逐attempt Billing admission提前设为launch必填，也不新增Run级预占。launch付款/消费授权上下文须IAM/Billing具名正式契约决定，新增引用是否导致breaking尚待该决定，身份本身不代替付款授权。

目标唯一计量真源是逐attempt的不可变证据序列；Run/segment totals仅由该真源投影，不同时保留callback聚合和attempt累加两套writer。旧schema/code/data不做迁移导入或fallback，正式clean-slate切片一次替换调用、查询、测试及机器语义。token_total保留执行预算语义时也来自同一已知计量投影，不成为定价真源；unknown另有显式状态。

统一发布顺序：共同冻结语义 → Agent strict evidence producer artifact先发布 → Billing固定消费该artifact并发布逐attempt admission/证据接收contract → Agent固定消费Billing → 必要BFF消费者切换。纯artifact不依赖运行服务已经启动；语义协作不等于循环等待对方先发布。

### 2. 目标持久事实（具体DDL在授权SQL切片落地）

| Agent-owned候选表 | 身份、数据与约束 | 生命周期/查询 |
|---|---|---|
| `kokoro_agent_model_call_attempt` | tenant/run/logical_call/provider_attempt opaque TEXT身份；attempt ordinal与原lease generation为正BIGINT；唯一(tenant_id,run_id,logical_call_id,attempt_ordinal)，attempt_id唯一。可信subject/actor、Agent admission/fence和受信付款上下文绑定、planned binding JSONB和digest、请求摘要；prepared尚无逐attempt Billing admission/authorization，获授后绑定本attempt且保持不可变，dispatch_started必须有合法许可，预占拒绝不得伪补引用；数量上限、执行状态、UTC TIMESTAMPTZ(3)、revision/CAS。JSON仅闭集校验，不存secret/全prompt；原fence不可变 | prepared→authorized→dispatch_started→observed/unknown，not_dispatched仅有零外发证据时；按run有序恢复及按状态/下次reconcile时间有界扫描。old started不因takeover重新授权外发 |
| `kokoro_agent_model_usage_evidence` | 稳定evidence/source identity；(attempt_id,evidence_revision)唯一，前revision引用与canonical digest；原attempt绑定、actual attribution与来源digest、usage三态、严格分类数量/单位/完整性、outcome、UTC observed_at。append-only；非负BIGINT计数，缺失未知为NULL/显式状态，不用0占位；跨字段CHECK与应用profile双门 | 同identity同digest只读幂等，不同digest冲突。unknown可由新可信evidence revision收敛，原记录不覆盖；已知矛盾需冲突/更正流程，不latest-wins |
| `kokoro_agent_usage_outbox` | 唯一source event identity，引用精确evidence identity/revision/digest；投递状态、有限attempt_count、next_attempt_at、delivery lease、Billing receipt opaque ref及UTC时间。不复制第二份可编辑usage，不借Run公开事件durable_seq | queued→delivering→acknowledged，ACK丢失按原identity重投；失败退避有界，per-attempt revision有序，Billing须拒漂移并幂等接收；终态后仍可独立投递/查询恢复 |

三张表分别承载可变执行journal、不可变观测历史、可恢复投递生命周期，非机械DTO复制。索引仅为上述唯一性、run恢复、pending有界扫描与per-attempt revision读取；准确字段类型/长度/NULL组合、索引predicate和catalog drift矩阵随正式schema共同评审。所有读写带tenant范围，同库owner schema，不添加外键、跨owner表引用、价格/倍率表或任意dimensions垃圾桶。

Run增加usage completeness/unknown attempt计数及有界汇总投影的实际表示，由同SQL切片锁定；已知部分总和不是最终总成本，也不把unknown显示成0。取消缺席段不补零。canonical schema仍唯一，installer/validator/测试必须同步，不靠Markdown证明fresh install/drift通过。

### 3. 原事务与锁序

1. 准备/dispatch CAS分阶段：沿现Run锁→attempt→evidence/outbox固定顺序，锁后取database clock。先核原owner/generation/expiry、Run及受信付款上下文，写prepared（无未来attempt admission）并提交；事务外申请该attempt Billing预占；再以原fence核精确call/attempt/主体/actualbinding/预算/期限，首次绑定admission进入authorized；外发前重验并CAS dispatch_started。每次实际attempt独立许可，不复用一次许可授权整Run。任何阶段都不持Run锁await provider/Billing。
2. 观察：原fence有效时，单事务严格校验attempt、写不可变evidence、更新attempt状态、已知用量投影及outbox intent；重复callback不重复累加。网络完成但数据库提交失败保留unknown恢复，不用第二次provider调用“补证据”。
3. 最终收口：现 `postgres_run_leases.finalize_terminal` 仍唯一协调器，同连接将最终attempt证据/remaining unknown分类、usage completeness和投递intent，与原terminal/Chat/outbox/control/cleanup一起提交或一起回滚。已独立提交的attempt evidence按identity引用，终态不再累加一次。
4. pause/drain：所有本地native任务先drain，已观察证据seal后才释放原lease；未证明结果的started显式unknown，保留持久journal。模型/工具/HITL attempt命名空间分离，禁止复用resume attempt身份或清tool journal连带清模型证据。
5. cancel/takeover与晚响应：旧worker失fence即停止改变Run、用量投影与授权；不借新lease补写旧闭包。Run已终态仍可能存在在途成本，终态事务保留started/unknown与后续reconcile意图，不自动释放全部Billing hold。
6. 正式reconciler是现worker的恢复职责，不是新executor。以独立、期限有限的reconciliation claim、原attempt身份及owner可验证结果读取恢复；事务仍先Run→attempt→evidence/outbox，可在终态后追加证据revision/投递，但不重开Run、不改原终态事件及其当时usage汇总快照、不授予provider执行权；迟到完整计量通过独立usage revision查询，不把终态旧快照冒充当前结算材料。原worker任意晚callback不等于此权威查询。
7. receipt/ACK丢失：预占请求保持原attempt/request identity查询，不生成新Run级预占；原event重投或查Billing原identity；Billing只返回受信状态，不由Agent计算capture/release金额。usage落库/结算ACK/Run终态是不同事实，分别恢复，禁止“先终态后无intent”窗口。

PG commit与provider实际发送之间无共同事务；dispatch_started崩溃窗口保守unknown，provider不支持查询/幂等恢复时进入待核对队列。失败收费决策未答不阻落真实证据，但不据此自动收费或免费结案。已观测的错误/超额实际消耗完整保留，停止新增调用，Billing裁定财务处理。

### 4. 保留、GC与隐私

不持久provider凭据、Billing bearer/proof、原始prompt/response、OAuth数据；输入摘要和来源引用仍按私有执行证据授权访问，日志只白名单状态/引用，避免散列当匿名化保证。

Run purge须感知未决attempt/证据投递/reconciliation引用；只有已取得Billing持久接收状态且恢复责任已交接、无在途/unknown与待决收费引用，并满足owner批准retention后才有界GC。不是永久免GC，也不沿当前terminal年龄直接删除未结证据。retention时长、法定保留及隐私删除由Root/Billing策略确认，不虚构天数。清除payload后仍须保留契约要求的幂等身份/digest到去重窗口结束，避免重投再次收费。

### 5. 后继数据验证

现schema保护不代表新DDL通过。正式切片必须覆盖fresh install+catalog列/类型/NULL/CHECK/索引drift；真实PG两个连接测试并发准备/attempt ordinal、同digest replay/不同digest rollback、所有故障注入点、旧lease写拒绝、cancel/terminal/late evidence所有顺序、outbox ACK丢失及重启、不重复Run terminal、不清unknown、同tenant-subject绑定、purge引用保护与到期有界释放。

精确文件集及测试落点见TECH前缀；`model/call_repository.py` 只拥有这些Agent事实的SQL，并与原Run finalizer共用连接。Billing schema/ledger、System schema、shared Redis namespace均无本仓写权限。本D0三面候选等待owner机器契约与SQL细化，不宣称数据库门已通过。

---

## R39 HTTP 4 owner 切换候选（当前解释覆盖下方历史 HITL 段落）

本轮仍为 main0245a36 基线上的完整 HITL 候选工作树，未发布 artifact、未更新 BFF/Web pin。
R40 精确追加现 JWKS contract test 的3处整体 HTTP version（标题/info/direct provenance）3→4；
其余 proof/JWKS/tuple/路径/SHA 断言逐字保持，最终范围为原19＋该1现文件，共20。
当前机器源已一次切为 Agent HTTP **4.0.0，原 /v1 单路径**；下方“机器仍3”的文字只记历史阶段。
既有已 typed4 的 control/events 保持，删除旧 resume tool_id/request_id 寻址与 Chat interaction alias；
Machine、两个 decoded payload mapping、owner checkers、生成 provenance、HTTP admission/receipt 和公开正负例同步。
这不是 scope/retry/effective-native/retention/P3B 完成；P3B 整段候选不变。

Root 本轮裁决覆盖历史 error/reason 承诺：receipt 继续现有 status/error_code，只有真实 typed
InteractionConflict 才落 failed/error_code=interaction_conflict；**不新增公开 reason/rejected 变体**。
内部 stale_pause/incomplete_collection/decision_not_allowed/not_waiting 不是额外 wire 字段。
missing context 保持现 control_apply_failed；读取失败/authority_lost 原样传播，不伪装 conflict，
不借新 authority 终结健康 Run。拒命令只收口该 command，当前 waiting head/source 不被清空。

### 数据边界保持

本轮没有 schema、Row、锁序或七 port 改动。control.body 仍唯一 canonical typed TEXT，request_digest
仍现规范化摘要；accept 锁内核同一 typed 字节/身份。InteractionConflict 只使用既有 command.error_code，
无 reason 新列。Run→command 的既有 UPDATE 只更新 admitted/persisted/applied，failed receipt 不被后续
control_apply_failed 覆写；pause/Run/Chat source 无副作用。HTTP admission 并不创建 accepted resume intent。
真实验收新例从 HTTP launch→读取原 pending dispatch→正式 claim/pause→HTTP 4 全集/旧字段拒绝，
由真实 worker 接受边界处理 stale，再独立 PG 读 body/digest/status 并验证 replay/source 不变。
该例使用明确 trusted pause 存储 fixture，不冒充 native 取证；native 事实由既有 45 PG 矩阵覆盖。

---

# kokoro-agent 数据模型

## AGENT-HITL-INTERNAL-CALLERS-R38：内部切换与生命周期候选

Root 已验前一桥冻结候选 44/44 真 PG（6.74s，自有库已回收）及独立审查 0P0/P1/P2；
此证据不覆盖本轮新增首次 worker pause 旅程，也不表示 HTTP4 发布。本轮仍基线 main0245a36＋既有候选工作树，Git/资源由 Root 独占。
原八 native proof、机器/OpenAPI/generated、SQL 与完整 P3B 后缀保持；下方 R35 等记录为历史，不再表示当前尚未运行那 44 例。

删除唯一遗留 `execution/approvals.py` 与 source 清单引用，原测试迁到真实官方 InMemory saver＋正式 checkpoint adapter 的完整集合/全集 map/安全 validation；
它只证明内存 SDK 形状，不充当 PG 消费或外部效果 exactly-once 证据。普通 caller 显式提供 reader/callback；
测试替身按明确场景区分初 pause、accepted、started/unknown，不把缺证据返回 active；control delivery 只更新正式 admission 的既有行。
裸 SDK 单测每 case 恢复原 registry/bootstrap，避免先于生产批准流程初始化全局状态，不放宽生产 plugin/registry 门。

`invoke_once` 必需 `on_native_settled(seal_usage)`；闭包绑定本次 native context 完全 drain 后的真实 token 总数及原 lease 的 record_usage，
并发/顺序重复成功 seal 至多写一次。真实 worker 在 waiting/unknown 分支、释放 lease 的 pause/reconcile 前 seal；active 不 seal，
正常 terminal usage 仍由唯一 finalizer 同事务提交。SDK interrupted 而 reader active 时，以仍有效原 fence seal 后返回非终态，不静默漏段或借新 authority。
usage/reader/settlement 持久失败在 native 异常映射外原样传播，绝不改写另一 run.failed；未 seal 的失败不推进 waiting。

initial/resume build 错误显式携带构建开始时捕获的 lease；仅此路径不走 control adoption，不从更新后的本地 lease 借 authority。
StaticRecipeAuthorityLost 零终态；Incompatible 自带 fence 与显式 captured fence 冲突也零终态；原 safe code/retryable 映射保持，retryable 不是自动重投。
本轮精确顺序 RED 2 例、必需 seal API RED 10 例、构建映射 RED 15 例及失效 fence race RED 6 例日志均保留。
授权 19 caller 文件纯组合已实际 438 passed/48 deselected（21.82s）；不是 Root 最终验收。首次组合人工中断 exit130 不计完成，
后续 -x 345 passed/1 failed 准确定位原裸 SDK registry 污染；修复 fixture 恢复后才得到上述组合结果。

新增既有 PG 事务测试 `test_first_worker_dispatch_seals_usage_before_durable_initial_pause`：正式 ingress/dispatch、正式 saver 与独立 reader、
官方 native projections，由真实 worker 首次进入 native（无预先 graph.ainvoke），独立 SQL 核 waiting/释放 lease/7+3 用量/零 terminal；
7+3 是确定性 SDK callback fixture，不声称真实 provider 用量。此新例及修改后原 44 例待 Root 真实运行。
完整 HITL4 机器/HTTP/contract 发布、跨 takeover 的原 generation 归属/absolute execution deadline、scope/retry/P3B/retention 仍各自原门，不以内部绿色缩减目标。

最终 worker 离线门（本候选）：全 Ruff format 267 files/check exit0，Pyright 0 errors；
fresh 全纯 1721 passed/38 failed/6 skipped/287 deselected（97.17s），剩 37 为锁定 machine/chat/public/proof contract，
此 38 中另 1 是 `tests/unit/execution/test_interactions.py` 原 valid fixture 缺 required action_result；
Root 于全纯柄结束后精确授权只补 action_result: None，原两个负例保留，单例实际 1 passed。
该一行之后未重复全纯（Root 冻结复验）；37 个机器/公开/证明契约失败仍原样保留，不宣称已获新的全纯 37-fail 计数。
正式 contract-check exit1 为 provenance aggregate stale，机器仍3的完整4发布门保留。
PG45只 collect（0.39s）；InMemory 首次 native entry 7/3 usage 探针通过，绝不据此宣称真实45已验。
日志 `/tmp/kokoro-agent-callers-r38-{full-pure-final,type-final,format-final,lint-final,contract-final,pg-collect,first-pause-probe}.log`。
本轮未启动/操作 PG、Redis、provider 或应用服务，未执行 Git/依赖安装/build；上述纯测试自身的离线/loopback fixtures 不代表生产服务验收。
候选冻结交 Root 后停写，由 Root 独占真实45、原相邻PG与完整发布门。

## AGENT-HITL-NATIVE-BRIDGE-GREEN-R35：实现候选，等待 Root 真实 PG

当前基线仍 main0245a36＋已验P2事务核；Root桥RED实际18 failed/26 passed/0 skip（4.61s），
`/tmp/kokoro-agent-hitl-native-bridge-r35-root-real-pg-red.log`。本轮已授权普通checkpoint_interactions.py与既有
Run/SQL/worker边界实现；不是HTTP4发布、完整scope/P3B或外部exactly-once验收。下方D0“未实施”保历史，
本节覆盖当前候选事实；机器/生成/依赖及P3B完整suffix保持，Git/真实PG仅Root。

七ports与完整observation canonical SQL、Run-first GC、独立官方reader、唯一StartedResume调用和drain后持久callback已接入。
原supervisor四mixin与execution/run_agent.py由Root精准追加：删除实际resume路径旧adopt/fingerprint/partial-awaiting selector，
不把其职责搬进门面；cancel/steer/普通dispatch保留。reader与on_native_settled均必需，无空默认/alias/fallback。
stream.interrupted()完成官方iterator后且context退出才调用持久callback；callback失败在native异常handler外原样传播，
不制造另一个run.failed。waiting/unknown不发completed；新validation直接waiting，active仅来自完整正向证据。

实际纯新增15/15已通过；三纯文件完整选择20 failed/45 passed/3 resource deselected（0.88s），旧HITL寻址/reader消费者明确未切。
域与架构选择1 failed/69 passed（2.38s）：唯一失败是原test_interactions.py候选fixture缺新必需action_result，未越权补默认；
首次误用不存在tests/architecture的collection错误保留日志，不计业务RED。44 PG最终只collect（0.02s），原26＋新18，尚未实际运行。
R37最终定点24个Python文件Ruff format/check全过，19生产Python定点Pyright0；full Pyright145错误为旧approvals35＋未切测试110（包含原8），不以局部0冒称全门。
官方InMemorySaver纯探针实际验证单input与root/child/mixed三集合材料化、一次全集map及正向分类；
它们只证明SDK形状，不是独立PG连接、事务/进程重启或外部副作用证明。所有首失败日志保留。

两处已授权测试fixture纠正：混合图节点统一approval_node（原state key/edge不匹配）；新增source正例补原必需input_schema={}。
保持原全集/一次map/最终向量与26断言；不以fixture修正记生产GREEN。原八nativeproof不变。
Root R37追加真实result-review consumer与其现测试：唯一request_id决策解析/匹配，拒旧tool_id与重复request_id；
工具call_id/journal/cache身份原样。实际RED为5 failed/6 passed，切换后11 passed（0.13s），所有approve/respond/reject/
非法缺项/缓存重入防双执行断言保留；日志`/tmp/kokoro-agent-bridge-r37-result-review-{red,green}.log`。
完整4仍须其他caller/fakes/机器/contract/generated统一切换，不以该consumer单点绿色宣称全链已完成。
最终四纯文件按原integration/e2e/acceptance排除：20 failed/56 passed/3 deselected（0.87s）；原20旧消费者RED保留。
验证选择曾误用not resource清默认过滤，3个旧integration setup尝试Redis后ConnectionRefused，未成功连接或执行native/provider；
该20 failed/56 passed/3 setup errors日志单独保留，不计纯门或业务RED；后已按原标记重跑。
初pause跨expired takeover缺原generation归属时严格拒native_pause_generation_unattributed，零再次invoke；
不借新authority给旧执行收口、不称A窗口跨generation恢复已完成；当前无独立absolute execution deadline。Root明确TTL不是执行期限；该跨takeover恢复/期限门保留，不新增deadline schema。

### 本片SQL/事务事实

canonical schema已增加10列observation表、command桥9列及具名CHECK/索引，时间仍TIMESTAMPTZ(3)，fresh-only。
read_resume_context复用原列，不增第二请求/plan事实；codec逐byte核原Run/request/body以及完整观察身份。
正向观察与native_observed绑定同事务；reconcile再锁Run/command/观察后原子写head/result/Chat。
Chat序列锁可能跨Run等待，record_pause/reconcile在append后再次DB clock核原lease，失效抛错使整事务回滚；
accept从锁后新DB clock设置新lease，不用等待前时钟偷延续旧authority。catalog精确类型/CHECK/索引检查已写，待Root真PG验证。
未知只发布unknown且保全集；terminal结清后Run-first purge先删观察，再command等children，晚到缺Run零INSERT。


## AGENT-HITL-NATIVE-BRIDGE-D0-R35：精确桥接口（候选，未实施）

Root已验R3事务26/26（2.51s）、相关四PG文件71/71（5.56s），资源均回收；它们不证明正式桥。
Root fresh default实际78 failed/1664 passed/6 skip/268 deselected（97.96s），Pyright仍43错误；
失败分布supervisor26、machine_contract16、chat_response14、hitl11、public_contract6、control_commands4、execution_proof_artifact1。
日志`/tmp/kokoro-agent-hitl-p2-r34-root-{related-pg,default}.log`。本轮仅四HITL前缀；生产、测试、机器与P3B整suffix保护。
以下精确决定覆盖下方历史D0的未定字段/方法；既有P2事务语义保持，新增桥/schema/完整4均待源码卡和真实门。

### 观察表：唯一canonical SQL候选

新表`kokoro_agent_run_checkpoint_observation`位于既有Agent schema，无FK/迁移/历史补值。
选择每次完整读取证据一行，而非每task拆行：全集与跨namespace因果关系在一个canonical envelope内，避免拼接不同读取的半集合。
已有Run/command不复制native内容；完整安全pause快照仅在pause证据内供A前crash重建，生命周期随Run。

| 列 | SQL类型/空值/默认 |
| --- | --- |
| run_id | TEXT NOT NULL |
| observation_digest | TEXT NOT NULL |
| generation | BIGINT NOT NULL |
| command_id / attempt_id | TEXT NULL / TEXT NULL，无默认 |
| kind | TEXT NOT NULL，pause/read/resume/probe |
| disposition | TEXT NOT NULL，current/audit，由锁内权威判定，不信caller |
| evidence_bytes | BYTEA NOT NULL，唯一完整canonical envelope，1..8388608 bytes |
| probe_read_id | TEXT NULL；仅probe非空 |
| created_at | TIMESTAMPTZ(3) NOT NULL DEFAULT clock_timestamp()，锁后DB clock写入 |

PK `(run_id,observation_digest)`；具名CHECK：`ck_checkpoint_observation_identity`要求run_id非空、generation>0、
digest为^[0-9a-f]{64}$；`ck_checkpoint_observation_target`要求command/attempt全NULL或均非空，
且read可两种、pause必须均NULL、resume/probe必须均非空；`ck_checkpoint_observation_kind`限定四kind及两disposition；
`ck_checkpoint_observation_bytes`限制octet_length；`ck_checkpoint_observation_probe`要求(kind='probe')等价probe_read_id非NULL且非空。
`uq_checkpoint_observation_probe`唯一(run_id,command_id,attempt_id,probe_read_id) WHERE kind='probe'；
`idx_checkpoint_observation_target` ON(run_id,command_id,attempt_id,generation,observation_digest)。
PK前缀同时支持Run清理；不加无查询用途的created_at索引。NULL初pause不用空串command或假attempt。

canonical envelope固定format=`kokoro-agent:checkpoint-observation:1`，字段为run_id/generation/command_id/attempt_id/kind/probe_read_id，
以及facts：按完整原group顺序的group_id/thread_id/checkpoint_ns/checkpoint_id/parent_checkpoint_id、
逐task的task_id/interrupt_id/item_ids、实际RESUME向量length/digest、当前ERROR标志、完整当前interrupt安全摘要，
和逐namespace有序successor链（checkpoint_id,parent_checkpoint_id,current_task_ids,current_interrupt_ids）。
root parent可NULL；namespace允许空串，其余有效ID非空；空RESUME向量明确length=0与空向量摘要，不伪造消费。
初pause还含完整既有DurablePauseSnapshot的安全groups/locator；read/resume/probe不得携message/decision/结果正文。
probe额外含quiescence与progress_digest；其他kind无此字段。kind=resume必须额外含原pause revision/ref/collection_digest，
classifier已核全部实际向量与dispatch plan相等及稳定后继；next_pause可空，有值时存完整安全snapshot并令结果waiting。
JSON精确字段/未知字段/重复key与ID/覆盖/顺序均由typed decoder拒绝；复用现canonical JSON UTF8规则。
observation_digest=SHA256(evidence_bytes)；不含created_at/disposition，完整身份在bytes内，重读逐byte核而非只信digest。
同PK不同bytes是corrupt；同read_id不同bytes是conflict；观察重放不更新时间/证据。SQL CHECK不冒充验证整个envelope。
received批次只在装饰器有限内存诊断中保摘要，不写此事实表，official独立读取才构造facts。
实际向量摘要采用native adapter的严格JSON值codec（同canonical UTF8，无repr/default=str），非JSON值拒归属并unknown，
不改变官方serde；pre向量与expected追加摘要必须同codec。只有输入值恰等且完整因果证据相符才证明归属。

### 现command新增桥列及约束

| 列 | SQL类型/默认 | 用途 |
| --- | --- | --- |
| resume_observation_digest | TEXT NULL | native_observed/reconciled正向消费证据引用；不从head最新观察补值。 |
| resume_result_kind | TEXT NULL | accepted/native_consumed/validation_failed/unknown/cancelled；拒绝接受的命令仍走原失败receipt。 |
| resume_result_revision / resume_result_source_index | BIGINT NULL / BIGINT NULL | 最近已公开动作结果的真实revision/source，原accepted identity另列保留。 |
| resume_probe_count | SMALLINT NOT NULL DEFAULT 0 | 0..3成功稳定读计数。 |
| resume_probe_progress_digest | TEXT NULL | 明确整条因果链/当前tasks/实际向量的摘要。 |
| resume_probe_quiescence_json | JSONB NULL | 严格local_drained、worker_boot_id、invocation_id；不保存SDK对象。 |
| resume_probe_last_read_id | TEXT NULL | 最近实际独立读取身份。 |
| resume_probe_checked_at | TIMESTAMPTZ(3) NULL | 锁后DB clock记录最后成功probe；失败读取不更新。 |

扩现`ck_control_resume_intent`加入native_observed，`ck_control_resume_attempt`要求其已started全列非空。
`ck_control_resume_observation`：digest可NULL或64hex；native_observed必非NULL；reconciled允许NULL仅既有record_pause已证明新暂停、
未声称native消费的历史路径，正式bridge的reconcile_resume正向结果必须非NULL；terminal保原值不抹去审计。
`ck_control_resume_result`：kind/revision/source全NULL或全非NULL，后者kind限定上表、revision>0、source>=0且intent非NULL。
accepted新事务同时初始化结果；新桥不改原accepted_revision/source用于历史重放。terminal仅对未结intent写cancelled，
不是取消Run也强称native没执行；此kind在API解释为动作因终态关闭，不是工具成功/失败判定。
`ck_control_resume_probe`：count在0..3；四个probe元数据（digest/quiescence/read_id/checked_at）全NULL或全非NULL；
全NULL时count=0，全非NULL时digest64hex、read_id非空、quiescence是object且attempt存在。progress改变可保新元数据且count=0。
`idx_control_resume_unsettled` ON(run_id,command_id) WHERE resume_intent_status IN
('accepted','dispatch_started','native_observed','unknown')；服务端limit有界扫描后逐Run重验，禁止扫描即授执行权。

QuiescentProbe首次插入观察及计数必须同事务；同probe_read_id重放只返当前计数不累计，独立新读取才生成新read_id。
没有旧元数据的首次合格读取count=1；相同quiescence+progress时count+1；已有元数据任一变化写新基线count=0；后续相同成功读取到count=3饱和。
当前本地tracked任务重新健康/有进展时，reset_reconcile_probe在Run→command锁内清probe元数据与count，不沿旧静止token累计。
读失败不制造probe行；观察已提交但ACK丢失重放同read_id。跨进程无local_drained证明时保持unknown、零计数，
不从lease过期推断旧执行已停止。到3只产生exhausted结果，终态仍原finalize_terminal。

### 锁序、引用和source事务

record_checkpoint_observation：Run FOR UPDATE→有command则command FOR UPDATE→精确observation；全部锁后DB clock。
request/原generation/command attempt与冻结locator必须匹配，observation.generation必须等于传入lease.generation。当前非terminal且Run owner/generation与传入lease匹配、锁后expiry有效可current；
旧generation或terminal仅在Run及原command/attempt引用仍存在且证据归属可核时audit，不能改intent/head/source。
初pause无command仅允许generation等于当前Run，失效generation不保无归属审计。锁后Run缺失零INSERT。
已有同PK同bytes观察可在原identity核验后精确重放（包括pause已释放lease），不更新disposition/时间；新插入才按当前或audit资格判断。
正向resume观察入库及intent→native_observed、digest绑定同事务；审计、read/probe/初pause都不推进它。
reconcile_resume重锁Run→command→证据，校验当前lease、原identity、尚未终态、stored bytes及全部digest；
正向ConsumedPauseEvidence分支写reconciled/result/head/Chat同事务；UnknownResumeEvidence分支无需伪正向digest，
只从原started/unknown身份写unknown/result与原resuming集合。新validation直接waiting并释放lease，active只清当前pending，原snapshot/command证据保留。
unknown动作结果保持resuming集合但新interaction_revision并同事务Chat；现P2 mark_resume_unknown仍只是私有状态，
后继协调从该状态显式补结果，重放同result/source不增revision。无新通道或借普通activity清等待。
terminal依旧原finalize_terminal原子结清未结intent/result与交互terminalsource，旧正向证据不反写终态。
purge先锁Run再复核terminal/retention/原sandbox清理及intent结清，统一delete_run_rows先删观察再command等children最后Run；
observer先胜则purge看其引用状态，purge先胜则late observer零insert。terminal结清后观察随Run有界删除，不永久免GC。
bridge表不复制native checkpoint表，最终checkpoint DAG/Conversation引用释放仍后继，绝不以本表删除冒称完成该门。

R35接线补名：read_resume_context仅在同一Run→command一致读取事务重建已有列；无新增列/表。
snapshot是当前Run head，original_pause/intent/dispatch_plan/attempt_generation/observation_digest来自指定历史command；
accepted plan/gen NULL，started/native_observed/unknown全存在且attempt一致，严格decoder核原UTF8请求及tenant/scope。
禁止从当前native状态反推原pre向量；明确返回六字段定义见TECH本R35补名节。

## AGENT-HITL-P2-R33-R3：故障注入固定组合修正

Root R2真PG **24 passed / 2 failed / 0 skip（2.62s）**；
`/tmp/kokoro-agent-hitl-p2-core-r33-r2-root-real-pg.log`，自有DB已回收。
两例在安装trigger前被测试helper旧phase白名单拒绝，未到start/terminal故障回滚断言；不归因生产事务。
R3仅将helper校验改为七个明确(stage,phase)组合，增加(command,dispatch_started)/(chat,terminal)，
仍拒任意字符串或无效组合；原真实AFTER trigger、RaiseException、wholefacts/outbox=0断言原样。
生产18路径及其余冻结25路径不改；P3B四suffix与两native proof保护。
本轮Ruff format/check通过、定点Pyright0，26 collected（0.05s）；无DB/Git/服务，无运行句柄。
日志`/tmp/kokoro-agent-hitl-p2-core-r33-r3-static.log`与`/tmp/kokoro-agent-hitl-p2-core-r33-r3-collect.log`。
Root下一次实际26 PG仍是放行门，不将24局部通过冒称26或完整HITL4完成。

## AGENT-HITL-P2-R33-R2：正式launch fixture与GC竞争补证（仅测试/文档）

Root R33实际PG **16 failed / 6 passed / 0 skip（1.78s）**，日志
`/tmp/kokoro-agent-hitl-p2-core-r33-root-real-pg.log`；自有DB已回收。
16例共同在真实control的scoped lookup早停404：原fixture只try_claim没有dispatch，
现正式get_request_scoped要求Run/dispatch同tenant及namespace JOIN，此资格正确，不放宽生产。
本R2仅改现事务PG测试及四docs批准prefix，18生产与其余冻结文件保持；P3B完整suffix、两份native proof未改。

统一fixture现在调用公开AgentIngress.launch创建正式dispatch/session，从get_pending_dispatch读取原RunRequest，
独立连接核原request_json UTF8、tenant/namespace/pending，再claim_dispatch且复核scoped Run原bytes，最后pause/control。
不手插dispatch、不在已pause之后补admission、不绕过真实Ingress.control。仅通知bus使用既有FakeBus；所有事实仍PG。
首正例由持久RunResume逐项重建expected kind/payload base64，与整份持久decisions codec比较，不只检查digest自相等。

独立Sol原生产审0P0/0P1/1P2保留：原串行terminal→purge→late不等于竞争证明。
现补purge↔admit_control及purge↔record_control_delivery各两锁序，共4例：
精确owned blocker PID持Run锁，先操作进入等待队列后再投第二操作；递归pg_blocking_pids包含soft queue边，
确认1→2独立backend均阻塞才释放，无sleep/共享waiter计数/timeout放宽/重投。
purge先胜则admission 404零新child；admission先胜只原命令创建一次且随同一次purge删除。
delivery是UPDATE既有command而非INSERT，terminal或missing两种资格均false；其先拿锁也不伪造成功更新，
既有child仍随一次purge清理，最终Run/control/dispatch均零残留。新测试尚待Root真PG，不能据收集声称竞争已证。
R1六个真实通过仅包含pause rollback与四catalog drift；accept/start/terminal/竞争的最终断言仍待修fixture后重跑。
本R2不修改生产、不运行DB/Git/服务，不把HITL4或native桥标完成。
实际离线：本测试Ruff format/check通过，定点Pyright 0 errors，26 collected（0.05s）exit0；
日志`/tmp/kokoro-agent-hitl-p2-core-r33-r2-test-static-r2.log`与`/tmp/kokoro-agent-hitl-p2-core-r33-r2-collect-r2.log`。
首新增断言列表未标类型产生的1个Pyright错误保留原log，补明确list[dict[str,str]]后真实重跑0，不放宽检查。
26 PG入口仍同一文件、`-o addopts='' -q`，由Root独占资源；无运行句柄，冻结待验。


## AGENT-HITL-P2-CORE-GREEN-R31：事务核工作树候选（未发布）

Root已以真实PG 6 failed/0 skip（缺record_pause；未到rollback）放行本批，基线仍main0245a36；
当前新增唯一普通生产文件infrastructure/postgres_run_interactions.py，并在既有Run/command/Chat上实现
pause/accept/start/unknown及原finalize_terminal收口。Root随后仅追加现infrastructure/chat_mappers.py持久类型decoder，
旧interaction拒绝，不留兼容分支。本批30路径（18生产含SQL＋fake＋7tests＋4docs），不等于36/52或后继22全授权。
机器OpenAPI/provenance/generated仍3；native桥、observation表、真实消费/恢复、全部旧worker消费者未切换。
以下历史D0/tests-only段保留其当时基线；本节覆盖其“生产尚未修改/仅四docs”当前状态解释。P3B整suffix未改。

当前内部API是`accept_resume(request, command_id, owner)`，caller不再传第二Submission；
同连接Run→command锁后，从现权威`control.body`唯一typed decode，重建Submission，再整批校验/归一。
`StartedResume`仅首次start提交返回；同attempt重放、历史command重放、unknown均不授第二执行许可。
PostgresRunLeases仍唯一终态入口；purge入口委托现context内统一Run-lock/children清理实现，
不新增GC模块。正常terminal同事务清集合并写interaction terminal在run terminal之前；私有quarantine保持无新增公开源。
现partial ToolAwaitingApprovalPayload缺完整pause/revision，旧projection显式拒绝，绝不伪装新source；后继bridge必须替换真实调用。
测试fake新增port只明确抛NotImplementedError，不伪造持久结果；本能力正例只使用真实PG repository。

### 入口与持久请求的唯一表示（Root R32裁决）

既有`body=typed_message.model_dump_json()`；既有request_digest不是该TEXT直接SHA，而是`sha256:`前缀＋
`json.dumps({run_id,...控制字段}, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)` UTF8摘要。
新4 resume入口先严格RunResume validate（必需revision/ref/item，无旧别名、未知字段/重复item拒绝），
再唯一`control_request_digest(message)`对`message.model_dump(mode="json", exclude={"command_id","request_digest"}, exclude_none=True)`算上述摘要；
将摘要回填typed message后以原model_dump_json写control.body，不保存raw第二事实。cancel/steer原表示不变。
锁内`decode_resume_command`同时核Run/session/command、body中的digest、重新计算digest与存储列，
以及`RunResume.model_validate_json(body).model_dump_json().encode("utf-8") == body.encode("utf-8")`；
不拿两个调用同一字符串的自相等替代这些独立身份/摘要校验。
cancel/steer即使携带null的resume专属ref/revision也在入口拒绝，保原表示不扩大。
nullable optional（如reject.reason、approve.args）允许入口null/omitted同义归一；required revision/ref/item无默认，
省略/null/unknown字段均拒。持久body重排、whitespace或被删默认字段即使解析等价也拒；
Run原request_json仍复用P3A原model_dump_json UTF8逐byte身份，不使用上述摘要codec替代。

### 当前实际证据与未到达门

worker最终受限纯选择121 passed＋架构36 passed，合并157 passed（2.38s），exit0；定点Ruff check/format-check通过。
事务PG仅22 collected（0.04s），零DB/Redis/provider/服务访问，所有新rollback/start/锁等待/竞争/terminal/catalog断言待Root实际运行。
完整机器候选两contract文件：30 failed/26 passed（0.97s），3机器及application decoded mapping仍旧，保留真实RED；
全Pyright 43 errors仅execution/approvals.py及旧tests/unit/execution/test_hitl.py的旧tool/request寻址消费者；
本片授权文件无新增类型错误。既有32fail tests-only、Root6fail及本片codec/duplicate/mapper RED日志保留。
不以局部绿或收集数冒称完整4/default通过；Root真实PG和独立审查后决定下一片，完整机器/bridge/消费者统一发布。

## AGENT-HITL-P2-RED-R31：首批契约与事务RED（tests-only）

基线main0245a36＋P2-D0冻结57140f75；WIN03独立审0P0/0P1/2P2。唯一授权现两contract测试、
新普通tests/integration/database/test_run_interaction_transactions.py及本四docs批准HITL前缀；生产/机器/SQL/SDK不改。
时间列候选明确TIMESTAMPTZ(3)；后继infrastructure/checkpoint_interactions.py是尚不存在的新增普通文件，
只列后继，不在本卡创建桥。P3B整suffix保持。

WIN03两修正落实：resume_accepted_at/resume_started_at严格TIMESTAMPTZ(3)，catalog后继检查precision=3；
checkpoint_interactions.py明确后继新增，本卡零生产/SQL表变化。事务测试使用现canonical schema的真实安装fixture，
每例Root自有DB内独立owner schema；触发器/function也只在该测试schema，fixture统一回收，不接共享库。
Pause输入完整snapshot摘要可重建；私有定位不出Chat，accept保存原完整snapshot及source index/revision。
缺现方法先显式assert；后续实现达到对应路径才可报告真实rollback通过，收集六例不算数据库已跑。

## AGENT-HITL-PERSIST-P2-D0-R29：两表精确增量（候选，SQL未改）

当前main0245a36仅P1纯域已验；本片零新表。原D0观察表和native_observed状态随真实桥后继首次writer落地，
不在P2预建空表/伪消费状态。Run/command/Chat原子边界及五方法见TECH；完整typed4是前置候选，不等于发布。

### Run列与具名约束

唯一canonical database/schema.sql，现kokoro_agent_run增加：

| 列 | SQL类型/默认 | 语义 |
| --- | --- | --- |
| interaction_revision | BIGINT NOT NULL DEFAULT 0 | 每次可见phase/集合变更+1；start/unknown不增加。 |
| interaction_source_index | BIGINT NULL | 当前可见interaction source的原Run event index；0轮次/私有quarantine无公开source为NULL；不拿revision冒充source_index。 |
| interaction_phase | TEXT NOT NULL DEFAULT 'active' | active/waiting/resuming/terminal。 |
| pause_revision | BIGINT NOT NULL DEFAULT 0 | 每个新等待轮次+1，包括同ID validation；接受不增加。 |
| pause_ref | TEXT NULL | 不透明本轮身份；0轮次NULL，>0非空；终态可保原身份用于审计。 |
| pending_groups_json | JSONB NOT NULL DEFAULT '[]'::jsonb | 原group/item顺序，安全display/validation；item状态由phase决定，不另存第二状态字段。 |
| pause_snapshot_json | JSONB NULL | 本轮不可变groups＋私有locator完整快照；head清空后仍用于摘要审计。locator按group/task/item记录thread/ns/checkpoint/task/interrupt，空namespace合法，其余ID非空。 |
| pause_collection_digest | TEXT NULL | 下述codec中pending+locator组合的SHA256小写hex。 |
| interaction_command_id | TEXT NULL | 当前/最近intent逻辑关联；读时核同Run，无外键；非intent场景NULL。 |

具名CHECK：`ck_run_interaction_revision`为0<=pause_revision<=interaction_revision；
`ck_run_interaction_phase`限定四值且(terminal = (interaction_phase='terminal'))；
`ck_run_interaction_collection`要求JSON array、waiting/resuming长度>0、active/terminal长度=0；
`ck_run_pause_identity`要求pause_revision=0时ref/snapshot/digest全NULL，否则ref非空、snapshot是object、digest匹配^[0-9a-f]{64}$；
`ck_run_interaction_command`要求resuming有非空command，其他允许NULL或非空历史command（拒空串）。
`ck_run_interaction_source`要求非NULL时>=0，interaction_revision=0时NULL，waiting/resuming时非NULL；
terminal的NULL仅用于原private quarantine无公开事件路径，adapter验证该边界；普通终态必须存本次source_index。
新Run默认active/0/[]，不是补历史数据；终态清groups但保pause定位到Run retention。terminal时digest仍指原冻结pause_snapshot_json，
不按清空后的pending_groups_json重算；waiting/resuming的pending_groups必须与snapshot.groups逐值相同。snapshot是不可变暂停审计事实，
pending是当前可见集合，生命周期不同；终态/未来active只清后者。历史command另保原snapshot，head进入新轮次不会覆盖它。完整ID唯一/非空items/允许动作
在typed decoder与域规则校验，CHECK不宣称验证整个JSON模型；phase=active且有历史pause只留给后继已证明消费转移，P2无解除writer。

### 现control command列、索引与CHECK

现receipt status/body/request_digest字段语义保持，新增列均NULL默认（非resume或尚未accepted）：

| 列 | SQL类型 | 语义 |
| --- | --- | --- |
| resume_pause_revision / resume_pause_ref | BIGINT / TEXT | accepted后正revision、非空ref；不从当前新head反填历史command。 |
| resume_decisions_bytes / resume_decisions_digest | BYTEA / TEXT | 完整分组规范化私有决策canonical bytes及SHA256，原payload bytes无损base64。 |
| resume_accepted_revision / resume_accepted_source_index | BIGINT / BIGINT | 接受时原interaction_revision>0与source_index>=0；精确重放从既有Chat唯一键读取原source，不从当前head猜原seq。 |
| resume_intent_status | TEXT | P2仅accepted/dispatch_started/unknown/reconciled/terminal；与HTTP receipt分离。 |
| resume_pause_snapshot_json | JSONB | 接受时原pause groups＋locator完整快照；head变轮次后仍可恢复历史归属。 |
| resume_pause_collection_digest | TEXT | 原pause集合摘要，accept一次保存，不随新head更新。 |
| resume_attempt_id / resume_attempt_generation | TEXT / BIGINT | 唯一非空attempt与当次有效generation；started后不可清空/改写。 |
| resume_dispatch_plan_json | JSONB | 按原group/task顺序的pre-resume长度/digest、预期追加值digest及原定位；精确形状如下，无SDK对象/凭据/结果正文。 |
| resume_accepted_at / resume_started_at | TIMESTAMPTZ(3) | 各自Run→command全锁后DB clock；不取caller/local clock。 |

`ck_control_resume_intent`：status NULL时上述所有resume列全NULL；非NULL时基础accepted列（pause revision/ref、
bytes/digest、snapshot、集合digest、accepted_at、accepted_revision/source_index）全非NULL，pause/accepted revision>0且accepted_revision>resume_pause_revision、source_index>=0、身份非空、JSON object、两个digest严格64hex，
bytes长度1..8388608。status限定上述五值；没有native_observed占位writer。
`ck_control_resume_attempt`：attempt_id/generation/plan/started_at四列要么全NULL要么全非NULL；全非NULL时ID非空、
generation>0、plan object、started_at>=accepted_at；accepted必须全NULL，dispatch_started/unknown/reconciled必须全非NULL，
terminal允许未开始或已开始两种。reconciled表示新pause已证明，不代表active/native消费成功。
现PK(run_id,command_id)保留；新增唯一索引`uq_control_resume_pause` ON(run_id,resume_pause_revision)
WHERE resume_intent_status IS NOT NULL，保证同轮次终态后也不接受第二command；新增`uq_control_resume_attempt`
ON(run_id,resume_attempt_id) WHERE resume_attempt_id IS NOT NULL，防不同命令复用attempt。无无用途poll索引；
本片按Run/command现PK读取，未来桥的协调查询/索引另按真实SQL评审。

### 唯一codec、Row与持久重建

codec局部放新postgres_run_interactions.py，不增公共序列化模块。JSON表示固定
`json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")`；
禁止重复key、未知字段、非finite数和非canonical bytes重建；JSONB不保原文本，读时以typed语义重建唯一bytes再核digest。
决策value为`{"format":"kokoro-agent:resume-decisions:1","groups":[{"group_id":...,"decisions":[{"item_id":...,"kind":...,"payload_b64":...}]}]}`；
group/items按原pause归一，base64标准alphabet带padding、严格decode并重encode相等；纯域可保存任意bytes；本生产payload是typed decision去除type/item_id和nullable None后的canonical JSON，approve无值固定为UTF8 `{}`，不手造第二Submission。
resume_decisions_digest=SHA256(该BYTEA)，不接受只同digest但bytes不同，不改变既有HTTP request_digest的表示或重排原request_json。
pause_snapshot_json的唯一value为`{"format":"kokoro-agent:pause-collection:1","groups":完整安全groups,"locator":完整私有locator}`，
pause_collection_digest=SHA256(该value canonical UTF8)，command复制同snapshot与digest；
JSONB组内domain字段(group_id/items/item_id/request_id/allowed_decisions/validation)与display分别显式validate，不使用asdict/reprdump。

内部_RunRow/_CommandRow局部不可变typed Row明确数据库nullability；decoder逐字段验证后组成既有InteractionState/
ResumeIntent、DurablePauseSnapshot及InteractionSnapshot，不把dict/cast当验证、不把DB Row露出port。
locator形状固定`{"groups":[{"group_id":str,"thread_id":str,"checkpoint_ns":str,"checkpoint_id":str,"tasks":[{"task_id":str,"interrupt_id":str,"item_ids":[str]}]}]}`；
各item恰好归属一个task/interrupt，group与snapshot.groups逐序覆盖；同task可以含多个action，禁止靠flatten丢interrupt边界。
dispatch plan固定`{"format":"kokoro-agent:resume-dispatch:1","attempt_id":str,"groups":[{"group_id":str,"tasks":[{"task_id":str,"interrupt_id":str,"pre_resume_length":int,"pre_resume_digest":hex64,"expected_append_digest":hex64}]}]}`；
定位来自存储snapshot，不复制第二套可变定位；length非负且bool拒绝，digest按实际native向量codec由后继adapter提供，P2只验证结构与覆盖，
不自造native值编码。plan.attempt_id与列相等；start plan摘要不是消费证明。三个JSON对象typed后canonical编码均限制1..8388608 UTF8 bytes；
数组中重复/遗漏/乱序错位均拒绝，大小由adapter验证，SQL的JSONB object CHECK不冒充UTF8大小验证。
_CommandRow保历史完整决策与定位及原accepted source index/revision；head只有一个command引用，不复制无限ledger。start plan对pause定位和所有group/items逐项覆盖，
任何缺项/重复/错namespace/摘要不匹配拒整批。plan来源的原生证据真伪仍属后继native adapter，不由SQL事务凭空证明。
schema.py注册列/默认/nullability/具名CHECK/索引精确catalog，fresh/drift测试逐项破坏；不增native SQL/外键/迁移/兼容补值。

### 原子与生命周期

record_pause同时写head、释放lease、Chat；accept专用paused领取generation并写command/head/Chat；start另事务写唯一attempt后才授许可。
原finalize_terminal同事务清head/结所有未结intent/有序Chat，private quarantine保原不公开边界。purge先Run固定序锁，锁后clock复核
terminal/retention/原sandbox清理及intent均结清，现delete_run_rows删commands后Run。admit_control创建command也Run-first，消失零INSERT；现record_control_delivery只更新既有行，
同锁图重验Run/terminal资格，消失零UPDATE，不将其误述为创建入口。
本片无新表故不修改AGENT_TABLES清单数量，不假称晚到observation表竞争已验；其后继必须先锁Run再写观察、统一清理同事务。
unknown不永久免GC：原权威终态把未结intent标terminal，但不篡改attempt/私有decisions或倒推native已消费；按现Run retention结清。
完整checkpoint引用DAG/Conversation最终释放仍后继硬门。新事务测试独立普通test_run_interaction_transactions.py，原八native测试不改。

R29 Root已裁决：P2为完整HITL owner目标内的先行事务TDD，36不是独立可发布4；旧审批消费者仍按tool/request寻址，
必须按TECH依赖阶段同一owner交付替换全部真实编解码/worker路径。必要candidate wire同步变更造成的旧tests RED如实保留，
待完整HITL验收，不用alias/default、伪source或只SQL方案掩盖；native内部HumanRequest身份与外部item_id的受信映射不等于wire兼容。
TECH列22精确扩展依赖（含真实attempt跟踪supervisor/context），每批Root另卡；本次仍只四docs，没有源码/协议授权。

## AGENT-HITL-DOMAIN-P1-R28：不可变内存规则，SQL未变

基线main512a846。本片只新增纯内部group/item/submission/intent/head值，显式阶段及完整集合不变量；没有新表/列/序列化数据库Row。
所有集合使用tuple、决策内容使用不透明bytes，源对象不可变；revision由纯规则递增，不自行读取clock或获取fence。
accepted/start/unknown/terminal是纯合法转换结果，尚未连接Run→command→Chat持久事务。
当前intent以外的历史幂等仍由后继command表查询，不在head无限累积；native_consumed与observations不由纯规则伪造。
本片无migration、补值、旧schema兼容或GC改变；后继真正durable门及P3B未提交suffix保持。
当前纯构造同时拒future intent及缺accept revision的resuming重建值；这不是数据库catalog/fence验证。
原schema/所有既有源码byte未变；PG8是已提交native证明的历史结果，不是本片事务验收。


## AGENT-HITL-D0-R27：持久交互head/intent/native证据桥（2026-10-01；仅设计）

### R27 Root已裁决的版本顺序（覆盖下方旧候选数字解释）

独立完整HITL owner切片使用 **Agent HTTP 4.0.0、原/v1单路径clean-slate替换**；当前机器源仍3.0.0，
本D0不提前修改。4.0版本号仅表示本次breaking协议，不表示完整scope第四阶段目标验收。下方历史候选的
“HTTP4 required retry/fullscope协调激活”不再作为本次4.0发布内容；这些能力继续完整goal，后续若breaking则另发Agent5.0.0。
P3B effective-native、scope/retry/checkpoint/retention功能目标均保留，库fork仍未批准。BFF4.0/后继4.1是其独立版本线，不机械同号。
一次替换旧interaction/resume解释、无新/v2长期双轨、无兼容fallback；Agent4机器＋实现/schema/artifact验证提交后，
BFF才固定pin并更新集合投影，再Web消费。HITL本身也必须完整实现、真门通过后发布，不发半contract。

当前main af45817260478f1ee755d8e6e6963051e2049062，已有Run、control ledger、独立native checkpoint和Chat interaction持久投影；
当前SQL没有下列交互head/bridge字段。本节不改SQL，不否定已有durable interaction；以下P3B effective列仍另一个未批准候选。

拟在现Run增加interaction_revision（BIGINT非负，初始0）、交互phase（初始active；无pause_ref时空集合不发虚构等待事件）、
pause_revision、opaque pause_ref、完整安全pending JSONB和内部native locator/集合摘要。phase与items状态由CHECK约束，
waiting非空awaiting、resuming非空submitted、active/terminal空；集合ID唯一及允许决策由事务应用规则验证，不假称JSONB CHECK涵盖全部。
pause_revision每次新等待/validation重问递增，不随每次resuming增；interaction_revision每次可见集合/phase更新递增。
terminal的交互清理和原Run terminal在同一finalize_terminal事务完成，不新增第二终态入口。

现control command表扩展expected pause revision/ref、完整normalized decisions私有表示/digest、内部checkpoint定位、
resume-intent状态（内部枚举固定accepted/dispatch_started/native_observed/reconciled/unknown/terminal），与HTTP receipt状态分离；
原applied/succeeded不自动当native_consumed。Run→command锁顺序，锁后clock/fence/tenant及revision比较，
最多一个同轮次accepted动作；同command同digest返回原动作，不更新新轮次。未知ACK只重读/重放原命令，不生成新ID。

拟新增owner内`kokoro_agent_run_checkpoint_observation`证据表：Run/command、generation、明确thread/ns/checkpoint/task/interrupt
关联、parent关联/集合摘要、已持久native观察类型及时间，幂等唯一键覆盖完整观察身份（不是只interrupt ID）。
该表是独立提交恢复桥，不是第二套checkpoint内容、并非复制SDK schema；证据记录不存消息正文或原始secret结果。
必要索引仅run+未结command协调及按Run生命周期清理。无外键、跨ownerSQL、新数据库/role、迁移或历史补值。
现Run/control字段与新表catalog必须在已裁决Agent4版本及桥证据准入后完整落唯一canonical SQL，并通过fresh/drift门。

source以(run_id, interaction_revision)作为稳定幂等身份；Run head、command动作结果及Chat事件必须共用同一数据库连接/事务，
分配source_index与Chat seq后一起commit，不通过另开连接的append_fenced伪造原子性。需要原Run→Chat锁顺序的相同SQL能力在
新窄adapter内承接；禁止事务里调用会另起事务的repository facade。通知在commit后发布已提交事实，不经普通emitter再造第二条source。
terminal在原finalize_terminal同事务分配独立且有序的交互清理source与最终terminal，重复调用重放原identity而不重增revision。

三段提交明确独立：
A. native已保存暂停，Run事务确认fence/定位后写等待集合＋同事务Chat source；A前crash从明确native事实补同一集合。
B. Run事务接受全集command＋resuming＋Chat source，commit后调native；B不是native成功。
C. 官方saver提交native writes/checkpoint后桥记录观察，当前owner核同链完整消费/稳定snapshot，再Run事务写active或新waiting＋动作结果＋Chat source。
C的native提交与桥/Chat不是原子；native已落桥缺失时通过官方aget_tuple/alist读取确切定位恢复。空记录不证明没执行，
graph lifecycle消息也不证明持久化。部分消费/新interrupt不能发空active；终态wins后旧观察只能留审计、不改head。

所有phase转换锁Run后再command，之后分配Chat source seq与append（沿既有Chat owner锁顺序，不反向先Chat再Run）；
消费端BFF采用其独立Conversation-first全局锁图，不跨库同事务。源writer与BFF cursor各有自己的原子边界，不混为一次分布式事务。
resume接受时验证原Run身份/当前pause；暂停状态可以无活跃expiry，必须专用paused→adopt current generation规则，
不能让任意已过期active lease借“paused”绕过。active转换核锁后当前有效authority，恢复转移明确绑定当前owner与原intent。

无法判定native是否已执行的窗口进入bounded reconciliation；只有可证明未执行才重投原决策，已消费则从该native链继续。
仅已失联且静止attempt判定超界保unknown证据并真实Run失败收口，绝不凭旧fingerprint相同重复执行；不承诺外部副作用exactly-once。
待结intent/观察引用阻该Run有关checkpoint先行GC；终态＋已结桥＋现source消费/retention条件允许后有界清理，不能永久保留。
完整4 scope.active/head/generation、checkpoint DAG/committed引用与Conversation最终释放仍后继，不在本片伪实现。

真PG故障矩阵见TECH：A/B/C各提交前后注入、原native写持久与桥缺失、部分task/子ns消费、unknown状态、同revision竞争、
锁后expiry/迟到generation、terminal竞争、源重复/分页/ACK丢失/重启。不得用默认pytest绿色或native内存saver代替真实提交证据。
本D0数据库未连接，当前PG43是P3A历史验收而非这些新矩阵。


### proof修订：attempt、证据与purge原子性

intent枚举候选固定accepted/dispatch_started/native_observed/reconciled/unknown/terminal。
accepted到dispatch_started是独立Run→command事务：唯一attempt identity、当时generation、原thread/ns/checkpoint/task组、
pre-resume向量长度/摘要和预期追加决策摘要一同commit，之后才native调用。无started且统一入口门成立才是确定未执行；
started后缺native写一律可能执行，不能通过清空started重新尝试。恢复证据不保存secret原值，私有decisions沿原命令权限管理。
observation仅记录委托saver成功提交后精确事实；NULL_TASK输入、task消费、task失败/新interrupt及因果successor分开，
历史pending_writes行残留不当最新结果。跨ns各自精确关联，无法归属原attempt时unknown，不按最近thread/latest猜测。
三次计数只对已失联且静止同attempt的成功读取判定持久累计；有效lease/tracked task、进展变化或读I/O失败不累计。
outer cancel可能丢task RESUME/ERROR，不能仅phase判native已消费。业务cancel的terminal由原事务权威决定。

52候选补现postgres_run_context.py的delete_run_rows，在原统一清理事务中删除新观察表，不旁建GC服务。
purge与observation均先锁Run；purge在锁后复验终态/retention/未结intent和引用，按children→Run删除；
late observer发现Run不存在零insert，不能创建孤儿或复活；observer先赢则purge必须看新引用再决定。
未结引用由上述恢复或权威terminal结清后按retention有界释放，不能永久豁免。native checkpoint最终DAG与Conversation释放仍独立门。
真PG新增测试只验证既有native checkpoint表的真实提交可见性，不创建候选业务表，不证明这些Run事务或purge已实现。

### R28 checkpoint写成功与值覆盖的区别

固定PostgresSaver对混合特殊/普通channel批次采用ON CONFLICT DO NOTHING，native RESUME的负slot也可能保留旧payload。
因此observation不得仅据aput_writes正常返回保存“全部新值已持久”的结论；原调用batch与独立读到的native事实是两类证据。
拟议intent预存的pre-resume向量/预期追加摘要仍须与实际持久事实核验；因果successor不替代缺失的attempt归属。
连续invalid、invalid、valid的PG exact-payload例只读现checkpoint表，不新增或修改native SQL；Root尚待执行本轮新例。
证据缺项进入unknown且不重调，不篡改保存的原intent或据最新输出补值；原Run terminal和有界retention规则保持。

## AGENT-P3B-D0-R26：第二阶段Run持久目标（2026-10-01；仅设计）

当前正式main `e977923ea9992cbddaf0cdbc6c8f8d23b3af120e`，HITL/HTTP4已发布；profile当前仅有assembly_recipe_bytes/fingerprint。
`af45817260478f1ee755d8e6e6963051e2049062`及真实PG43/43是P3A历史验收证据，不是本D0新增验证；当前SQL没有以下effective字段。本D0不改SQL/adapter，历史候选段不表示P3A仍待验。
第二阶段实施以TECH顶节明确observer、真实disarmed backend、Root依赖ADR与制品门通过为前提，不把静态配方重命名为effective。

拟在唯一canonical `database/schema.sql` 的现Run增加 `effective_native_policy_bytes BYTEA NULL`、
`effective_native_policy_digest TEXT NULL`，成对全NULL或全非NULL；非空bytes长度1..8388608、digest严格64小写hex。
CHECK同时要求effective非空时static两列非空；不要独立profile表/索引/外键。catalog drift逐列及CHECK精确验证；
fresh owner schema一次安装，拒旧schema，不迁移/补值/兼容。其他owner schema同库不影响本owner fresh检查。

表示唯一复用现canonical_json，domain_tag为 `kokoro-agent:effective-native-policy:1`，SHA256为该canonical UTF8 bytes摘要。
内容为所有peer及main/GP/catalog完整有序真实材料化政策，定义见TECH；不纳route revision/health/URL/凭据/运行对象，
不把动态执行输入预先当最终prompt，也不漏实际模板、工具归一化/override/exclusion、GP或middleware source与无secret选项。
两个阶段各存真实bytes与digest：完整profile是两阶段组合，不把static SHA当runtime_profile_digest。

目标 `freeze_or_verify_effective_policy(request, lease, static_binding, effective_binding)` 在现PostgresRunProfiles完成：
1. 校验输入canonical/domain/大小/digest与完整observation；全peer本地构造完后才调用，一次gate而非逐peer写。
2. 同连接transaction内tenant+run锁FOR UPDATE，锁返回后另取clock_timestamp；核原owner/generation、nonterminal、有效expiry。
   请求唯一比较 `request.model_dump_json().encode("utf-8")` 与stored原TEXT UTF8；不parse再序列化，不接受等价JSON。
3. 同锁核static已存在、内部digest正确并与本次static_binding逐byte相同；缺失/损坏/不等拒绝，第二阶段不补第一阶段。
4. effective已有则校验保存bytes/domain/digest并与本次逐byte比较，matched零UPDATE（updated_at不变）；任何差异typed拒绝，绝不覆盖。
5. 两列均NULL仅表示fresh Run尚未首次绑定。正常claim已RUNNING而持久counter/usage/sandbox均无执行事实可首次写；
   started/HITL/native已执行缺值是损坏。以真实现字段/记录判断，不只phase；未来native head存在也必须拒绝缺值。
   部分NULL、未知domain、非canonical、错误digest、越界大小拒绝；不以NULL当相等/空政策。
6. commit成功后才activation任何peer backend；网络/模型/connector不在SQL锁内。rollback/cancel零执行；ACK丢失重连后只verify。
   typed不相容携原build fence终态，authority失效零写/零终态，不借新generation。commit后的lease变化继续执行侧校验。

本片仅已有Run fence；没有scope表时不虚写scope已锁。后继完整scope/retry目标必须改为scope→有序dispatch→有序Run、核active_run/head/generation，
新retry从origin复制齐备两阶段，worker比较；父阶段缺失拒retry、不补父。native Checkpoint lineage、terminal原子committed晋升/释放、
retention DAG与Conversation最终引用释放门保持，不以永久免GC假闭环；这些目标未实施，不属于已发布HITL4的完成声明，后续若breaking按既定Agent5.0发布。
真PG矩阵：首次RUNNING零事实/已执行缺值、static缺损/不同、effective缺损/不同、exact request变体/跨tenant、迟到generation、
锁后过期、同异值两连接竞争、回滚/取消/ACK丢失与零UPDATE；沿现ownedDB精确PID rooted wait graph，包括soft queue，不增timeout。
历史P3A PG43通过不等于这些新增矩阵通过；实施后由Root在fresh隔离owner schema实测。无外部wire变更，canonical fresh DDL仍须部署门。


## 历史 AGENT-PROFILE-P3A-R25：Run静态绑定已编码、真PG待验（2026-10-01）

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
