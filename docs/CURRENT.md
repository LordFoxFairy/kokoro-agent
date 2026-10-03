## R122 实现与验收当前态（2026-10-02）

当前为Agent main `2653bcc723da5366fd877db73d701410e7bdc3a8` 之上的完整HTTP5/Todo、安全过程、安装资源与生命周期候选，待Root提交/发布；下方R104提案与R117 RED均为历史阶段，不覆盖本节。事实owner/模块、唯一canonical SQL与机器契约不变；distribution `2.0.0`、HTTP `5.0.0`、execution proof `1.0.0` 是不同身份。当前OpenAPI SHA256 `bca8e4f4fd613e4325f594266893d5b089168cf14f2ad7a7df03f3f116af85f2`；跨仓消费者必须随后固定正式owner commit/digest，不靠未发布源码或兼容协议。

Root E95实际1960纯节点全部通过/零skip、七静态门通过；E96当前源码新wheel/sdist与完整runtime安装、四布局64负向/219步骤通过；E97同包installed CLI真实PG首次安装/重复拒绝、canonical catalog与六漂移回滚通过；E98同包仓外安装态现HTTP acceptance实际36通过/0失败/0跳过（36 setup/call/teardown），151 loaded模块在collection/finish对应本次site-packages/RECORD。当前wheel SHA256 `4d0e7c8d1455fa395faaebc5e0de123f7131a31d02c413f04266ec67bad78fc3`；Root运行/manifest详见Root `docs/progress.md` E95–98。自有库精确回收/Redis测试区空/owned进程自然终态、371冻结路径保持；七固定测试工具按当前lock URL/hash单独供应，资源测试不出localhost。

这些是限定源码/安装/资源门，不是完整Agent或用户链闭环：真实console启动/SIGTERM、S3/Docker/E2B/custom/provider、完整retention、全部数据owner同库组合，以及正式BFF/Web过程消费与浏览器仍未验；T-Q03/T-R02继续开放。此四文档前缀只修正当前态，不改源码、SQL、协议、生成物、依赖或锁；发布前Root须重建当前最终文档集合的wheel/sdist并核与已测包的全部entry/RECORD一致性，差异则重新验证。

---

## R104：安装发布门真实 RED 与四文档候选（2026-10-02，当前）

Agent main `17c73541ae5d9f123d85cf531a79503df5c463bd` 加已冻结源/机器/测试，Root基线 `264de6b8`。本轮仅TECHNICAL_DESIGN/API_CONTRACT/DATA_MODEL/CURRENT增加本节；旧正文原字节保留，不改已冻结HTTP5/Todo/SQL语义，不是实现或发布完成。

- Root安装当前wheel SHA256 `230b41bc0038927f5ad5abf588c6ffa6f5fd22dbe7c78472fd810749e4692c25`：安装exit0；仓外cwd、Python -I、已安装distribution entrypoint及网络守卫下，checker exit1 / missing-installed-openapi、network_attempts0、临时target已删除。真实证据 `/tmp/kokoro-r104b-installed-checker-red.json/.log`；只读补充 `/tmp/kokoro-r104-agent-publish-gap.md`。本窗口未复跑。
- 推荐扩现setuptools data-files为只读contract/scripts/src审计树，以当前distribution元数据确定定位；审计Python副本必须匹配实际安装模块。与包内资源＋逻辑reader方案的比较、完整清单、无fallback与venv/target门已写三设计面，**待Root裁决/独立审，不授源码写权**。
- canonical OpenAPI/proof/SQL仍单一owner编辑；wheel资产仅派生只读。后继source inventory/digest变化须现generator机械生成并另授路径，不删checker、不放宽raw向量/生成/Platform/provenance门。distribution 2.0.0 / HTTP 5.0.0 / proof 1.0.0身份不混用。
- Root已报现HTTP acceptance `2884a4fb7d4ab04e7e4849105fd78a64327b6e7d0366df27333ff8383fd8e0f4` 真实36通过、typed代码独立审0；PG65和此前源树门保留其限定范围。原HTTP RED rawlog被覆盖的审计缺口仍保留，不用新报告伪补原日志；上述成功均不代替installed门。
- R105窄修正：独立审查指出DDL预检承诺与当前先连接/先ensure_schema实现不符，后继GREEN范围补入 `src/kokoro_agent/application/schema.py`，入口纯定位/读取/校验后才connect；现 `src/kokoro_agent/infrastructure/schema.py` 在ensure_schema前取得已验SQL并沿原事务执行，不新installer/API。后继扩现 `tests/unit/test_cli.py` 与 `tests/contract/test_canonical_database_schema.py` 证明初始缺失/漂移零connect/零schema创建及直接installer零ensure_schema/DDL，区分调用者既有连接。此次只纠正文档，未修实现，仍待Root独立复审后tests-only。
- 待验：最小安装回归/篡改负例、准确wheel/离线依赖隔离安装、installed CLI/完整checker、DDL fresh/重复拒绝/catalog、installed HTTP Todo组合与资源清理。tests-only→Root复现/审查→源码窄授权→新wheel各门；本轮不build/sync/install/import archive/运行测试或共享资源，不操作Git。
- retention已有terminal/interaction/Delivery GC部分实证，但bounded/native/Chat引用及正式保留政策仍未闭环；BFF/Web固定消费与完整用户链也未完成。Root统一审查、资源、集成/发布与台账；四docs冻结后本窗口停写。
- 本轮完整前后hash、原正文后缀保护及实际文档检查见 `/tmp/kokoro-r104-agent-installed-d0.json`（0600）；该manifest是文档交接，不是业务门通过。R105修正的前后hash与保护检查另见 `/tmp/kokoro-r105-agent-installed-d0-correction.json`（0600）。

---

## R90-W03：安全过程四文档 D0 闭集冻结（2026-10-02）

Agent main基线17c73541ae5d9f123d85cf531a79503df5c463bd；Root现R90/T-A01–06（任务卡已记030c6b89）。仅替换四份未提交R87前缀；TECH/API/DATA≤80行、CURRENT≤20行，原HEAD全文与370外围文件保护。
Root闭集已落：tool.execution/subagent.execution唯一display码、零truncated；Skill failed必须二选一error_code/其他phase禁止；Todo0..100项、1..1024 Unicode码点、完整payload确定UTF-8 JSON≤65536 bytes，孤立surrogate/缺失/超界整表拒绝。
API精确冻结C序列化与域分隔SHA256完整64小写hex/68字符ID；输入仅受信Run四元组及真实调用/segment身份。首durable resolving source_index作anchor；确认前零Skill I/O，真实重验新轮，不复用旧ready。
started在stage_critical_frame原Run锁内查询queued/published并返原StagedFrame/newly_staged=false，否则才分配；build成功initial invoke调用，resume不新增，删index==0资格。所有build/catch/caller/fake/现generator与两contract checker路径已列TECH。
ordinary progress同原lease emitter、先durable后live；持久化/失权不伪装Skill/assembly失败或第二terminal。私有诊断、完整HITL、成功Delivery、proof语义保留，Chat原identity/seq不改。
当前HTTP4/source/test/SQL/生成字节仍锁；目标Agent5未实现/发布。后继Root快审→tests-only真RED→实现/机器/生成/PG门→Agent5发布→BFF在direct/public6后固定消费/同水位snapshot→Web正规pin→Root整链。
Agent5仅fresh批准测试或正式批准数据处置cutover；不自动清用户数据/双读旧raw。Run purge未清Chat/Chat-BFF retention缺口保留为发布与整链门，不阻挡纯RED、不发明保留天数。
R80/R81 usage/v1独立后继、同writer串行，不占HTTP5、不把Billing/付款展示作为前置；不以过程事件充当计费证据。
剩余是实施/验证/发布门，不再将code/presence/容量/身份/started列待裁。本轮只源码定位与文档/字节保护；pytest/Ruff/Pyright/build/生成/HTTP/PG/浏览器/provider均0。
Root独占Git/index/提交/共享资源/台账；本窗口不向其他线程发消息。四SHA、原正文备份、外围保护与验证manifest交Root；冻结后停写，等待快审及精确tests-only卡。

---

## R80-W03：逐 actual call/attempt 严格 usage 设计交付（2026-10-02）

跨仓依据：[ADR-033：逐实际调用用量与 Billing 单一定价 owner](../../../docs/kokoro-handbook/decisions/ADR-033-actual-usage-and-pricing-ownership.md)。按 Root 已接受 owner 裁决同步；ADR独立审查不作为本仓实现验收。

状态：四文档D0候选，待Root/owner联审及后继机器契约；不是生产实现、schema发布或收费全链通过。基线 main `444684d32473c96ddbb70247081b1d1cdb8558f1`。本节与TECHNICAL_DESIGN/API_CONTRACT/DATA_MODEL的R80-W03前缀同步，旧全文完整保留；R71历史回归不转记为本卡测试通过。

### 已收敛设计

- Billing Metering/Credit是唯一采购费率、可配置销售倍率7/5、Credit换算/舍入、预占和结算owner；System仅planned模型/provider/route事实；Agent记录实际逐call/attempt严格usage，不算用户金额。
- 区分launch受信付款/消费授权上下文与Agent durable admission；逐attempt Billing admission在Agent准备call/attempt后取得，不是RunRequest提前必填，不虚构Run级预占，前次attempt许可不覆盖后续。launch若需新付款授权引用，先由IAM/Billing具名正式契约决定再列breaking写集。另区分logical call、provider attempt、原lease与planned System revision/digest、actual provider/model。gateway alias和单次HTTP不证明真实底层归属/内部重试。
- known_zero / known_nonzero / unknown与执行结果、actual attribution、结算状态正交；缺usage不补零，缓存/推理等子集不重复相加；重投保原事件、实际retry新attempt并先获授额度。
- provider前Billing预占/超预算重授权；并发额度由owner原子裁定。沿官方SDK和原唯一Run终态事务，新增模型attempt journal/append-only evidence/usage outbox目标；unknown跨取消、HITL、takeover保留，恢复不再次自动推理。
- 统一发布顺序：共同冻结语义 → Agent strict evidence producer artifact先发布 → Billing固定消费该artifact并发布逐attempt admission/证据接收contract → Agent固定消费Billing → 必要BFF消费者切换。纯artifact不依赖运行服务已经启动；语义协作不等于循环等待对方先发布。
- 精确现/新源码候选、contract与纯/PG RED矩阵、无旧数据兼容及GC引用条件已写入三面。不增第二executor或System成本事实。

### 独立审查退回与定点修订

Root接收上一候选独立Astra结论0P0/1P1/1P2：四旧正文4/4精确保留，但launch提前要求逐attempt admission造成生命周期冲突，双方artifact发布顺序倒置。本轮按Root同卡裁决修正上述两项；此前外围370项只属worker自检，独立审查员未取得可复核manifest，不声称已获独立外围验证。修后仍待Root/Astra复审，不沿用旧候选通过结论。

### 本轮事实核对与保护

实际只读检查现RunRequest、System client/factory、SDK callback聚合、TokenBudgetMiddleware、Run usage segment、原finalize_terminal、tool journal及现测试入口。当前仍只有generation粒度input/output汇总；缺少本D0目标持久事实与Billing admission，不将设计写成实现。

本轮只允许四现文档新前缀；开始时记录374个非缓存工作文件的SHA-256/长度（排除.git、.venv、__pycache__及测试/构建缓存），其中四目标原字节：
- TECHNICAL_DESIGN：286562 bytes，`1f8a2a5852397231b6e50608729141ea644bbc073fd20b689864afd5e8b16db9`
- API_CONTRACT：108240 bytes，`8880166791e7bb670e3887017e5e3080c67662982d3fb2cfeae2c273f088e103`
- DATA_MODEL：112829 bytes，`883771d74ee913011d197a3ef1971e17564b56f9d2305b05bf4e99bdebe581a3`
- CURRENT：127463 bytes，`790f74555dd498abc49a92a191013e7dba5b012311a341818145a054ac42282a`

交接执行纯文档断言：四目标后缀与原长度/SHA相等、其余370文件SHA及文件集合保持、HEAD与Git index摘要保持、四前缀R80标记/必要语义/Markdown围栏/后继现路径核对。上一候选worker文档检查为79项通过/0失败（含35个现源码/测试路径存在性），不是本修订的独立审查结论。本修订重新核对四原正文及370外围实际digest，完整清单与新前缀/整文件SHA、文件集合和HEAD/index检查写入 `/tmp/kokoro-agent-r80-w03-d0-repair-20261002/manifest.json`。Root可据此逐文件复核，实际命令及结果见交接；这些是文档/范围断言，不是业务测试。

本轮pytest/Ruff/Pyright/build/schema/真实provider/浏览器/PG/Redis/ObjectStore运行数均0；原因是授权仅文档前缀、source/SQL/wire只读且共享资源归Root。没有写新计划中心、Root台账或其他仓文件，也没有Git命令/index/commit操作。

### 具体未完成与后续owner

1. Agent原WIN03先发布strict evidence producer artifact；Billing原WIN06固定消费后发布逐attempt预占/增额/接收/查询/unknown恢复机器契约，Agent再固定消费，必要时BFF后继切换；不是现feature quantity=1/调用方actualMicros的临时接线。
2. System原WIN05与Root：明确planned binding消费以及真实gateway执行证据owner；actual归属/内部attempt证据缺失仍阻正式收费profile，Agent不以planned填空。
3. Agent原WIN03：Root联审后按共同冻结语义及上述producer先行顺序推进机器契约/SQL门与行为RED，再授权精确源码集；模型journal/终态后reconciler和outbox仅设计，未实现。
4. 用户待答的失败/取消收费规则继续待决；不阻真实成本/证据基础研发，不默认扣费或免费。
5. 后继文档清理：现README仍写“按provider accepted invocation次数、非token计费”；该历史入口在本卡四文件写集外，原字节保持。正式实现切片需同步README/INDEX相关入口，不把它作为R80目标依据。
6. Root：统一独立审查、主工作树复验、发布顺序和同卡台账/Git；本窗口freeze后停止写入。

---

## R71：R70-03 内存执行回归收口证据（2026-10-02）

本节仅记录 Agent 既有两份执行单测的 EOF 回归，源基线为
`d131c3f4f61b46ed1cca1b8a44fe18e42c8522de`。没有生产源码、机器契约、Schema、依赖或 P3B 变更；
下方原正文完整保留。本切片待 Root 最终三路径审查、index 与提交，不表示整体 HITL 或完整用户链完成。

### 回归范围与证据边界

- Live stale delivery 使用继承正式 `_consume_control_frame` / `_apply_recorded_control` 的窄测试入口：
  旧 revision 被拒绝，原 Run、waiting pause 与 lease 保持，native 零执行；后续 bookkeeping、重复 delivery
  与 admission replay 保留原 `failed / interaction_conflict` 回执，不发 applied 回执。
- Mixed batch 使用正式 checkpoint bridge、官方 `InMemorySaver` 与实际 SDK root/child graph，
  三组四项（两 tool approvals、一 result review、一 input）完整映射后实际消费一次。
  独立 memory reader 得到 `ConsumedPauseEvidence`，核 command、attempt/generation、原 pause revision/ref/digest；
  保存 observation 后 reconcile 为 active，重复 start、accept 与 bridge replay 不再授予 dispatch 权限，native effects 保持三项。
- 以上 repository 与 saver backing 均为内存测试；独立 reader 不等于独立 PostgreSQL 持久证据，
  不证明外部副作用 exactly-once，也不扩大 P3B、scope、retry、retention 等原未决范围。

### 实际验证归属

- **Root 主工作树复验**：Root 在 R71 放行消息确认原句柄 `51399` 已完成两份完整测试文件，
  **52 passed / 0 failed，0.66s**；这与 worker 同一测试集合，不重复累计。
- **独立审查**：Root 确认 native Sol 对 R70 冻结测试切片审查 **0 P0 / 0 P1 / 0 P2**；
  此结论不替代本次 CURRENT 前缀的最终三路径审查。
- **Worker R70 定点检查**：新两例 **2 passed / 50 deselected**；两份完整文件 **52 passed，0.56s**；
  Ruff `format --check --no-cache`、`check --no-cache` 均 exit 0；定点 Pyright **0 errors / 0 warnings**。
  精确命令、输出与 SHA 记录在 `/tmp/kokoro-agent-r70-win03/freeze.json`，
  日志为同目录 `new-tests-final.log`、`full-files-final.log`、`ruff-format-final.log`、`ruff-check-final.log`、`pyright-final.log`。
  首次新增测试对象引用及类型收窄错误仅在追加段修正，不计业务 RED，不改旧断言或检查规则。
- **本轮未执行**：PostgreSQL、Redis、provider、browser 与完整用户链验收；不以以上内存纯门覆盖这些门。
  R71 只追加本节并检查字节保护，不重跑或推算全仓 pure/build/contract/resource 结果，不操作 Git、服务或共享数据。

### 冻结测试字节

| 文件 | R70 完整 SHA-256（R71 保持） |
|---|---|
| `tests/unit/execution/test_control_commands.py` | `21ce2359e9903ccb2daae60a3eef3ca0eab8266b29728bf8e75492fd4d2da23b` |
| `tests/unit/execution/test_hitl.py` | `5b5d0ccd48ee4d908a9105a9bddb2bd4f4a574d764b21b4bf56080276a243bc1` |

---

## Root R43：本 HITL/HTTP4 切片验收证据（2026-10-01）

本提交仅收敛完整66路径的HITL/HTTP4业务切片，不表示全Agent外部能力或Wave0–7闭环。Root冻结复验：Ruff267文件format/check通过、Pyright0 errors；contract/check及failure生成检查通过；完整pure1799passed/6skipped/288deselected（96.81s，364 warnings），uv lock --check与wheel/sdist build通过。真实owner资源：完整database196passed/0skip；HTTP4邻接45PG+35HTTP=80passed；unit资源18passed/1356deselected（3.32s，1034 warnings）。每轮自有临时库created/closed=true，unit Redis15 reservation后仅精确删除本次17stream、remaining0；无清理共享数据。官方saver的persisted list[Interrupt]严格验证后与snapshot tuple比较，root→child精确因果/locator/完整ID与value、approval0→1/review1→1及终态保持。日志 /tmp/kokoro-agent-r43-root-final-pure.log、/tmp/kokoro-agent-hitl-r41-root-all-database-r2.log、/tmp/kokoro-agent-http4-r40-root-real-pg-http-r2.log、/tmp/kokoro-agent-r42-root-unit-resources-r4.log。

BFF消费者仍待固定本提交的4.0.0机器artifact后更新正式projection/resume；Web、正式收费、真实provider/浏览器完整旅程尚未通过，不宣称完成。原P3B工作树候选与两native proof保留不纳入此提交；四docs仅批准HITL前缀，HEAD历史body保持。外部MCP/Storage等资源不由以上pure/PG门替代。

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

### 实际过程与待验

- Root R38 复验：45 PG/0skip；全 pure 37failed/1722passed/6skip/287deselect，Ruff267/0、Pyright0；旧37机器/proof不是已绿。
- R39 receipt 首 RED 5failed/41passed；补错 kind 的 steer 字段后 RED 7failed/44passed（0.71s），日志 `/tmp/kokoro-agent-http4-r39-receipt-red-r2.log`。
- checker strict required-fields RED 6failed/34deselect，日志 `/tmp/kokoro-agent-http4-r39-checker-red.log`；均行为断言失败、非 import/collection。
- 当前源/机器已切候选4，并通过真实 failure generator 更新完整 source hash/provenance；生成的 Failure tuple/body不改。
- 新 unit fixture 的 launch 未 claim 导致2个404，以及 bus omit-null 与持久 typed body不同导致2个断言失败均保留日志；已按真实 admission→claim 与 bus序列化规则修测试，未改生产 scope/fence。
- 全 pure 实跑 **1failed/1798passed/6skip/288deselect/364warnings，97.93s**，日志 `/tmp/kokoro-agent-http4-r39-full-pure.log`；旧37收敛，唯一旧 JWKS 整体 version3 被前置遮住。R40 Root只准同文件3处整体HTTP版本更新，非 proof 放宽。
- 静态实际 Ruff267/0、Pyright0、contract-check0、failure generator --check0；资源 **80 collected**（45事务PG＋35HTTP），未执行，日志 `/tmp/kokoro-agent-http4-r40-static.log`、`/tmp/kokoro-agent-http4-r39-resource-collect.log`。
- R40 首次定点1failed/262passed（1.42s）到达同文件 direct HTTP provenance version3，已按Root单点授权同改4；最终实际定点 **263passed（1.37s），exit0**，连同 fresh Ruff267/0、Pyright0、contract0、generator --check0 见 `/tmp/kokoro-agent-http4-r40-final-gates.log`。不重跑一次全suite推算全绿，完整纯门交Root fresh复验。
- worker未访问真实HTTP/PG/Redis/provider，无服务/Git操作。
- Root 接收并重跑资源前不称 HTTP4 已发布；整体 scope、跨 takeover 生命周期、effective native/P3B 与最终 GC仍保持原未决。

---

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

### 当前门禁及未完成

worker未成功连接PG/Redis/provider、未启动服务或操作Git；上述一次Redis连接尝试失误明确保留。Root接收冻结文件后独占实际44 PG及相邻回归；
任何catalog/fixture/SDK后段实际失败原样记录，不把18首port/table RED之后未到达断言算通过。
当前候选未满足完整default/typecheck/contract；P3B/fork、scope/retry/native-retention及Conversation引用最终释放仍各自原门。
日志集中`/tmp/kokoro-agent-bridge-green-r35-*`，最终manifest另交Root，当前尚未提交。


Root已验R3事务26/26（2.51s）、相关四PG文件71/71（5.56s），资源均回收；它们不证明正式桥。
Root fresh default实际78 failed/1664 passed/6 skip/268 deselected（97.96s），Pyright仍43错误；
失败分布supervisor26、machine_contract16、chat_response14、hitl11、public_contract6、control_commands4、execution_proof_artifact1。
日志`/tmp/kokoro-agent-hitl-p2-r34-root-{related-pg,default}.log`。本轮仅四HITL前缀；生产、测试、机器与P3B整suffix保护。
以下精确决定覆盖下方历史D0的未定字段/方法；既有P2事务语义保持，新增桥/schema/完整4均待源码卡和真实门。

本卡只补已裁决三处实施接口：观察表精确SQL/幂等identity与Run-first GC；ConsumedPauseEvidence/port及
健康与静止计数；native_observed→reconciled和完整action_result/source原子映射。没有新源码/SQL/test/机器修改。
初pause无command/attempt为合法NULL；received批次不当持久观察；缺证据unknown零重投。
官方公共saver装饰器和现worker/main接线，不加fork/库/依赖/P3B。新桥文件仍仅候选，未创建。
现P2未实现观察表、source action_result、native_observed或任何全入口许可；历史PG绿色不可推出这些能力完成。
后续仅待Root四前缀三面审查后，另卡授权四现tests先RED，再按真实失败授权生产，不一次放全部52。
无Git/DB/Redis/provider/服务访问；无worker测试进程。Root默认全门已返回上述实际RED，未去修改冻结消费者。
未决Conversation最终引用释放仍只阻最终DAG/GC发布；不阻本桥独立实施。任意不完整native归属保unknown，
不将无自动恢复保证表述为需要预先等待fork。完整4发布和Root真实native桥故障矩阵均未完成。

最终离线日志：`/tmp/kokoro-agent-hitl-p2-core-pure-final-r2.log`（157/0）、

R35 RED期间Root精确批准两个native入口补名及第7只读read_resume_context；四docs仅此增量，生产仍未授。
它复用原Run/command列，无新SQL事实；恢复分支无Command/新attempt，原P3B及其余历史内容保持。
纯14 RED已到达（0.80s）：source action_result七例、六原port缺失、安全validation一例；初次测试误假定checkpoint变化已纠正并保日志，
实际invalid→invalid→valid完成一次效果、同nativeID和checkpoint保留。新四test正在按批准接口继续；不是bridge通过。

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


`/tmp/kokoro-agent-hitl-p2-core-collect-final-r2.log`（22 collect）、
`/tmp/kokoro-agent-hitl-p2-core-static-final.log`（Ruff）、
`/tmp/kokoro-agent-hitl-p2-core-pyright-final.log`（43个后继旧消费者错误）、
`/tmp/kokoro-agent-hitl-p2-core-contract-red.log`（30 fail/26 pass）。
新增source已纳现显式manifest（235 resource registrations/115 distributions/12 dynamic edges）；
测试实际读取新domain/infra所属package资源并验证descriptor，未构建/安装wheel，不冒称本轮wheel门通过。
Root独占临时PG后运行：`KOKORO_AGENT_DATABASE_URL=<owned-url> uv run --frozen --offline --no-sync pytest tests/integration/database/test_run_interaction_transactions.py -o addopts='' -q`。
22例中fresh catalog与精确pg_get_constraintdef、真实触发器回滚、两连接竞争/锁后clock及terminal/purge均待此实际门；
worker只collect。默认全链/机器生成发布/native8复跑/外部资源/wheel未运行，禁止由本局部候选推断完成。
当前没有运行中的测试句柄；全部生产、测试、四docs以最终manifest冻结交Root复验。

## AGENT-HITL-P2-RED-R31：首批契约与事务RED（tests-only）

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

基线main0245a36＋P2-D0冻结57140f75；WIN03独立审0P0/0P1/2P2。唯一授权现两contract测试、
新普通tests/integration/database/test_run_interaction_transactions.py及本四docs批准HITL前缀；生产/机器/SQL/SDK不改。
时间列候选明确TIMESTAMPTZ(3)；后继infrastructure/checkpoint_interactions.py是尚不存在的新增普通文件，
只列后继，不在本卡创建桥。P3B整suffix保持。

实际单进程纯contract首RED：32 failed /24 passed（1.01s），pytest实际exit1；
日志/tmp/kokoro-agent-hitl-p2-red-r31-contract.log。失败对应仍为3版本、缺revision/ref/item schema、
旧decision仍接受、缺interaction.state枚举/decoded mapping；非导入或collection失败。
相邻正例前置尚失败时，其后negative/runtime分支未执行，不冒称全部负例或HTTP实际路径已测完。
真实PG未运行，首六例只收集待Root执行；静态/收集最终结果冻结前追加。36生产及22后继依赖仍未授权。

R31最终冻结证据：纯contract重跑 **32 failed/24 passed（0.97s），实际pytest exit1**，
/tmp/kokoro-agent-hitl-p2-red-r31-contract-r2.log；新PG文件 **6 tests collected（0.03s），exit0**，
/tmp/kokoro-agent-hitl-p2-red-r31-collect-r2.log。真实PG尚未执行，expected首RED是现真实Repository缺record_pause/accept_resume的测试体断言，
不是已证明五处rollback失败；待生产能力落位后同六例继续跑真实边界，不删后续断言。
定点三test Ruff format/check通过，Pyright **0 errors/0 warnings**，/tmp/kokoro-agent-hitl-p2-red-r31-static-r2.log；
初次Pyright7 errors（私有helper引用/未标注嵌套fixture）保留/tmp/kokoro-agent-hitl-p2-red-r31-static.log，
改为公开AgentIngress.control与显式测试类型，未加ignore或改生产。唯一通知替身为FakeBus，Repository/Schema/Chat持久事实均真实PG；
无Redis/provider/native推理，纯contract也未启动服务。原native八例未改。现所有执行句柄已结束。

Root真实RED入口（仅Root自有资源）：
`KOKORO_AGENT_DATABASE_URL=<owned URL> PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync pytest -o addopts='' -m integration tests/integration/database/test_run_interaction_transactions.py -q`。
现fixture先真实安装schema及try_claim再assert能力，PostgreSQL不可达fail-loud不skip；只清本例唯一schema。
本轮manifest /tmp/kokoro-agent-hitl-p2-red-r31-manifest.json绑定三tests＋四docs；冻结后停写，Root真PG/独立审后才授权必要生产子集。

## AGENT-HITL-PERSIST-P2-D0-R29：仅四docs设计冻结准备

Root已提交推送main `0245a36c85422b4e0e85cc22aba426b0e30fec12`，纯DOMAIN-P1已验收，下面P1“候选待验”段保历史。
本轮唯一writer只四docs批准HITL prefix；P3B四整suffix、源码/SQL/机器/tests/依赖原样，未使用Git/DB/Redis/provider/services。
Root采纳零新表先Run/command/Chat事务核、三事务与started_now单次许可、purge Run-first及完整typed4候选前置。
本D0已精确列两表列/Row/codec/CHECK/唯一索引、五port方法和36路径后继候选（2新普通文件；没有创建）。
新事务PG测试拟放tests/integration/database/test_run_interaction_transactions.py，不混入原八native证明。

当前机器仍3.0，canonical SQL尚无交互列；完整native bridge/active消费证据、所有worker入口、真实事务PG与HITL4发布均未实施。
本次只文档一致性/现contract检查；实际命令结果冻结前追加，不把原1695/default、native8或P3A PG43说成本P2事务通过。
未决是Root对本36精确源码集/完整candidate4的实施放行、真实PG结果及后继桥证据门，不重新讨论已定三事务/零新表顺序。
Conversation最终释放只阻最终DAG/GC发布，不阻P2；P3B fork/依赖仍未批准。完成后停写，Root独立审查后另卡TDD。

实际验证（本D0）：`PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync kokoro-agent-contract-check`
→ `kokoro-agent contract: ok`，exit0，日志`/tmp/kokoro-agent-hitl-p2-d0-contract.log`；仅现3.0未漂移。
四P3B后缀4/4 byte相等、Markdown fence/尾空白检查通过；36先行候选去重且唯二新普通文件未创建。
源码/SQL/tests/contract保护核验只见Root已声明的ignored egg-info/SOURCES.txt构建更新，无本轮生产变更。
Root已裁定协议/native真实消费者切换属于同一完整HITL owner交付，P2先行TDD不等于36可独立发布完整4；
TECH补22实际扩展依赖及各批RED/退出门，Root另卡不一次授权。保全部旧失败，不为局部default绿加兼容。
本轮没有运行新的pytest/PG/build/provider，无运行句柄。四whole与P3B suffix manifest冻结后停写，独立审查/源码授权仍待Root。

## R29 Root 纯规则验收（仅本切片）

绑定冻结manifest8f684dc01f7c6de59163fdcc81f009cf7690a3f85d841ea95b466a4a876e0b78。
Root fresh `ruff format --check .`265/check/Pyright0，默认pytest **1695 passed/6既定skip/242 deselected/364 warnings（94.62s）**，
日志 /tmp/kokoro-agent-domain-p1-r29-root-default.log；当前3.0 contract-check通过。
`uv build --offline`退出0，wheel/sdist构建成功（/tmp/kokoro-agent-domain-p1-r29-root-build.log）。
构建只更新ignored egg-info/SOURCES.txt以纳入新文件；331/332原保护hash保持，唯一此自产构建清单变化，不暂存构建物。
独立Astra0P0/0P1/0P2，六候选/四P3B hash通过。仅纯domain接收，未运行PG/Redis/provider，
持久start/Run→command→Chat/fence/native恢复/GC与4.0发布均不在本片完成范围。

## AGENT-HITL-DOMAIN-P1-R28：纯规则候选冻结待Root验收

基线main `512a8462c52acfc1d87ee20c71bccea04ce63b98`（Root提供、worker不操作Git）。
本轮新增domain/run/interactions.py与tests/unit/execution/test_interactions.py，加现四docs批准HITL prefix；
完整P3B未提交suffix保持，332个既有src/tests/contract/database及manifest/lock保护文件hash全同。

已实现纯不可变group/item/decision/submission/intent/head：原分组顺序、全集校验、精确payload bytes重放与冲突、
同ID validation新pause、不可逆start、unknown不重投、terminal吸收。无SDK/DB/Redis/wire/worker引用，域文件只有stdlib导入。
允许历史command持久owner先加载原intent后verify_replay；当前head不累积无限ledger。当前没有native证明判定器或active解除API。

真实RED保留：
- tests-only首次运行因新域模块尚不存在，collection 1 error，`/tmp/kokoro-agent-hitl-domain-p1-red.log`。
- 追加非法重建revision规则时实际 **2 failed/36 passed**：future intent、resuming缺accept revision；
  `/tmp/kokoro-agent-hitl-domain-p1-red-invariants.log`。补纯不变量后38例全绿。
- architecture实际 **1 failed/64 passed**（源码导入cast违规），`/tmp/kokoro-agent-hitl-domain-p1-gates-r2.log`；
  已删除cast，通过实类型tuple参数保留runtime校验，无ignore/门禁配置放宽。Pyright初2→1→0亦保日志。

定点最终门 `/tmp/kokoro-agent-hitl-domain-p1-gates-r3.log` 整链exit0：
`uv run --frozen --offline --no-sync ruff format --check src/kokoro_agent/domain/run/interactions.py tests/unit/execution/test_interactions.py`；
同两路径ruff check、pyright均通过（0 errors/0 warnings）；
`PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync pytest tests/unit/execution/test_interactions.py tests/contract/test_architecture.py -q`
→ **65 passed（1.47s；38规则＋27架构）**；当前3.0 `kokoro-agent-contract-check` → ok。

默认全仓离线门 `/tmp/kokoro-agent-hitl-domain-p1-default.log` 整链exit0、原session94122已完成：
`uv run --frozen --offline --no-sync ruff format --check .` →265 files；`ruff check .` →All checks passed；
`pyright` →0 errors/0 warnings；`PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync pytest -q`
→ **1695 passed/6既定skipped/242 deselected/364 warnings（90.96s）**。原SDK warning与测试loopback request异常诊断原样保留，未隐瞒或放宽门。
未运行真实PG/Redis/provider/wheel/build；没有启动或清理共享基础设施。此纯切片不新增持久/schema，Root后继真PG门另卡。

六文件manifest `/tmp/kokoro-agent-hitl-domain-p1-manifest.json`；当前候选纯规则尚待Root独立审查/重跑/提交。
Run→command→Chat同事务、durable start及锁后fence、native精确观察/unknown恢复、terminal/GC竞争、机器4.0与consumer pin均未完成。
完整scope/retry/effective-native/retention目标保持；本片不是52完整实现或HITL4发布。

Root追加默认pytest实跑：**1657 passed /6既定skipped /242 deselected /364 warnings（90.67s）**，
/tmp/kokoro-agent-hitl-proof-r28-r4-root-default.log；测试HTTP取消路径有既有服务端stderr，pytest仍exit0，
LangChain beta/deprecation warnings如实保留，不屏蔽。此默认门不替代PG；PG8单独真实通过。

## R28 Root native 证据与 HITL D0 验收（生产实现未完成）

冻结33c8b20e六文件经Root6hash/4P3B后缀与独立26保护核验；独立Astra最终0P0/0P1/0P2。
Root真实PG全部 **8 passed /0 skipped（0.51s）**，/tmp/kokoro-agent-hitl-proof-r28-r4-root-real-pg.log，
自有agent_terminal_atomic_97ebd2406c704f6d已回收；原5/3和1/7 observer RED完整保留。
Root定点Ruff format/check、Pyright0、contract19 passed（0.40s）、当前3.0 contract-check通过。
当前native证明只是输入/消费/PG写slot与因果单节点边界，不是生产Run恢复、外部效果exactly-once或HTTP4发布。
四docs只接收批准HITL设计与证明prefix，工作树未批准P3B整后缀留存而不暂存；不授权fork/依赖变更。
后继只放最小纯domain状态规则与测试，SQL/worker/完整52/机器4/消费者发布仍独立待门。

## AGENT-HITL-PROOF-R28-R4：仅观察定位投影返修（2026-10-01）

Root R3真PG **1 failed/7 passed（0.54s）**，日志`/tmp/kokoro-agent-hitl-proof-r28-r3-root-real-pg.log`；
新增重复validation精确向量case已通过，原multi-interrupt map子graph仍interrupt而主second完成，Root正在定位，未宣称8绿。
纯native定点只读探针确认子graph saver config含AsyncCallbackManager、Runtime、Saver自身、PregelScratchpad与send/read/call闭包。
完整deepcopy越过只观察定位的职责；它是否唯一导致此次partial-map仍待Root真实复验，不把相关性冒充根因。
按精确授权仅改observer为thread_id/checkpoint_ns/checkpoint_id三个str标量投影；原config原样委托native，
payload仍在任何await前复制，未改调度/等待屏障/最终map结果，也未单独再投child决策。
Root独立审查进一步定位：完整config复制可触发SDK对象复制异常，child GraphInterrupt退出路径可掩盖后台saver异常，
缺pending_writes时scratchpad又不应用resume_map，符合child重问/root成功现象。此源代码因果链不代替新一轮真PG结果。
按Root补授权记录有限received/delegated/exception stages，异常原样raise；值副本仅测试JSON，不复制SDK对象。
原并行map测试在初始pause后立即检查零observer异常，并在resume前通过独立connection、精确locator/task/interrupt ID
证明root和child暂停都已commit，再且仅再执行一次全集map。未加sleep/重试/child补投，最终精确结果不变。
除本CURRENT和PG test外其余四hash保持。真实PG8等待Root单次验证；若仍失败保留RED，另报原生提交/map调查。
本轮静态日志`/tmp/kokoro-agent-hitl-proof-r28-r4-static.log`整链exit0：Ruff格式/check、Pyright0、
contract文件19 passed（0.41s）、PG8 collected（0.02s）、当前contract: ok；六hash见`/tmp/kokoro-agent-hitl-proof-r28-r4-manifest.json`。
P3B后缀/26保护文件保持，生产52及新native接口未授权。

## AGENT-HITL-PROOF-R28-R3：测试observer窄返修（2026-10-01）

Root实际PG8前轮 **5 failed/3 passed（0.82s）**，日志`/tmp/kokoro-agent-hitl-proof-r28-r2-root-real-pg.log`，
自有DB已回收。直接原因是本测试observer把NULL_TASK的标量RESUME也assert为list；这改变了native执行，
该RED不构成混合batch持久语义结论。原日志完整保留，不以新静态绿覆盖。
本轮只修PG observer与本CURRENT：在任何await之前deepcopy收到的config/RESUME，明确分开NULL_TASK input与task列表，
不在observer做会抛出的类型断言；委托原saver后只追加收到参数快照，绝不冒充持久值。新增精确scalar/vector对照断言。
invalid→invalid→valid的完整委托三项向量、独立连接旧两项向量、因果successor及零重调断言全部保留。
其余四冻结文件未改，生产/SQL/机器/P3B后缀/26保护文件均不动；Root独立审查须绑定R3新manifest而非0ff5546d。
静态与收集日志`/tmp/kokoro-agent-hitl-proof-r28-r3-static.log`整链exit0：Ruff格式/check、Pyright0 errors，
contract文件19 passed（0.40s）、PG8 collected（0.02s）、当前contract: ok。真实PG8等待Root重跑，没有worker DB连接。
新六hash见`/tmp/kokoro-agent-hitl-proof-r28-r3-manifest.json`；此前原7绿与本次observer RED严格分别记载。

## AGENT-HITL-PROOF-R28-PG-VALIDATION：精确向量证明候选（2026-10-01）

正式基线仍main `af45817260478f1ee755d8e6e6963051e2049062`；输入六文件manifest `b34be947`。
本轮仅原六文件，原P3B后缀/26生产等保护hash不变，生产52未获授权。没有连接DB、provider、Redis或操作Git。
Root上轮真实PG已7 passed/0skip（0.45s），自有DB已回收；已读取实际日志
`/tmp/kokoro-agent-hitl-proof-r28-root-real-pg.log`。下方R27待验叙述仅历史，本轮新增第8例仍未真运行。

源码确认PostgresSaver混合batch采用INSERT DO NOTHING，区别于InMemorySaver覆盖负slot。
新增连续同值invalid→invalid→valid测试：每轮独立连接检查确切payload/同ID，记录真实提交batch，
丢弃observation后读取原checkpoint和直接successor；精确预期旧slot两项invalid且最终输出valid，待Root核事实。
任何不符保留真实RED，不把不同实际值改成“存在RESUME”放宽；没有预先声称PG新例通过。
纯内存对照实际完整三项向量，1 passed/18 deselected（0.11s），日志`/tmp/kokoro-agent-hitl-proof-r28-pure.log`。
没有制造业务RED→GREEN；本轮Pyright初测2 errors、修类型边界后0，原日志保留。

最终静态日志`/tmp/kokoro-agent-hitl-proof-r28-static-r2.log`整链exit0：两tests Ruff format/check通过、
Pyright0 errors/0 warnings、整contract文件19 passed（0.41s），真实PG文件仅8 collected（0.02s）。
命令均`uv run --frozen --offline --no-sync`，pytest加`PYTHONDONTWRITEBYTECODE=1`；原完整命令同R27，
PG仅`pytest tests/integration/database/test_run_interactions.py --collect-only -q -o addopts=''`，未执行fixture。
Root新例入口（显式自有KOKORO_AGENT_DATABASE_URL）：
`PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync pytest tests/integration/database/test_run_interactions.py -k repeated_validation -q -o addopts=''`。
随后Root可同文件全8例复跑；不共享清理或扩大timeout。当前3.0 contract-check另见`/tmp/kokoro-agent-hitl-proof-r28-contract.log`。

设计结论仍保守：旧向量＋成功successor不足以补齐精确attempt消费关联；缺证据unknown、零再次执行、不猜active。
现公共saver不因此被宣称能自动恢复所有观察丢失场景；未新增native接口/依赖。进程重启、Run事务、外部效果、
完整生产recovery/GC与HITL发布均未证明。六hash/保护检查见`/tmp/kokoro-agent-hitl-proof-r28-manifest.json`。

## 历史 AGENT-HITL-PROOF-R27：恢复桥可行性证据，生产未授权（2026-10-01）

准确正式基线：`af45817260478f1ee755d8e6e6963051e2049062`；输入四docs冻结manifest `8c6ed31f`。
本轮只四docs＋现tests/contract/test_deepagents.py＋新普通tests/integration/database/test_run_interactions.py，
未写生产/SQL/机器/依赖/安装目录/Git；工作树未提交P3B后缀保留，fork/依赖/source接口仍未批准。

设计补正：NULL_TASK输入非消费，map可无此写；task RESUME先内存后后台保存，RESUME+本次ERROR/INTERRUPT非成功；
历史INTERRUPT行在成功resume后仍可能残留，必须精确intent向量与因果successor的当前快照消歧。
accepted与dispatch_started分事务，统一native前commit started；started后缺证据unknown不盲投。
三读失败仅限已失联且静止attempt，不误杀当前有效lease/健康长task；晚到observation与purge同锁Run、复验引用后children→Run删除。
TECH后继52候选补现postgres_run_context.py统一清理落点（原47现有/5拟新；本轮PG test创建后48已存在/4待新建），仍非生产授权。

实际纯native证明（无provider）：完整map、标量NULL_TASK与task消费区别、同业务/native ID validation重问、子namespace、
RESUME+ERROR/INTERRUPT、三次读取仍在执行、成功后旧INTERRUPT残留。新增7例通过，原整文件18 passed（0.40s）。
新增取消探针真实RED：期望取消必有task RESUME时 **1 failed/6 passed**（0.21s），日志
`/tmp/kokoro-agent-hitl-proof-pure-r2.log`；实际只剩旧INTERRUPT，修正为缺证据unknown断言后7 passed（0.13s），
`/tmp/kokoro-agent-hitl-proof-pure-r3.log`。这是原生假设的反例/修正，不是生产bug RED→GREEN。
类型首轮29 errors→修正native节点参数名与明确SDK overload边界后0 errors，无新增ignore/门禁放宽。

最终实际命令日志 `/tmp/kokoro-agent-hitl-proof-final.log`，整链exit0：
- `uv run --frozen --offline --no-sync ruff format --check tests/contract/test_deepagents.py tests/integration/database/test_run_interactions.py` → 2 files already formatted。
- 同两路径`ruff check` → All checks passed；同两路径`pyright` → 0 errors/0 warnings。
- `PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync pytest tests/contract/test_deepagents.py -q` → 18 passed。
- `PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync pytest tests/integration/database/test_run_interactions.py --collect-only -q -o addopts=''` → 7 collected，未执行fixture/连接数据库。
- `PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync kokoro-agent-contract-check` → contract: ok（仅当前3.0未漂移）。

Root真PG入口：在Root自有临时数据库设置非空KOKORO_AGENT_DATABASE_URL后，于本仓执行
`PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync pytest tests/integration/database/test_run_interactions.py -q -o addopts=''`。
预计7例：健康node/写提交前barrier、成功观察丢失/error/reask/cancel、跨namespace多interrupt map。
每例独立owner schema沿现fixture安装清理；独立connection读原checkpoint_writes/官方aget_tuple，非同连接spy通过。
worker未运行PG，Root须核实际0skip/0fail、返回后可见性和取消分支是否同纯native；如固定saver事实不同，保RED回报，不放宽到成功。

**已证范围：** 固定native的上述纯执行语义与反例；测试observer只委托saver、Event显式边界，不替换调度。
**未证范围：** PG七例尚待Root；effect计数只是本地标记、没有真实外部调用；无真实进程kill/重启、Run lease/start事务、
生产恢复predicate/多轮向量归属/完整Chat source、GC竞争或Exactly-once实现。模拟丢弃观察列表仅证明可重新读取native事实，
不等于跨进程业务恢复闭环。生产52路径继续等待Root三面/测试门，机器4.0与消费者pin尚未发布。
四docs三面一致性、P3B后缀/保护源码hash、52路径及六文件hash见`/tmp/kokoro-agent-hitl-proof-manifest.json`；
实际纯native/contract绿色不代表HITL生产完成，原完整scope/retry/effective/native/retention与Conversation最终释放目标不缩减。

### R27 Root已裁决的版本顺序（覆盖下方旧候选数字解释）

独立完整HITL owner切片使用 **Agent HTTP 4.0.0、原/v1单路径clean-slate替换**；当前机器源仍3.0.0，
本D0不提前修改。4.0版本号仅表示本次breaking协议，不表示完整scope第四阶段目标验收。下方历史候选的
“HTTP4 required retry/fullscope协调激活”不再作为本次4.0发布内容；这些能力继续完整goal，后续若breaking则另发Agent5.0.0。
P3B effective-native、scope/retry/checkpoint/retention功能目标均保留，库fork仍未批准。BFF4.0/后继4.1是其独立版本线，不机械同号。
一次替换旧interaction/resume解释、无新/v2长期双轨、无兼容fallback；Agent4机器＋实现/schema/artifact验证提交后，
BFF才固定pin并更新集合投影，再Web消费。HITL本身也必须完整实现、真门通过后发布，不发半contract。

## AGENT-P3B-D0-R26：设计冻结待审（2026-10-01）

当前正式已验基线 main `e977923ea9992cbddaf0cdbc6c8f8d23b3af120e`，HITL/Agent HTTP4已发布；R64仅修正现四文档新增P3B段的当前事实。
P3A历史基线为`af45817260478f1ee755d8e6e6963051e2049062`，下节Root默认1649/6/234、真实PG43/43、独立review0/0/0与安装wheel均是历史证据，不是R64新增验证。
原P3B D0收敛实际单次native材料化/GP/catalog/LangChain工具归一化、真实disarmed BackendProtocol、全peer一次effective持久gate；
所有源码/SQL/机器contract/依赖/lock/安装目录未修改，effective尚未实现；已发布HITL4不表示P3B或完整scope/retry/native/retention目标完成。

只读证据：固定安装DeepAgents0.6.6/LangChain1.3.2真实源码、factory/backend与三设计；官方primary仓库/定制文档/版本release commit
已核，版本/hash/MIT与未验供应链项详见TECH顶节。不把官方签名推论为本地wheel验签，不声称锁定版本latest或无漏洞。
当前公开API缺完整observation；维护fork最小接口是候选，尚待Root ADR、维护owner、新制品精确版本/commit/hash及库contract证明。
不改site-packages、不vendor、不建library目录，不无限等待上游；Root可批准独立维护artifact后继续，当前不提前接入。

设计明示：冻结实际装配政策和runtime模板/规则，不声称预先冻结每轮动态prompt；真实变化必须纳入digest，动态授权仍重验。
执行顺序保持static commit→全peer preflight/System→本地单次材料化→一次effective commit→backend activation→原graph执行。
SQL目标同Run fence/static同值/原request字节/锁后clock/无执行首次资格、matched零更新；现effective列尚不存在。
TECH列拟议30路径（26既有+4新、无新目录）与RED/真PG/安装wheel矩阵，不是本轮代码授权。

当前未决：Root依赖ADR/维护owner及精确artifact、候选observer接口实证；P3B源码/真实故障矩阵未运行。
完整scope/retry/native/checkpoint/retention仍硬门；Conversation最终引用释放产品未决只阻最终GC及上述完整目标闭环，不阻本片独立推进；后续若breaking按既定Agent5.0发布。
原P3B D0历史验证（非R64重跑）：`PYTHONDONTWRITEBYTECODE=1 uv run --frozen --offline --no-sync kokoro-agent-contract-check` exit0，
输出`kokoro-agent contract: ok`，日志`/tmp/kokoro-agent-p3b-d0-contract.log`。只读hash范围检查确认仅四授权docs变化、
20个监视的非文档文件byte不变；拟议30路径唯一、26现有/4新且父目录均存在，Markdown围栏/行尾空白/基线检查通过。
四文件SHA交付`/tmp/kokoro-agent-p3b-d0-manifest.json`，实际diff为`/tmp/kokoro-agent-p3b-d0-doc.diff`。
R64仅做文档事实修正与字节保护核验，当前HTTP4静态contract复验由Root后继执行。未运行未来native/SQL/PG矩阵，
不把历史contract通过冒充P3B实现；Root独立审查后决定文档提交，源码实施仍须另行批准。

## P3A Root 正式代码验收（2026-10-01）

22路径最终52e4a9f9冻结Root hash/范围复核，独立初审及PG屏障复审均0P0/0P1/0P2。Root真PG两文件 **43 passed/0 failed/0 skipped（2.72s）**，包括schema catalog与事务原request/fence/rollback/取消/ACK失联/精确PID竞争；日志 /tmp/kokoro-agent-p3a-r26-root-real-pg.log，临时DBagent_profile_17ac684fe518473e已drop，无Redis/provider访问。此前41/2 RED保留，修正soft queue观察不是生产降门。

Root最终默认完整链exit0：lock/sync离线、Ruff262/Pyright0、contract/generated、**1649 passed/6既定skipped/234 deselected（94.95s）**及wheel/sdist；/tmp/kokoro-agent-p3a-r26-root-full.log。Root自有wheel从/tmp独立安装，230资源/115依赖/12动态边/三Feature与源码完全相同、缺memory拒绝、新profile adapter和canonical SQL准确；/tmp/kokoro-agent-p3a-r26-root-wheel.log，临时target已回收，user dist未改。本片fresh schema，不热patch旧应用库，不宣称当前运行已经切换。P3B effective native/materialization/all-peer执行前屏障、scope/native/retry/retention/4发布尚未完成，原owner已只读推进实际技术落点。

## AGENT-P3A-PG-BARRIER-R26：真实PG屏障返修候选（2026-10-01）

Root已核前次22hash，独立源码审查P0/P1/P2=0；主树完整默认链exit0（`/tmp/kokoro-agent-p3a-root-full.log`）。
Root真实PG43实际 **41 passed / 2 failed（13.23s）**，日志 `/tmp/kokoro-agent-p3a-root-real-pg.log`；
canonical CHECK catalog各项实际通过。两个失败均为同/异配方竞争case期待两个事务直接被外部root PID阻塞而超时；
PG队列soft blocking可形成第二事务→第一事务→root，旧direct count漏掉真实等待者。原RED保留，不把默认GREEN当PG通过。
Root自有DB843c903debef4573已回收，无Redis操作。

本返修仅 `tests/integration/database/test_run_profiles.py` 与本CURRENT，其他20路径保持前次冻结hash。
测试观察生产adapter创建的真实连接PID（不替换事务/SQL），在 `datname=current_database()` 的活动集合上从精确root PID
递归追踪pg_blocking_pids的直接及soft队列边；必须全部预期participant PID实际出现在rooted wait graph才释放屏障。
不泛数实例waiter、不降低参与者数量、不改胜负/最终事实断言；原5秒timeout不变，删除固定poll sleep，依真实SQL观察推进。
expiry case复用相同精确单participant观测，生产schema/authority/事务完全不变。

实际定点Ruff format/check通过，Pyright首次3处set推断诊断后明确类型修复至0 errors/0 warnings；
证据 `/tmp/kokoro-agent-p3a-r26-barrier-static-r2.log`。PG定点collect仍43例通过（`/tmp/kokoro-agent-p3a-r26-pg-collection.log`），
writer未连接数据库/服务/Git。Root尚须重新实际运行43例及完整默认门；本段不宣称返修PG已GREEN。

## AGENT-PROFILE-P3A-R25：实现冻结候选，待Root真PG与独立审查（2026-10-01）

基线main `a37e8f1e308286d922212f2d5365634fd3ff2c21`；Root已验收r2四docs并提交后明确授权22路径。
本片只改22批准路径，新增1生产adapter/1独立真PG测试，无新目录/contract/protocol/generated/lock或其他owner改动。
已实现同plan静态bytes/digest持久freeze/verify、精确request原UTF8 TEXT身份、Run锁后DBclock/lease generation校验、
commit后才preflight，以及原build fence错误收口；未实现scope/native第二阶段/HTTP4/完整retention，不称完整4完成。

**实际RED与中间证据：** `/tmp/kokoro-agent-p3a-red-order.log` 3 failed（chat/music/music_chat均在freeze前进入外部preflight）；
后焦点40 passed；扩展请求原TEXT/恢复/缺值/authority/HTTP-only后182 passed（`/tmp/kokoro-agent-p3a-focus-r2.log`）。
新增四worker入口4 passed；Pyright真实9→1→0修复，无ignore或放宽门；旧默认1644/6/233是追加测试前中间结果，不替代最终门。

**冻结代码最终worker门：** `/tmp/kokoro-agent-p3a-full-final.log` 全链exit0：uv lock --check --offline、
uv sync --frozen --offline、Ruff format262/check、Pyright0 errors/0 warnings、contract-check、failure-model generator --check、
default pytest **1649 passed / 6 skipped / 234 deselected / 364 warnings（101.06s）**、wheel/sdist build成功。
保留原LangChain beta/deprecation和Pyright版本提示，不升级锁文件。默认门不含真实DB/provider/外部服务验收。

**安装wheel实证：** `/tmp/kokoro-agent-p3a-wheel-evidence.log`；源码证据 `/tmp/kokoro-agent-p3a-source-evidence.json`。
真实离线安装wheel到自有target，CPython3.14.3从/tmp仅target导入，包括新增PostgresRunProfiles；所有kokoro_agent模块路径
均约束到安装目标，22来源组/230唯一资源/115runtime distributions/12动态边及chat/music/music_chat指纹逐项等于source；
移除已安装memory.py时failclosed后恢复。wheel/sdist在 `/tmp/kokoro-agent-p3a-dist/`；本次build启动前不存在，属自有产物。

**真PG明确未验：** `/tmp/kokoro-agent-p3a-pg-collection-final.log` 43用例仅collect成功，没有连接PG。
Root须在自有schema运行test_run_profiles.py与test_schema_installation.py；尤其CHECK catalog精确输出、双连接PID barrier、
锁等待后expiry、late generation、terminal两序、真实UPDATE后异常/SQL错误/task取消rollback、commit ACK丢失、原TEXT变体、
已执行缺binding、保存值损坏/partial与8MiB边界均需实跑。Python/fake结果不代替该门；未启动DB/Redis/服务/provider/浏览器。

Root独立范围/hash/源码审查、真PG及主树完整门仍待；Git/index/commit由Root独占。P3B真实单次native实际材料化输出与
all-peer有效政策执行前绑定继续由Agent owner推进；完整scope/native/retry/retention与Conversation最终release未闭环仍如实保留。

## 历史 AGENT-PROFILE-P3-D0：四文档候选、等待Root设计门（2026-10-01）

基线 `main 7e902c08296cacdacfe810ccbb4a6233d1b2ca7b`，P2已正式提交，不再作为当前待验候选。
本D0仅改TECHNICAL_DESIGN/API_CONTRACT/DATA_MODEL/CURRENT；未操作Git、源码、SQL、机器契约、设施或服务。
已读取Root任务R25/P3-D0、map、SQL03/Python09/API05、Agent三设计与实际factory/plan/Run/schema/control入口及安装native源码。

候选裁决：下一最短P3A为22路径/2新文件/无新目录的Run静态recipe持久freeze/verify；同plan在preflight/System前commit，
真实Run lease/generation/锁后DB clock事务、fresh schema/新列约束drift、typed failure与所有build入口测试。
P2源码清单/codec不重写；来源登记只补新adapter。HTTP-only不读worker/private配置、HTTP3机器不变，无3.0临时scope状态机。

正式两阶段目标不降义：P3B仍须在所有peer真实native本地材料化后、任何sandbox/provider执行前一次绑定有效policy，
retry/resume/takeover相等与scope-first属于后继完整门。当前DeepAgents constructor不返回完整实际policy记录；
Agent/library owner需建立真实单次材料化输出接口后实现P3B，禁止复制selector、graph闭包inspect或静态假descriptor。
该技术后继门及Conversation最终GC产品未决均不阻独立P3A SQL/事务实施。具体文件/RED/真PG故障矩阵见TECH顶节。

Root初次四hash/范围及独立复审P0=0/P1=1：请求身份序列化授权歧义。本轮仅四docs定点修订，
明确唯一 `request.model_dump_json().encode("utf-8")` 与stored原TEXT UTF8逐byte比较，拒解析等价授权；
补字段重排/空白/等价转义/默认差异/未知字段负向矩阵及正常首次claim RUNNING无执行事实正例。
原build generation终态authority、两阶段全peer屏障和22路径边界不变；复审关闭由Root决定。

本轮仅文档校验：UTF8、围栏、尾空白、22唯一路径/20现有+2拟新增且父目录存在，四doc hash manifest。
没有执行contract/pytest/build/真PG；下方1627/6/192及wheel229/115是已提交P2的Root证据，不是P3代码验证。
本D0待Root四hash/范围/三设计复核后提交，再明确授权P3A代码；不把本节写成实施通过或完整4发布。

## AGENT-PROFILE-P2：Root 独立验收（2026-10-01）

Root核最终26/26范围/hash，独立只读审查agent_p2_review_r25 P0=0/P1=0。随后在停写主树完整执行 lock-check、frozen sync、Ruff format/check、Pyright、contract、failure generator、默认pytest、wheel/sdist，全链exit0：format260、Pyright0 errors/0 warnings，pytest **1627 passed / 6 skipped / 192 deselected / 364 warnings（83.20s）**。命令输出工具摘录（保留显示截断说明，非完整原始日志）`/tmp/kokoro-agent-p2-r25-root-check-excerpts.log`。

Root另对本次自己构建wheel作离线独立目标安装，在/tmp导入且每个kokoro_agent模块路径受安装目录约束；22来源组、229唯一实际资源、12动态边、115精确runtime distributions，chat/music/music_chat与源码fingerprint逐值一致；删除安装目录memory.py后fail-closed。证据 `/tmp/kokoro-agent-p2-r25-root-wheel-evidence.log`、`/tmp/kokoro-agent-p2-r25-root-source-evidence.json`。本片只完成static recipe/source manifest/真实factory同plan与policy装配；后置effective native policy持久绑定、SQL/lease/scope/retention/HTTP4、真实模型与九owner端到端仍未完成。

## 历史 AGENT-PROFILE-P2-R24：实现候选已冻结、待Root审查（2026-10-01）

基线`main 9dcaa34a3664668c3ad2da6adcc71f271ea96224`，Root已提交P2-D0，writer不操作Git。
当前精确26路径＝原批准14生产/7tests/4docs＋Root补授权现`tests/contract/test_deepagents.py`隔离fixture；
6个新增文件、无新目录/依赖升级/SQL/wire/lock改动。已接生产manifest、实际factory单PreparedFeaturePlan、worker同policy、
插件metadata零副作用拒绝及二次枚举gate；仅前置foundation，不是Run持久freeze/完整profile或4.0激活。

本worker真实RED历史：缺模块8 failed（`/tmp/kokoro-agent-p2-r24-red.log`）；全peer预检/单policy2 failed
（`/tmp/kokoro-agent-p2-r24-red-assembly.log`）；缺源码闭包1 failed（`/tmp/kokoro-agent-p2-r24-red-closure.log`）；
manifest/绑定metadata/worker政策漂移3 failed（`/tmp/kokoro-agent-p2-r24-red-drift.log`）；native task模板1 failed
（`/tmp/kokoro-agent-p2-r24-red-native-task.log`）；bootstrap再次枚举晚插件1 failed
（`/tmp/kokoro-agent-p2-r24-red-plugin-race.log`），最后一项曾实际load/call，现已前置阻断并显式抛错，未静默吞掉。
另有runtime distribution传递闭包缺登记1 failed（`/tmp/kokoro-agent-p2-r24-red-distribution-closure.log`），
现115个runtime依赖（含extras/markers）固定登记。中途3 failed/5 passed、native私有API错误29 failed/19 passed、
Pyright15→5→晚枚举fixture空集合类型1诊断均为真实修复历史，不计GREEN。
原contract native探针注册全局profile的泄漏以逐case snapshot/finally恢复，不给生产加fake白名单。

焦点141通过（`/tmp/kokoro-agent-p2-r24-focused-final.log`，晚枚举/依赖闭包追加前）；追加晚枚举修复后原生/工厂/
contract55通过（`/tmp/kokoro-agent-p2-r24-green-plugin-race.log`），完整四包import闭包9通过
（`/tmp/kokoro-agent-p2-r24-source-closure-all-imports.log`）。中间default1602/6/192与1623/6/192保留作诊断，不代替最终冻结门。

**最终worker离线门：** `/tmp/kokoro-agent-p2-r24-full-final.log` exit0；`uv lock --check --offline`、
`uv sync --frozen --offline`、Ruff format260/check、Pyright0 errors/0 warnings、contract-check、failure generator-check、
default pytest **1627 passed / 6 skipped / 192 deselected / 364 warnings（78.85s）**、wheel/sdist build全部通过。
保留原LangChain beta/deprecation及Pyright升级提示，未升级/放宽配置。未运行真实PG/Redis/provider或服务组合验收。

**真实安装wheel证据：** `/tmp/kokoro-agent-p2-r24-wheel-evidence.log`；wheel/sdist位于
`/tmp/kokoro-agent-p2-r24-dist/`。以同一3.14解释器离线无依赖安装到自有临时target，在`/tmp`工作目录从该已安装包
实际导入；全部22组/229唯一资源与源码descriptor逐项相等，chat/music/music_chat真实静态plan fingerprint相等，
移除安装包memory源码必须失败（随后恢复）。仅复用已锁第三方依赖；逐个核本包已导入module绝非源码checkout回退，
未用src路径补丁掩盖打包。build目录启动前已确认不存在，为本次构建自有产物，交付时回收；不动dist/用户文件。
Root独立源码审查、冻结hash复核及主树重跑仍待执行，本节不是Root验收或完整4发布。

完整两阶段持久绑定、全peer后置顺序、缺第二阶段时retry资格仍属后继Agent门；Conversation最终引用释放产品未决
只阻最终释放/完整发布。第三方native plugin与未登记custom backend保持显式拒绝，未把任意扩展伪装为已批准实现。

## 历史 AGENT-P2-D0-R24：两阶段边界与插件准入整改待审（2026-10-01）

当前`main ec65d04f9915580eb57629126fffffc20f4c4033`已由Root提交/推送P1，起始clean由Root任务卡提供；
本writer不操作Git。P1当前为已验收，不是候选；其1588/6/192、静态/build/wheel证据见下一节，历史失败不删除。

本次只更新四设计文档，提出25路径P2实施门（14生产/7测试/4docs，6个拟新增文件、无新目录），**尚未授权源码**。
目标是完整生产来源登记、静态recipe/政策fingerprint、preflight前全peer计划与真build共用，recursion_limit已追踪至
Supervisor→invoke_once实际config，拟从main同一policy装配，不增加无消费者字段。

R24独立评审原四hash通过、P0=0/P1=2；本次仅整改四文档，是否关闭由Root复核。生产第三方plugin批准集合
默认空；lazy bootstrap/ep.load/call前pure metadata枚举拒unknown/重复/同key冲突，未来批准清单须显式有序
identity/dist/version/source，拒late mutation；负向counter=0及乱序/重复/晚注册加入RED矩阵。runtime-only
middleware callable不静态执行，P2默认拒绝不支持声明，后继若支持须绑定实际输出。

Root已裁决正式4两阶段：pre-System static recipe envelope冻结；route后仅本地model构造，在任何sandbox/provider
执行前绑定main+全部peer实际prompt/tool override/exclusion/GP/middleware source的effective_native_policy_digest；
retry/resume/takeover严格比较，route revision/health/凭据排除。P2仅25路径前置foundation，不实现持久第二阶段，
也不冒称完整Run profile已冻结；旧“所有最终descriptor在pre-System已知”的目标叙述已纠正而非降义为recipe。
后继Run SQL/事务、全peer装配顺序及缺第二阶段时retry资格须单独通过门。Conversation最终引用释放仍为独立产品
未决，仅阻最终释放/完整发布，不阻P2；两阶段方案本身不再列待裁决。

本D0实际只读源码/已安装资源及本轮R24任务卡/map，验证四文档UTF-8/围栏/尾空白、25唯一路径（19现有/6拟新增，
全部父目录存在）与只读HTTP机器版本3.0.0，生成新四文档hash manifest；未跑pytest/build/contract或schema测试，
未请求provider、访问PG/Redis或启动服务；SQL/wire/contract/lock/Python未改。完整production manifest、真实wheel
闭包、guard与plugin负向RED/GREEN、Root代码放行均待后续，不把P1四资源wheel证据移作P2完成。

## AGENT-PROFILE-P1：Root 独立验收通过（2026-10-01）

基线 main7570141；Root核11/11冻结路径与hash、独立审查P0/P1=0后，重跑完整离线门：lock/sync、Ruff format254/check、Pyright0 errors/0 warnings、contract-check、failure generator-check、pytest **1588 passed/6 skipped/192 deselected/364 warnings（58.24s）**、wheel/sdist，全部exit0。日志 `/tmp/kokoro-agent-profile-p1-root-r21-check.log`；Root构建wheel四生产resource与源码逐字节匹配，Root自有build产物已回收。

本片验收仅覆盖纯profile编码与共用选择计划；没有完整生产manifest/worker装配、持久freeze、scope/native或HTTP4发布，没有当前provider/browser组合证据。PG/Redis无本片行为变化，未拿离线测试替代全项目组合验收。下方worker交付及失败历史保留。Root精确11路径提交，不包含其他仓变更。

## 历史 AGENT-PROFILE-P1：worker 交付与返修证据（2026-10-01；现已提交）

基线 Agent `main 757014139cce9e6eb73a1b62e9420917a1e984a0`。唯一 writer 只改 D0 批准11路径：
4个生产、3个测试、4个文档；新增仅runtime_profile.py与test_runtime_profile.py。Git/index/commit仍由Root持有。
纯profile v1完整白名单编码/摘要、显式包resource来源、无secret toolbox metadata，以及tool/subagent共享plan已实现候选；
真实factory通过现build_toolset/build_subagent_bundle复用plan，profile直接投影同一plan，不维护第二selector。
现MCP授权、重复名、GP/declared/guards、preflight顺序保持；run_token_budget=0关闭语义保留。
没有SQL/wire/RunRequest/持久freeze/授权gate、没有完整生产manifest/worker装配，也未实现scope/native/retention或发布4.0。

**本 worker 实测（非 Root 验收）：**

- tests-only首轮RED **45 failed/32 passed**，最小GREEN77；追加同源投影/缺metadata/未知provider RED **5 failed/78 passed**→GREEN83；来源不可变/metadata挂载漂移RED **2 failed/47 passed**→聚焦GREEN88；预算0/未知subagent RED **2 failed/49 passed**。日志 `/tmp/kokoro-agent-profile-p1-red.log`、`/tmp/kokoro-agent-profile-p1-red-r2.log`、`/tmp/kokoro-agent-profile-p1-red-r3.log`、`/tmp/kokoro-agent-profile-p1-red-r4.log`。
- 首轮完整default真实 **1585 passed/1 failed/6 skipped/192 deselected/57.99s**：新模块cast违反既有architecture门；已用逐值运行时校验与sound TypeGuard修复，未改门或配置。历史 `/tmp/kokoro-agent-profile-p1-full-gates-r2.log` 保留。较早Pyright8→2类型诊断及full-gates-r1的测试lambda类型2错亦保留，不记为最终PASS。
- 修复后焦点三文件＋现architecture **117 passed**（3.80s），日志 `/tmp/kokoro-agent-profile-p1-green-r4.log`；Ruff/Pyright不放宽，当前模块497行。
- 最终完整离线门 `/tmp/kokoro-agent-profile-p1-full-gates-r3.log` **exit0**：`uv lock --check --offline`、`uv sync --frozen --offline`、Ruff format254/check、Pyright0 errors/0 warnings、contract-check、default pytest **1588 passed/6 skipped/192 deselected/364 warnings/57.26s**、wheel/sdist build全部通过。现LangChain deprecation/beta及Pyright新版本提示保留，不在本片升级依赖。
- `uv run --frozen --offline python scripts/generate_failure_models.py --check` exit0，日志 `/tmp/kokoro-agent-profile-p1-generator-check.log`；机器contract仍HTTP3.0。
- wheel产物 `/tmp/kokoro-agent-profile-p1-dist/kokoro_agent-2.0.0-py3-none-any.whl` 与sdist；在自有临时target离线无依赖安装，隔离Python实际从wheel导入，4个生产资源hash与源码相同，显式descriptor及真实tool plan通过。日志 `/tmp/kokoro-agent-profile-p1-wheel-evidence.log`；临时target已回收，无服务/owner/provider请求。这不是完整生产manifest/安装部署smoke。

以上验证不含PG/Redis/integration/acceptance/浏览器/provider；P1无这些行为变化，不拿默认测试数量替代它们。
范围与最终11文件hash随交付manifest；Root冻结复验并提交后才标本片已验收。Conversation最终释放决定仍待答复，
它不阻独立P1；完整4.0的scope/profile freeze/native/head/GC/协调消费者发布继续按D0依赖推进，九owner目标不缩小。

## 历史 AGENT4-D0：当时基线与 P1 实施门（2026-10-01）

当前 Agent `main 224d0f19ff2199c38b95f621015ea7856f589454`；本轮起始 clean，只改
TECHNICAL_DESIGN/API_CONTRACT/DATA_MODEL/CURRENT。`64665cb0` 已提交 terminal 原子收口，
`224d0f19` 已提交 durable RunRequest 备用入口收敛。下方各历史阶段的“实施中/待提交/当时未验”保留历史证据，
不覆盖本段当前状态。HTTP machine 仍3.0.0，scope/lineage/baseline/head/profile/run-bound saver尚未实现。
BFF e7a325ce 已由 Root R18 验收内部 Chat terminal FIFO 与非法 post-terminal source 阻断；不是 Agent scope 或组合通过。

D0 统一现 finalize_terminal 起点、全部入口 scope-first/锁后DB clock、cleanup locator、现schema仅查缺表与正式drift差距。
独立 P1 的精确放置表、7个源码/测试路径和4份文档、共享选择计划/编码与完整生产装配的分割边界已写入 TECHNICAL_DESIGN。
下一动作是 Root 审查后派 P1 tests RED→纯编码/真实build同源选择→离线验证，而非再次泛审。
P1 不改 SQL/contract/RunRequest、持久freeze、preflight顺序或运行授权，不半激活4.0；完成也不称profile已冻结。

Conversation删除/expiry的取消、保留/恢复窗口、执行记录/native引用释放仍待产品决定；它只阻最终释放与完整生命周期发布，
不阻独立P1或其他owner工作。scope/native正式各片仍需自身一致设计与Root授权；活跃/被引用事实保护、bounded Run purge及
native reachability和最终释放均为正式4.0发布门，不靠永久免GC闭环。Agent完整机器/runtime/schema与真实owner门通过后，
先发布不激活artifact，再BFF全normal Chat/Scheduled+retry、其他sender、Web，Root自有fresh协调切换。

本轮未执行pytest、schema/DB/Redis、服务、provider或browser；不复用下方历史测试数量作为D0/P1 GREEN。
仅文档差异、允许路径、保护文件摘要与交叉引用验证；Git/index/commit由Root持有。实际文档验证结果随交付报告列出。

## AGENT-DURABLE-INGRESS-P0：Root fresh 门已验证（2026-10-01）

仅备用 RunRequest 入口复用现 durable `_consume_request`；无intent不persist-user/claim/build，已有canonical覆盖Redis通知内容且duplicate不重复启动。无新SQL/wire/scope/409/retention/provider实现，resume/steer/cancel分支未改。本片不是完整Agent4或session FIFO。

Root最终真实PG+HTTP135/135、0跳过（15.73s），自有DB `agent_terminal_atomic_b9312aeba3e448fe` 已回收；日志 `/tmp/kokoro-agent-durable-ingress-root-pg-r2.log`。首轮134/1 RED完整保留，acceptance仅先正式enqueue，terminal Chat异常注入及Run/outbox/Chat回滚断言未放宽。Root fresh默认1530通过/6既定跳过/192排除；lock、Ruff format/check、Pyright、contract、wheel/sdist均成功，日志 `/tmp/kokoro-agent-durable-ingress-root-full-gates-r2.log`；既有LangChain warnings仍记录，非真实provider/browser验收。独立冻结9hash静态审查0 P0/P1，Root检查额外tracked路径0；唯一Root提交。

# kokoro-agent 当前实现

## AGENT-DURABLE-INGRESS-P0：实施中（2026-10-01）

基线 Agent main `64665cb0e5a0bca1cb4ff08147e0119aff769d6b`，起始 clean。正常 serve 已走
`get_pending_dispatch -> claim_dispatch`；本片仅将备用 `dispatch(RunRequest)` 从直接 `try_claim`
收敛到同一 durable consume 路径。不改 DDL、machine contract、scope/retry/retention、Provider 或
其他 owner，不宣称 session FIFO/Agent4。三设计文档已先于源码明确该局部门。

本 worker 已观测 tests-only RED：无 durable intent 的直接通知错误创建 Run；伪造 Redis
envelope 未使用 canonical pending request（2 failed）。最小实现后聚焦两例 2 passed，
`test_supervisor.py` 101 passed，`tests/unit/execution` 441 passed/12 deselected，全 `tests/unit`
1137 passed/6 skipped/18 deselected。`uv lock --check`、Ruff format/check、Pyright（0 errors/0 warnings）、
contract checker 与 wheel/sdist build 均 exit 0。测试输出含现有 LangChain deprecation/beta warnings，
且 unit 运行中有一条未导致失败的 loopback request-handler stderr；真实 PostgreSQL 回归留给 Root。
Root 首轮真实 PG+HTTP 为 134 passed/1 failed：旧 terminal Chat rollback acceptance fixture 直接
`dispatch(RunRequest)` 且未持久 dispatch intent，因本片 fail-closed 而正确 no-op，未进入故障注入。
现该现有用例已先 `enqueue_dispatch` 再 dispatch，保留 terminal rollback/outbox/Chat 断言；
本 worker 未运行真实 PG，修正后 135 例 GREEN 由 Root 复验。

## 历史实施记录：AGENT-TERMINAL-ATOMIC/P0（2026-10-01，已提交 64665cb0）

基线 Agent main `dd5afc3528fe3a835756bc3ff55dfacaa8ca76d3`；33 路径实现候选由 Root 统一审查/Git。本片没有 DDL、机器 wire、generated、lock 或其他 owner 改动。
自然/执行失败/build/resume失败/cancel/NACK 统一由 finalize_terminal 收口；旧三原语、claim callback 与 execute_active_effect 已删除。正常终态的最终 usage、固定 outbox、Chat identity/seq、cleanup 同事务；quarantine 仅私有 superseded audit，不新增公开 Chat/Redis。retained started 的 Chat 缺口在同终态事务先恢复；recovery 只验证已有 terminal Chat，缺失或漂移 fail-closed，不补写终态。
Run/Chat active fence 与 add_usage 都在取得 Run 锁后读数据库时钟。terminal 只接受既存精确 usage segment 重放。quarantine replay 严格核 private audit kind/payload、NULL index、timestamp、durable counter/fence；normal/cancel 不越过有效 rejected receipt。delivery 的真实 journal/scoped canonical Chat barrier 不变；active ACK 后保留原 outbox 映射，terminal 后按最终 consumed 水位重扫 GC，ensure 不重建第二 delivery。网络均在 commit 后，无锁等待网络。

**Root 冻结真实验证**：`tests/integration/database` + `tests/acceptance/test_http_ingress.py` **135 passed / 15.58s / exit 0**，日志 `/tmp/kokoro-terminal-atomic-root-expanded-pg-final.log`；自有 DB `agent_terminal_atomic_8fff30a8b58b4db5` 已回收。覆盖 Chat 写后 rollback、HTTP 安全失败、started 顺序、usage 过期/封口、receipt/private audit 篡改、GC 与 ensure/finalize 真行锁竞态、重复与 GC 后不重建。Root fresh `uv lock --check`、ruff format（252 unchanged）、ruff check、pyright（0 errors/0 warnings）、contract-check 均 exit 0；默认 pytest **1528 passed / 6 skipped / 192 deselected / 57.97s**，不混算 integration/acceptance。`uv build` wheel + sdist exit 0，输出为 Root 自有 `/tmp/kokoro-terminal-atomic-root-dist`。证据摘要 `/tmp/kokoro-terminal-atomic-root-gates-summary.json` 明确默认测试原始 stdout 在 tool transcript，不冒充已捕获 raw log；真实 build/contract 日志为 `/tmp/kokoro-terminal-atomic-root-build.log`、`/tmp/kokoro-terminal-atomic-root-contract.log`。本切片 Root 门通过，精确 33 路径已由 Root 提交为 64665cb0。独立冻结审查代码 P0/P1 均 0、保护范围外变化 0；本次仅收尾文档中两处已删除路径的历史时态。

worker 冻结后默认证据：`uv run --frozen pytest -q` **1528 passed / 6 skipped / 192 deselected / 56.86s**；不是 integration/acceptance 数量。pyright 0 errors/0 warnings，ruff check 0、format check 252 unchanged、diff check 0。中途真实失败保留：terminal Chat rollback RED；started 顺序 RED；usage/NACK/replay/锁后时钟 RED；Root 扩展 122 pass/4 fail→125 pass/2 fail→新边界2 pass/8 fail，均按断言修复；architecture 949>800 曾 1 fail/1527 pass，按现 events 核验职责拆分后原门通过，未放宽阈值（摘录 `/tmp/kokoro-terminal-atomic-architecture-red.txt`）。

本片不是完整 Agent4/FIFO/retry/profile/native/GC、receipt producer 或真实 provider 组合完成。retention 产品决定仍未决，完整设计/owner4 发布门保持未通过。源码/tests/其余文档冻结；本 worker 未执行 Git 或设施操作。

## AGENT-RETRY-DESIGN 文档候选（2026-09-30）

本轮起始 `main f3be3b97dd67df69ed3c6cb88c59f3bc2db97703`、Agent clean；该基线已有下文
Root 验收的 3.0 failure/cursor 与粒度切片，不重复把旧 main da056b 写成当前 HEAD。
本阶段唯一改动是 TECHNICAL_DESIGN/API_CONTRACT/DATA_MODEL/CURRENT 四现文档，暂无新 machine/SQL/源码。

目标为 required nullable `retry_of_run_id` 的正式 4.0 launch、原 user 与 attempt lineage、同 scope admission、
完整 pre-turn native checkpoint baseline/head 与 run lease CAS；保留失败后 normal 的稳定 Human 上下文，
不复制 failed AI/tool/native tail，也不复用旧批准。不是 assembly-only/phase-only重试，不靠改 retryable=false缩目标。
当前源码尚不支持：同原 user 新 run 会 identity conflict，native 按 run_id 追加 Human，恢复 config 无 checkpoint_id，
生产 saver 写入尚无 run-bound fence。R2按 Root `bf038a25` 当前任务补逐入口scope-first锁矩阵、两连接
竞态测试、retry profile非NULL原子复制、实际rg检出的8构造文件及1decoded fixture、publish不激活与全部sender
协调切换；未扩大源码授权。当时完整文档门 **未通过**；现按顶部D0区分实施子门与完整发布门，独立P1不等待生命周期决定，机器/DDL/生产实现及真实验收仍待完成。

实际只读证据：安装 METADATA/源码核验 DeepAgents 0.6.6、LangGraph 1.2.2、checkpoint 4.1.1、
checkpoint-postgres 3.1.2；native saver 精确 locator 与默认 latest、Pregel 新输入/丢 pending tasks、
namespace 子图路由、同 connection pipeline 语义已核对。一次 stdin、PYTHONDONTWRITEBYTECODE=1 的纯
StateGraph/InMemorySaver 无模型探针 exit0：baseline next=()、pending_writes=0，中途失败 next=('last',)，
retry next=()、原/新 logical Human 各一次、baseline完整不变、retry不继承failed parent。
这只是 native API 定点证据，不是完整 DeepAgents、PG rollback、子图/Delta 或 HTTP/integration验收。

Root后继native PG R3实际 exit0/PASS：同现PG5432自有随机DB经生产installer/drift，9场景；
`/tmp/kokoro-retry-native-pg-spike-r3-result.json` 已只读核对，临时driver为
`/tmp/kokoro-retry-native-pg-spike.py`。DeepAgentState Delta根/子图×empty/历史四场景 exactfork、完整先前messages、
原Human唯一、failed后继排除、baseline channels/pending不变、旧失败证据与另连接restart精确locator均过；
公共saver同连接+test-only head的application abort/head CAS lost/SQL abort/task.cancel四rollback与success commit均过。
资源已删除：database_remaining=false、cleanup_errors=[]。不证明生产Run lease/generation adapter、HTTP、
完整DeepAgents middleware/HITL/profile/GC或provider；这些仍待正式实施。Memory首子图断言错误及PG前两次fixture安装
失败保留（prepared多语句、继承role search_path=pg_catalog）；修driver用生产installer/显式public，失败轮亦各自清理。
Retention用户尚未回复，不擅造DELETEAPI或以永久跳过purge闭环；active/引用保护与生命周期释放仍分别待验。

2026-10-01本片前的只读入口审查中，Root当时仅授权四文档retry候选：彼时try_mark_terminal先提交而terminal
payload后写，heartbeat只补已有outbox；execute_active_effect持Run锁await Redis。这些终态/锁网络旧路径已由上方P0切片替换；
profile尚未在外部_preflight前freeze，此项仍待后继实现。候选现改为typed outcome单事务usage/barrier/terminal/outbox/cleanup/
Chat terminal事实/session seq/identity、成功head晋升/active释放、commit后仅Redis发布，删除旧不存在的CAS→outbox恢复描述。live保持可丢与现
Chat/native恢复；保留reserve→另事务fenced append→无锁publish，terminal先赢拒durable live，live先提交
则HTTP Chat seq先live后terminal。BFF正式AG-UI不读Redis；BFF e7a325ce已验非法post-terminal source block与内部Chat FIFO，
此事实不替代Agent scope/native fence。terminal原子持久Chat，重放保留原generation/identity及原seq，不承诺Redis绝无迟到字节、不建全帧ledger。
profile版本1字段/编码/source职责已按真实Feature/Agent/Toolbox/Subagent入口写明，无secret、不freeze动态route；
拟新增runtime_profile.py与run_checkpoints.py，无新目录。补worker/main、supervisor_recovery、execution/events、
domain/run/repositories等遗漏允许集；锁后DB clock及terminal/live RED矩阵、bounded Run purge/native reachability
为正式后继依赖。该修订不是源码完成、测试通过或生命周期已决定；原HEAD历史正文保持不变。

Root R2源码审查进一步确认：PostgresChatRepository._append_projection:380/_next_seq:473才分配session seq，
outbox index/durable_seq/fence是per-run；因此terminal Chat必须随outbox/head/active在同连接事务提交，
不能release后补投。现候选明确leases协调、events窄同cursor outbox协作、Chat唯一SQL的append_on_cursor，
delivery barrier须证Chat已持久；新增跨run barrier、Chat写后全rollback、重放原seq与Redis失败HTTP已见矩阵。

Root本轮原源码baseline复验（不是retry4新功能GREEN）：lock/format/Ruff/contract exit0；默认pytest
1520 passed、6 skipped、174 deselected（57.17s），默认不含integration/e2e/acceptance。
正式 `uv run --frozen pyright` actual0、0 errors/0 warnings，日志
`/tmp/kokoro-agent4-doc-correction-pyright-uv.log`。首次裸 `.venv/bin/pyright` 1262错误源于checker选择
Python环境错误，已保留该失败记录，不记为1262个源码缺陷；正确环境通过亦不替代未实现的真实执行/生命周期验收。

Root真实PG旧源码诊断 `/tmp/kokoro-agent-terminal-gap-probe.py`（结果同名前缀 `-result.json`）：自有fresh DB经生产installer/RunRepository，started发布后terminal CAS提交处模拟中断；新连接观察terminal=true、terminal_fence_seq=null、lease已清、queued terminal=0、reclaim=0，gap_reproduced=true、closed=true、脚本exit0。
这是旧代码崩溃窗口实证，不是修复GREEN或截图根因证明；Root wheel/sdist build exit0，产物目录 `/tmp/kokoro-agent4-doc-correction-build`，349个受保护tracked文件一致。
四文档仍为明确未通过设计门的候选，提交不代表实现放行或artifact发布；生命周期与整owner真实验收继续未完成。

本Agent未运行服务、PG/Redis、模型、浏览器、pytest完整门或 schema安装；未改依赖、机器源、生成物、Git index/commit。
底层同连接pipeline/Delta根子图API已定点通过；生产fenced adapter与两连接矩阵、完整middleware/HITL/profile/GC
按各实施子门逐片 RED→GREEN；独立P1不等待生命周期决定，最终释放及完整发布仍等待该决定，
Agent发布4.0但不激活→BFF全normal Chat/Scheduler producer/parser与retry同片→其他sender核查→Web→Root自有fresh/协调组合。失败历史、旧运行组与总目标未闭环状态保留。

## AGENT-FAILURE3-GRANULARITY Root验收（2026-09-30）

Root逐15候选与7保护hash独立核对、最终只读审查P0/P1/P2=0/0/0。fresh完整离线门实际exit0：lock/frozen sync、Ruff252/Pyright0、generator/checker、1520 passed/6既有skip/174deselect/364warnings（74.09s）、wheel/sdist；日志 `/tmp/kokoro-agent-granularity-root-final-gates.log`。同现PG/Redis的自有fixture完整HTTP acceptance22 passed/100warnings/7.20s、无skip/deselect，System/model为doubles；数据库及Redis15残留均0、cleanup_errors[]，不触活跃DB10/共享schema，日志 `/tmp/kokoro-agent-granularity-root-real-acceptance{.log,-result.json}`。Root标准实际FAIL137/0unverified，相比本片前139仅移除events/proof两项粒度，无新增项；日志 `/tmp/kokoro-agent-granularity-root-standard.json`。本片不声称全工程标准清零、BFF/Web3.0已消费或真正外部模型全链。

当前 main 为 `da056b0103cced10188cdc1f5baef841d8333889` 且本门开始时 clean；该提交已包含
Root 验收的 Agent HTTP 3.0 与 Run 初始 evidence cursor。Root 实测默认 pytest 1518 passed /
6 skipped / 174 deselected，真实 PostgreSQL/Redis/HTTP acceptance 22 passed，资源残留 0。
当前受管 3310 仍为旧 2.0，BFF/Web 3.0 消费未完成，不把已提交 owner artifact 写成产品已切换。

新 Root 标准 139 项实际新增两项失败：`execution/events.py` 806 行、
`execution_proof_contract.py` 804 行。已裁决后继把完整 failure 归码移到新
`execution/failures.py`，把具名 negative metadata exact validator 移到现
`execution_proof_negative_specs.py`，并把唯一公开 `json_exact` comparator 同移、由 checker 无别名
直接调用；无 alias、System client 归码、通用 utils、重复 comparator、压缩凑行或门禁豁免。
第一阶段仅四文档与两个现测试，生产源码当时未修改；OpenAPI/provenance/generated、proof artifact、
SQL、lock 至今均未修改。现行为聚焦 baseline 为 97 passed / 4 deselected；目标归属 RED 证据如下。
新增两项归属断言后，返修后完整定点实际 2 failed / 97 passed / 4 deselected（2.77s，exit 1）：
一项仅因 `execution/failures.py` 尚不存在，另一项仅因 negative specs 尚未发布目标 validator；
没有 collection/import error。排除这两项目标断言后，既有行为仍为 97 passed / 6 deselected
（2.67s，exit 0）。两个测试文件 Ruff format/check 均通过；源码门、默认 suite、Root 标准和真实
HTTP 当时尚未运行；该第一门未创建目标模块或接触运行基础设施。
独立 review 的两项 P2 已由 AST import-binding（含 `as`）门及 comparator 单 owner/direct-call/
bool-int 精确门覆盖。

源码候选新增 `execution/failures.py`，从 events 完整移出归码表、`failure_code` 与
`run_failed_payload`；初次执行、supervisor 恢复及三个直接测试消费者均改为新 owner import，
events 没有 alias/re-export。唯一 `json_exact` 与 `validate_named_negative_metadata` 已移入现
negative-spec policy，proof checker 无别名直接导入调用并删除旧实现；无循环或新 helper 层。
`events.py` 806→752 行，proof contract 804→769 行，negative specs 393 行，新 failure owner 56 行。

两项目标 RED 已转为 2 passed；相关 unit/contract 为 108 passed / 4 deselected。完整离线候选门：
Ruff format 252 文件与 check、Pyright 0、failure generator --check、contract checker、wheel/sdist
均 exit 0；默认 pytest 1520 passed / 6 skipped / 174 deselected / 364 warnings（58.41s）。
未运行 Root standard 或真实 PostgreSQL/Redis/HTTP acceptance，未访问服务、数据库、Redis、模型或
浏览器；这些仍由 Root 在冻结 hash 上独立验收。本片未改机器 contract、provenance、proof artifact、
SQL、lock 或外部行为，当前受管 3310 仍保持旧 2.0。

## AGENT-FAILURE-3.0 与 Run 首事件：Root 验收通过并已提交（2026-09-30）

验收候选基于 main `58b59cf7cdc4132042d25460b4928d71a66ae7ec`，最终 29 文件已由 Root
提交为 `da056b0103cced10188cdc1f5baef841d8333889`；
Root 与独立只读审查员逐项核对 manifest
`a517ac70a57777b534a3da7db7ac2e1158d406cc1e22970defac735c6d5f023e`，源码审查 P0/P1/P2 均为 0。
Root 在主工作树重新执行完整离线门：`uv lock --offline --check`、`uv sync --frozen --offline`、
`uv run --offline ruff format --check .`、`uv run --offline ruff check .`、`uv run --offline pyright`、
`uv run --offline python scripts/generate_failure_models.py --check`、
`uv run --offline kokoro-agent-contract-check`、`uv run --offline pytest -q`、`uv build --offline`，
组合实际 exit 0。默认测试 1518 passed / 6 既有 skipped / 174 deselected / 364 warnings，58.74s；
Ruff format 251 文件、Pyright 0、wheel/sdist 构建均通过。
完整日志 `/tmp/kokoro-agent-evidence-cursor-root-final-gates.log`。

Root 另以同一个 PostgreSQL role 复用 5432/6379，在唯一自有随机临时测试库与原子占有的空 Redis15 中
运行完整 `python3 -m pytest -q -o addopts= tests/acceptance/test_http_ingress.py`：
22 passed / 100 warnings / 7.31s，actual exit 0，无 skip/deselect。
这是生产 HTTP handler、真实 PostgreSQL/Redis/outbox/replay 的 owner acceptance；System/model 使用声明的
test doubles，不是外部模型、跨仓或浏览器 E2E。包括安全失败 retryable true/false、初始 index0 terminal、
tenant 隔离与重复终态 fence；没有 fake START、重编号或弱化断言。
fixture 临时数据库及 Redis15 精确回收，残留均 0、cleanup_errors 为空；未触活跃 Redis10 或共享 schema。
日志 `/tmp/kokoro-agent-evidence-cursor-root-real-acceptance-all.log`，结果
`/tmp/kokoro-agent-evidence-cursor-root-real-acceptance-all-result.json`。

本片无 SQL、依赖/lock 或 proof schema/vector/direct digest 变化。下面的失败与候选记录为历史，
其未通过状态已被本次 owner 验收后继；BFF/Web 严格消费及受管运行组协调发布仍未完成，不能据此宣称产品闭环。
当前受管服务保持旧 2.0 组合，不热加载本候选；提交后按 Agent → BFF → Web 固定来源并验收。

## 历史：AGENT-RUN-EVIDENCE-INITIAL-CURSOR 实现候选（2026-09-30）

Root R2 真实 safe failure 两例实际 2 fail/20 deselected（1.91s），自有 DB/Redis15 清理均 0。
outbox 两 audit row、Chat/Redis safe payload 与唯一可见帧断言已通过；HTTP evidence
初始 after_seq=0 排除了真实 terminal index=0，返回 terminal=False。此为现 owner cursor 缺陷，
不是 safe failure 模型失败或重复终态，亦未以 fake START/索引改号规避。

当前候选实现 Run-only EvidenceAfterSeq integer/int64 minimum/default=-1、maximum=int64 上界、exclusive last-seen
index；EvidencePage 空页回显 -1，Chat 共享 AfterSeq=0 不变。ingress/server、OpenAPI、
provenance 与生成 header 已同步，failure 模型内容不变；真实两例 GREEN 仍待 Root 下一门。
未访问服务/数据库/模型，不改当前受管组；下一发布仍按 Agent→BFF→Web 协调顺序。
定点纯 RED：两个现测试文件 8 failed/28 passed（1.55s，exit1），均为目标 cursor 断言，
无 collection error；无效 cursor 400 与 after_seq=0 exclusive 保留门已通过。
实现后相同两文件先 36 passed；追加 int64 上界门复现 3 failed/34 passed（1.72s），
实现 maximum 后最终 37 passed（1.43s，exit0）。
离线完整门：Ruff format 251 文件/check、Pyright 0、generator --check、contract checker、
uv lock --check、frozen sync、wheel/sdist 均 exit 0；默认 pytest 1518 passed/6 skipped/
174 deselected（58.25s）。6 skip 与 174 deselect 沿既有默认配置，未新增 skip/xfail。
本实现会话未启动服务或访问数据库/Redis；Root 仍须独立执行真实两例并审查提交。


## 历史：AGENT-FAILURE-CONTRACT 实现候选（2026-09-30，未协调发布）

基线 `main 58b59cf7`；四文档与 R4 RED 经 Root 放行后实现本仓候选。OpenAPI 3.1 的唯一基础
Failure 定义 code/retryable/合法 tuple；RunFailure、ChatFailure 最终封闭，单向生成 protocol 内只读
RunErrorCode/RunFailedPayload/ChatFailure。HTTP artifact 为 3.0.0，URL 仍 /v1，payload_json 仍 string
且 run.failed 有精确 decoded 映射。不是全事件导出或第二 wire。

System client 严格验证 HTTP/code/retryable，取消 status 掩码；共享执行归码保留 typed 失败与合法 bool，
初次/恢复的 lease/CAS/取消不改。Run wire 删除原异常类名/原文，Chat 安全投影保留 retryable。
未知 owner code 不猜 message，System unknown 仍不准入，不自动 retry、不承诺重试免费。

实际证据：R4 RED 52 fail/121 pass；新增 runtime/codegen 门 RED 2 fail/13 deselected。
四测试首 GREEN 175/175；首全量仍有 50 fail/1455 pass（旧字段断言、临时 contract copy inventory及检查器
粒度/诊断次序），未据此放行。修复保持原门后，contract＋相关 unit 590 pass/5 deselected；
最终默认 pytest 1505 pass/6 skip/174 deselected（57.67s），Pyright 0，Ruff format 251文件/check通过，
生成 --check、3.0 contract checker、uv lock --offline --check、uv sync --frozen --offline、
uv build --offline wheel/sdist 均 exit0。
6 skip 为既有 1 parent-repo examples 缺失、5 workspace archive 因本地 MinIO 9100 不可达；未新增 skip/xfail。
默认 suite 对 MinIO 有既有可达性探测，未启动该服务。174 项为默认排除的 integration/e2e/acceptance；
其中新增两个 safe failure true/false acceptance 用例已由 Root 真实执行；当前失败与资源清理事实见顶部。它们覆盖生产归码/
RunEmitter、真实 owner outbox/Chat/Redis 与正式 HTTP replay、重复 terminal fence，仅由 Root 在自有资源运行。

本片无 SQL/锁/依赖/proof schema/vector/direct digest 变化。Root 已运行自有 PG/Redis acceptance，
未通过原因见顶部；未运行真实 System/provider 或浏览器，未改当前受管组。源码/机器仍待 Root 固定提交，之后 BFF strict repin/发布、Web固定消费，
再由 Root 对明确自有 fixture 一次 fresh 切换及真实持久/跨 owner 验收。旧 JSON 无兼容，未授权数据不动。
下文为先前阶段记录，不代表这些后继门已过。

## W3 OAuth 响应扩展返修候选（2026-09-30）

基线 `e728fe24d9528efe02a53282f1dfd8328a122f9a`。Root 正常 Source 组合在安装阶段确认 IAM 成功响应
含整数 `expires_at`，旧 strict/extra=forbid 解析误报 `PLATFORM_TOKEN_INVALID_RESPONSE`。
本片仅在 OAuth 成功响应边界忽略未知成员，保留已知字段及传输/缓存安全检查；不剪裁 IAM fixture 或伪造 token。
无 SQL/Proto/generated/锁文件变化；聚焦42/default1431通过，锁/格式/静态类型/契约/build通过，
6 skip/172 deselected原因见 [ACCEPTANCE.md](ACCEPTANCE.md)。真实 owner 组合仍由 Root 在固定候选验收后重跑，尚未宣称通过。

## W3 Run Skill metadata 返修候选（2026-09-30）

基线 `534d3f80efb158910fde73e2a8ecf5390f874bba` 尚未由 Root 放行：同 session checkpoint
使 SDK 默认 SkillsMiddleware 跳过后续 Run 的 frozen refs。现有 `skills/middleware.py` 子类仅调整公开
before_agent/abefore_agent 生命周期；复制输入 state 去掉上次 metadata/errors 后委托父类，原生 parser、
private state schema 与模型 prompt 均保持。factory 通过公开 middleware 参数装配唯一实例，禁用重复默认实例。
新 Run（含同 refs）重新读当前包；空选择更新为 [] 且零 Skill client/reader 依赖；旧 load_errors 明确清空。
同 Run HITL 重建 graph 的 preflight 仍重验授权，Command resume 原 checkpoint 节点不重入 discovery；
模型/工具 lease guard 保留。授权失败与取消阻断模型，恢复时重试当前 Run 加载，不用旧 metadata 继续运行。

生产 Factory＋真实 DeepAgents＋InMemorySaver 已覆盖选择转换、模型 prompt、私有 checkpoint、恢复和 guard；
外部 Skill owner 与模型为具名测试 double，非真实服务组合。无 clients/ZIP/SQL/契约/generated/锁/服务修改。
实际门禁见 [ACCEPTANCE.md](ACCEPTANCE.md)；固定提交、独立审查、Root 复跑与真 owner 组合仍待主控验收。

## 前一候选：W3 typed Skill reader（2026-09-29）

基线 `dd34a4800b4ce0cc61eb80dd715e528b9d4517da`；本片已实现完整 typed reader，待独立审查与 Root 固定 SHA 验收。
Agent 固定 Platform `6a09913a96c686b316bfe707b823d039e625607a` v4 原始 21 JSON、Proto 与再生客户端；
保留 bindingVersion 3.0.0、24 bindings/六个允许出站 RPC，ZIP profile 29 向量进入 contract checker。
BFF `571b51de` 与 Web `1dc211bb` 已消费 exact refs/[]；Root `772208ba` 已验基础真实 worker/Chromium 恢复
（System/model 为 fixture）。下文旧阶段的“BFF未接/launch未发布/仍有静态music”仅为历史，不代表当前代码。

非空 Run refs 经 `WorkerPlatformRuntime.skills_for_run(LeasedRun)` → 当前 lease/token/fresh proof sender →
Resolve/每次 Approved → Storage signed GET → 完整 ZIP/manifest验证，先于模型、sandbox和工具创建。
空 refs 不创建 Skill client、不调用 IAM/Platform/Storage。旧 name/scope/hash、静态 Agent.skills、旧读取注入全部删除。
只读根段是 exact SkillId ASCII 的无填充 base64url；原文件 bytes 不重写、不落盘、所有写操作拒绝。

GET 仅当前固定 Storage v2 所允许的空 `required_headers`；这是 owner `transfer-reference-headers.ts` 与两种 GET signer
的精确约束，不是自创非空 header 支持。ObjectStore origin 可单独配置；Storage 写入继续需要 URL+secret+origin。
HTTPX 公开 transport 不保留 cookie jar、不产生 AsyncClient 的带签名 URL INFO日志；不跟随 redirect，显式
connect/pool 3s、idle/write 10s、总 30s（且不超过签名期限），32MiB压缩硬限与SHA-256，取消传播。

不缓存包 bytes 或授权；每次访问重新取 Approved 并完整验证 GET/ZIP。glob/grep 逐包处理，下载每次最多128条、累计128MiB；
grep最多1000条/1MiB文本，超限明确失败不输出部分结果。单包展开128MiB/单文件16MiB/128条/manifest16KiB保持owner边界。
未新增SQL、Redis键、服务、依赖或锁文件。纯解析/边界/loopback及fake owner组件证据不是生产IAM/Platform/Storage真组合。
Platform v4激活、用户安装启用产品链、Storage退役仍由后续owner关闭，ACTIVE发布列表不替代installed+enabled授权。

本候选最终默认门：1399 pass / 6 skip / 172 deselected；Ruff248文件、Pyright0错误、contract含29ZIP、
生成drift、wheel/sdist与隔离Python3.11生成消费smoke通过。skip/资源边界/日志详见[验收](ACCEPTANCE.md)。
交付commit由Root任务表记录，独立审查与真owner组合尚待验收，不以单仓成功更改激活状态。

## 历史：W3 Agent launch 契约代码片（已由 Root 验收）

已在本仓 OpenAPI `2.0.0`（URL 仍 `/v1`）、HTTP ingress、Redis `RunRequest` 加入必填 `selected_skill_source_refs`，严格检查 exact ref/16 项/4 KiB/重复/顺序；`[]` 承载基础 Chat，旧 payload/旧 trace fallback 不接受。现有 dispatch 与 Run JSON/fence 接收同一请求，同 `run_id` 改选择为 409。非空 refs 在 AgentFactory preflight 明确失败，先于模型解析、backend/工具和旧 name Skill 访问，待真正 Source reader 切片替换此 guard；不能把 202 当运行成功。单元/契约聚焦验证已过；真实 PostgreSQL/Redis roundtrip 未验，不声称两表实测。Platform v4、Source adapter、Storage signed GET/ZIP、只读 backend 与旧 name 删除均未接；BFF 普通 Chat durable outbox 和 Scheduler launch 未发送字段，旧 BFF 后台派送会得到 400，须紧接 owner-first 更新 BFF。

## W3 typed Skill source 设计门（历史基线 `7dfcfa9`，仅作目标背景）

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
