# kokoro-agent 当前实现

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
协调切换；未扩大源码授权。文档门状态 **未通过**：生命周期用户决定未决，机器/DDL/生产实现及真实验收尚未完成。

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

2026-10-01只读入口审查后，Root授权本次仅修四文档retry候选：实际发现try_mark_terminal先提交而terminal
payload后写，现heartbeat只补已有outbox，没有缺payload恢复；execute_active_effect持Run锁await Redis；
profile尚未在外部_preflight前freeze。候选现改为typed outcome单事务usage/barrier/terminal/outbox/cleanup/
Chat terminal事实/session seq/identity、成功head晋升/active释放、commit后仅Redis发布，删除旧不存在的CAS→outbox恢复描述。live保持可丢与现
Chat/native恢复；保留reserve→另事务fenced append→无锁publish，terminal先赢拒durable live，live先提交
则HTTP Chat seq先live后terminal。BFF正式AG-UI不读Redis；post-terminal source block属于后继FIFO待实现，
不把现去重/gap门冒充该保证。terminal原子持久Chat，重放保留原generation/identity及原seq，不承诺Redis绝无迟到字节、不建全帧ledger。
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
仍待四文档门与生命周期裁决后逐片 RED→GREEN，
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
