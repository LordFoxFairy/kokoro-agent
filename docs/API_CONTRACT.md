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

### 当前候选机器语义

唯一机器源为 `contract/openapi/v1/openapi.json`。ResumeControl required revision/ref、全部 item_id decisions；
旧字段即便同时携带新 item_id 也拒绝。required revision/ref/item 无默认；仅语义允许的 approve.args /
reject.reason 可 null/omitted 同义。先严格 typed validate，唯一 typed model_dump_json body 持久化；
request_digest 仍以现 sort_keys/compact 的规范化请求算法生成，不加 raw 第二字段或新 hash 规则。
HTTP 202/receipt succeeded 是 durable admission/delivery，不是 native consumption、不是清 waiting 证据。

ChatEvent 精确 decoded mapping 为 run.failed→ChatFailure、interaction.state→ChatInteractionState。
后者 required action_result（nullable），resuming 必须 accepted/unknown；groups 为完整替换集合，
validation 只安全 code/instance_path。跨字段 revision 比较、跨 group item 唯一性继续由 strict runtime 校验；
机器 schema 覆盖可表达的 phase/集合/null/preview/封闭字段约束，不承诺 JSON Schema 能表达任意跨字段相等。
Failure code/retryable tuple、proof/JWKS、Storage/Platform pins 与原 HTTP 路径/权限未改。

---

# kokoro-agent API 契约

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

### 当前候选接口事实

ChatInteractionState.action_result为required nullable；初waiting显式null，其他结果绑定原command/pause revision。
未知保持resuming全集并产生新revision/source；validation重问动作原revision与新等待revision分离。
公开item_id唯一寻址；native原request_id只在adapter内部还原，SDK approval的action结构不成为公开alias。
七Run ports及R35 R2两native入口签名未换第二版本；HTTP机器仍3，候选typed变化须后继完整4 artifact门。


## AGENT-HITL-NATIVE-BRIDGE-D0-R35：精确桥接口（候选，未实施）

Root已验R3事务26/26（2.51s）、相关四PG文件71/71（5.56s），资源均回收；它们不证明正式桥。
Root fresh default实际78 failed/1664 passed/6 skip/268 deselected（97.96s），Pyright仍43错误；
失败分布supervisor26、machine_contract16、chat_response14、hitl11、public_contract6、control_commands4、execution_proof_artifact1。
日志`/tmp/kokoro-agent-hitl-p2-r34-root-{related-pg,default}.log`。本轮仅四HITL前缀；生产、测试、机器与P3B整suffix保护。
以下精确决定覆盖下方历史D0的未定字段/方法；既有P2事务语义保持，新增桥/schema/完整4均待源码卡和真实门。

### 完整4 source与动作结果的准确增量

内部port和不可变值唯一签名见TECH本R35节；SQL列/CHECK/codec唯一见DATA本R35节，不新增HTTP endpoint。
完整Agent HTTP4仍原/v1单路径；机器当前3，P2内部候选不作为已发布artifact。所有新字段同时进入机器、runtime严格model、
Chat decoded-payload mapping及正反例；不默认补旧字段、不保旧interaction/tool_id/request_id寻址。

ChatInteractionState候选新增必需`action_result`字段，值为null或严格对象：
`{command_id: 非空str, pause_revision: 正整数, kind: accepted|native_consumed|validation_failed|unknown|cancelled}`。
初waiting无已接受动作时显式null；原P2 typed模型尚未有此字段，下一完整4实现必须补，不据本D0声称已支持。
action_result.pause_revision指原动作所属轮次；新validation source的顶层pause_revision是新轮次，两者不得混用。
拒绝接受（stale/partial/不允许）仍为现command失败receipt的`rejected`结果及稳定安全error/reason，不改变当前waiting集合，
不凭无accepted intent制造native消费结果。对已接受的拒绝决策，native实际消费证明到达后可为native_consumed，
它不是工具执行成功；工具结果只走真实native结果通道，不在resume前synthetic returned。

| 持久阶段 | 公开全量source |
| --- | --- |
| accept事务 | resuming、原全集、action_result=accepted；只是接受。 |
| start或native_observed提交 | 不发新phase/source；调用许可仅首次StartedResume。 |
| 完整消费且稳定无新interrupt | active、空集合、native_consumed，同事务写command/result/Run/Chat。 |
| 完整可归属的新validation/interrupt | 直接新waiting完整集合；有结构化validation为validation_failed，否则native_consumed；无中间空active。 |
| 缺完整证据 | unknown动作结果与保留resuming集合一起持久；零再调用；健康任务不因暂缺证据立刻判unknown失败。 |
| 原权威终态 | terminal空集合；未结动作cancelled表示被Run终态关闭，不倒推工具没执行或成功；已结动作保持原结果。 |

action_result随全量state持久，source仍(run_id,interaction_revision)，历史重放返回原Chat source；
未知ACK查原command，不生成新command/attempt。native_observed是私有可恢复中间状态，不是新wire phase。
公开字段禁止native locator、观察digest/向量、quiescence token、原决策、raw异常/validation value；
validation只code=json_schema_invalid与安全instance_path。HTTP-only不加载worker/native/private配置。

官方独立读取证明实际写入，不证明所有saver收到的新值均覆盖；NULL_TASK只输入、旧RESUME槽/混合batch/缺子namespace归属仍unknown。
观察丢失可只读重建充分证据，否则保持歧义，不能以成功successor、HTTP ACK、run.started或普通活动解除等待。
healthy invocation受当前有效lease及真实tracked task保护；只有明确已drain静止的同attempt三次新成功稳定读取才耗尽协调计数。
终态从既有authority/finalizer产生，跨worker缺handle或lease到期本身不授权重投/错误终态。

43类型错误由真实全集item映射替换旧审批消费者收敛；30机器contract RED由完整4的机器/checker/生成provenance同向更新收敛。
Root新增default失败还覆盖public_contract/execution_proof_artifact等真实消费者/证据绑定，后继精确授权后同步更新；
不删断言、不写旧alias，不因相关PG71绿发布半4。Agent完整门及artifact先验收，BFF后固定pin，Web再消费。

R35接线补名：新增只读read_resume_context不改wire，恢复读取原command冻结pause/plan而非当前head。
accepted只返回PreparedNativeResume，仍须StartedResume授许可；started/native_observed/unknown只返回无Command的
ObservedNativeResume，reconciled/terminal只精确重放。两个native装配/读取入口及六字段只读上下文见TECH本R35补名节。

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

首RED锁Agent4版本、ResumeControl必需expected_pause_revision>=1/pause_ref/decisions、五类decision只按item_id寻址，
旧tool_id/request_id寻址拒绝；实际inbound_adapter及HTTP _parse_control均须保新pause身份，不仅OpenAPI示例可通过。
ChatEvent删除旧interaction、增加interaction.state，decoded mapping必须指向严格ChatInteractionState；
其interaction_revision/pause_revision/pause_ref/phase/groups是全量表示，waiting/resuming非空、active/terminal为空。
负例锁缺集合/ref、零revision、raw args/checkpoint locator、validation原值及未知字段拒绝；正例预检防“所有payload均无效”的伪绿。
本次只新增失败断言，不宣称当前3支持这些4字段/验证/运行时语义。

## AGENT-HITL-PERSIST-P2-D0-R29：候选事务接口与发布边界

基线main0245a36：P1已提交；实际机器仍3.0，SQL/事务核未实现。仅四docs HITL prefix更新；P3B后缀保持。
本P2的五个内部RunInteractionPort方法及参数/返回以TECH同名表为唯一接口清单：read_interaction、record_pause、
accept_resume、start_resume、mark_resume_unknown。内部Row、Native locator和私有决策编码见DATA，不复制成HTTP DTO。
AcceptedResume仅证明Run/command/resuming Chat同事务提交；StartedResume才表示本次唯一start提交/一次调用许可，
ReplayedResume永不授第二次调用。start/unknown不推进公开phase revision；admitted/applied/HTTP ACK仍不清等待。

完整Agent HTTP4候选是P2 typed Chat前置：原/v1单路径，resume强制expected_pause_revision、pause_ref、全集decisions，
decision各分支固定item_id为全集身份，既有type及对应args/reason/response/value语义保留；
tool_id/request_id由pending关联承接而非第二caller寻址身份，不留旧字段alias；统一typed interaction.state及decoded payload mapping、完整动作结果/错误测试；不只增enum就称完整contract。
当前strict ChatEventType与3.0机器/checker只有旧interaction，新payload不得借旧名称或任意JSON绕过。
候选代码的schema/运行模型/生成hash必须一致；完整native/全部入口/恢复/消费者门之前不发布artifact、不改消费者pin，
不把P2内部可测试能力当当前3运行时的新承诺，不加运行开关/兼容fallback做半4。

公开source每次完整替换：waiting/resuming均非空原group顺序；前者awaiting、后者submitted；active/terminal为空。
只公开安全display/validation及动作类别，排除原decision bytes、locator、resume vector、secret/原始异常；
native_consumed及validation_failed留真实桥才能发，P2不合成这些结果。terminal由原权威原子清集合，不倒推已消费。
source身份为(run_id, interaction_revision)，仍使用既有Chat envelope、source_index与session seq，不新增第二网络通道。
数据表示/内部codec不改变现control request_digest算法；原command必须核存储归属和规范化完整决策原bytes，不能只信caller摘要。
同command重放先读历史intent，不改新pause或lease；不同command争同pause至多一个新accept；unknown不发新command试探执行。

HTTP入口现先get_request_scoped再admit_control有purge竞争；后继Run-first admission锁内Run消失以typed run_missing
映射已有404 run_not_found，异tenant继续按现隐藏存在性规则，不误报409 digest mismatch。完整candidate4预期映射：结构/unknown字段/重复item→HTTP400 invalid_run_control；原HTTP command digest冲突→409 command_digest_mismatch；
集合/轮次接受冲突发生在worker事务而非HTTP admission，command结果failed/error_code=interaction_conflict，安全细分reason为stale_pause/incomplete_collection/decision_not_allowed/not_waiting；
它不清当前pending、不发伪native_consumed。内部authority_lost只丢弃失效writer、零head/source，不借新generation造失败；
corrupt_state沿现当前authority的internal_error/false终态，若authority也失效则零终态。以上新reason/command error须机器候选登记及负例测试，
不从异常message生成wire值、不把HTTP ACK升级成accept。request原TEXT身份比较沿P3A精确UTF8规则。

P2生产候选36路径及事务PG文件见TECH；本轮仅设计，contract/provenance/generated/source一字未改。
机器候选生成时现run_failure_generated.py会随源hash重建，不能手改。发布仍Agent完整4先于BFF固定artifact及Web集合消费；
后续scope/retry/effective/native retention目标不降义、若breaking再5。当前3的contract-check只证明未漂移。

选择快照方案B：Run pause_snapshot_json冻结完整groups（含display/validation/allowed_decisions）＋locator，
command resume_pause_snapshot_json复制接受时原快照；当前pending_groups_json只表示可见集合。waiting/resuming与snapshot.groups一致，
terminal清当前pending而保快照；新pause替换Run快照不改历史command。decoder从各自完整快照重算摘要，不信裸digest，不从新head补旧值。
这两份快照分属当前pause和历史命令生命周期，只有record_pause/accept单向写入，不设双向同步或第二可编辑真源。

R29 Root已裁决：P2为完整HITL owner目标内的先行事务TDD，36不是独立可发布4；旧审批消费者仍按tool/request寻址，
必须按TECH依赖阶段同一owner交付替换全部真实编解码/worker路径。必要candidate wire同步变更造成的旧tests RED如实保留，
待完整HITL验收，不用alias/default、伪source或只SQL方案掩盖；native内部HumanRequest身份与外部item_id的受信映射不等于wire兼容。
TECH列22精确扩展依赖（含真实attempt跟踪supervisor/context），每批Root另卡；本次仍只四docs，没有源码/协议授权。

## AGENT-HITL-DOMAIN-P1-R28：仅内部纯规则

基线main512a846；本片新增domain/run/interactions.py内部不可变值/转换，无HTTP/Redis/Chat source发布，机器仍3.0。
分组顺序归一后的完整决策逐byte比较，重复command精确重放零转换，差异/不完整/stale拒整批；不引入新的wire schema或code。
当前head只保当前intent；更早命令幂等由后继durable ledger加载原intent并调用纯重放校验。没有无限内存ledger/fallback。
接受规则不等于durable accepted；start规则不等于已提交dispatch；unknown不重投、terminal不复活。
4.0发布继续等待Run/command/Chat事务、native证据与consumer pin，未提交P3B suffix保持。
实际内部规则与38例pure候选已落位；现机器3.0 contract-check仍通过，没有新source/decoder/HTTP版本发布。


## AGENT-HITL-D0-R27：revisioned pending与动作结果候选（2026-10-01）

### R27 Root已裁决的版本顺序（覆盖下方旧候选数字解释）

独立完整HITL owner切片使用 **Agent HTTP 4.0.0、原/v1单路径clean-slate替换**；当前机器源仍3.0.0，
本D0不提前修改。4.0版本号仅表示本次breaking协议，不表示完整scope第四阶段目标验收。下方历史候选的
“HTTP4 required retry/fullscope协调激活”不再作为本次4.0发布内容；这些能力继续完整goal，后续若breaking则另发Agent5.0.0。
P3B effective-native、scope/retry/checkpoint/retention功能目标均保留，库fork仍未批准。BFF4.0/后继4.1是其独立版本线，不机械同号。
一次替换旧interaction/resume解释、无新/v2长期双轨、无兼容fallback；Agent4机器＋实现/schema/artifact验证提交后，
BFF才固定pin并更新集合投影，再Web消费。HITL本身也必须完整实现、真门通过后发布，不发半contract。

当前main af45817260478f1ee755d8e6e6963051e2049062，HTTP仍3.0.0；P3B下节只候选，HITL本节也只设计，
机器源/消费者尚未修改。现Agent Chat interaction确实持久，含pending_tool_ids/schema/result，但args={}会丢validation_error；
Run critical outbox不含原始awaiting。现HTTP/control applied是调度receipt，不是native解除事实。

拟由Agent发布严格typed Chat `interaction.state`完整替换source（候选schema，Agent4.0.0版本已裁决，待实现门）：
- run_id沿既有受信事件envelope；interaction_revision正整数单调，pause_revision标识等待轮次；opaque pause_ref不暴露native checkpoint key。
- phase=waiting/resuming/active/terminal；waiting完整非空awaiting集合，resuming仍完整但submitted，active/terminal为空。
- items含唯一pause-item ID、tool/request关联ID、kind、allowed_decisions、安全display/schema/result字段；validation为显式
  allowlist机器错误与安全字段path，不用原始异常message或将敏感value塞args。display只含name、description、editable、既有input_schema与受限result_preview；不得复制raw args/defaults/调用凭据。validation只含code=json_schema_invalid与instance_path，不放无界异常message/输入值；result_preview沿现裁剪规则且显式标truncated/来源。
- 动作结果携command_id、关联pause_revision和结果类别固定accepted/native_consumed/validation_failed/rejected/unknown/cancelled；native_consumed不是工具执行成功；工具实际结果走原真实结果事实。全集接受仅表示resuming。
- 每条state是完整集合，不允许缺items表示“沿用上一集合”；不得从activity尾项、receipt、正文、run.started猜解除。

resume目标必带expected_pause_revision、pause_ref与全集item decisions，command_id仍Idempotency-Key；
受信tenant/actor/subject来自既有认证上下文。Worker核Run归属、当前revision/集合、allowed kind、全部ID与分组，
同事务接受intent并写resuming source；结构/集合错误拒整批且不推进。HTTP admitted/pending/未知ACK仍不改变消费者等待。
同command同digest重放，异digest409；两个不同command竞争同一revision最多一条接受；旧revision拒绝不消费新轮次。
保持全集提交，不新增partial接口；一个interrupt的多action有序，跨interrupt恢复必须native interrupt-ID map，不能flatten。
input schema最终消费点校验失败时，同request_id但新pause_revision完整waiting；显式validation source可重放。
普通reject不是Run取消；cancel/failed/completed以唯一终态原子清集合，迟到native通知不能复活。

checkpoint与Run/Chat不共享事务：durable resume intent→native已持久证据→source状态commit，恢复规则见TECH。
GraphResumeEvent只是通知；无完整checkpoint/task/interrupt/command证明不发active。未知执行窗口不盲重投；仅已失联且静止attempt的bounded协调失败
保unknown动作证据并Run失败，不伪造成功结果或承诺自动retry安全。消费者可见resuming持续到可验证native边界。

Root已比较并批准首发前Agent HTTP4.0.0原/v1单路径corrective，不另增/v2。删除旧不完整interaction
消费解释、无双轨fallback；不能仍声称固定3.0兼容。owner机器/source/schema先发布并固定artifact；BFF repin、完整pending与head/cursor
同事务，然后Web升级。P3B无wire变化的独立结论保持，不能把本HITL breaking混成P3B已经发布4。
新增payload必须补OpenAPI decoded-payload mapping和negative schema测试，现failure generator随source hash再生成；
当前contract-check仅证明现3.0未漂移，不证明这些候选字段已存在。精确实施集/RED见TECH顶节，当前四docs＋两份可行性tests；没有机器/业务源码授权。


### proof修订后的消费与可见状态边界

NULL_TASK RESUME只证明输入写入；多interrupt map可以没有该行。task RESUME先在内存、后异步持久；
真实RESUME+ERROR不是成功，RESUME+本次INTERRUPT是新waiting。同request_id/native interrupt ID可重复，pause_revision仍须前进。
历史checkpoint保留的INTERRUPT/ERROR行不是本次结果；须原intent预存resume向量/明确定位与已提交因果后继的当前完整tasks共同证明。
外层取消可能没有任何task RESUME/ERROR持久写；无证据保持unknown，不从取消ACK猜动作已消费。
accepted/resuming与commit后的dispatch_started为不同内部事实；后一状态不公开为succeeded。
只有确定尚无dispatch_started且所有native入口执行此前置事务才允许首次投递；start后失联绝不据缺行盲重投。
健康task/有效lease或可能仍写的执行不适用三次终止；限已失联且静止attempt的三次持久判定后才可原authority失败收口。
消费者仍只凭Agent受信完整state source替换集合，不能实现另一套native推断。Root批准4.0单路径版本顺序不变。
本轮仅tests-only验证native语义，未发布新的消费source或业务恢复保证。

### R28 消费证据缺项不由成功输出补齐

现固定PG saver的混合普通输出batch可能不更新已有RESUME槽；返回成功、出现successor或输出valid，
各自都不等于精确command/attempt的完整消费向量已经持久。连续同值validation的新PG证明例仍待Root执行；
内存对照完整向量已通过但不代表PG。若丢observation后仅有旧向量，保持unknown动作证据、零重投，
不发active/native_consumed；terminal仍按原权威清集合，不倒推已消费。无新API/错误码/版本例外，4.0发布门不变。

## AGENT-P3B-D0-R26：内部实际policy契约，待依赖/源码门（2026-10-01）

当前正式main `e977923ea9992cbddaf0cdbc6c8f8d23b3af120e`：HITL/Agent HTTP 4.0.0已发布；
P3A静态持久已验，`af45817260478f1ee755d8e6e6963051e2049062`仅为P3A历史验收基线。当前没有effective观察/持久gate。TECH顶节为当前P3B候选接口与30路径来源，以下候选/P2文字仅历史。
本D0只四docs；受维护fork是待Root ADR/依赖门候选，上游现0.6.6/1.3.2没有本方案完整observer，未修改依赖或安装目录。

目标只增内部 `EffectiveNativePolicyBinding(canonical_bytes, digest)` 与
`RunProfilePort.freeze_or_verify_effective_policy(request, lease, static_binding, effective_binding)`。
返回frozen/matched而不是可忽略bool；typed incompatibility携原build fence映射现contract_incompatible/false，authority lost零写且
不借新adopt终态。HTTP admission不接受caller policy，HTTP-only不导入worker/private/native装配；不新增wire字段/错误码/Redis协议。
P3B不改变已发布HTTP4，fresh schema/执行器须协调替换；正式retry/scope机器字段仍为后继owner-first发布目标，
后续若breaking按既定Agent5.0发布，非本片偷偷激活。

候选library contract必须覆盖每peer main/GP/catalog实际SystemMessage、真实ToolNode归一化后的有序工具schema/description/source、
实际override/exclusion/GP决策与middleware顺序/模板/安全选项/source，并有完整路径清单与completed观测。
单DeepAgents输入tools不是实际tool registry；Runtime hook不是构造期观察，禁止两次constructor/第二selector/闭包推断。
原生runtime对状态/Skill/结果的动态变换仍是执行输入；冻结其**实际选中的政策、模板和规则**，不伪称提前得到每轮最终prompt。
route revision/health/endpoint/凭据排除，真实路由引起的政策变化必须纳入；动态授权照常重验。未知runtime callable不执行而拒绝。

唯一顺序为static commit→全peer preflight/System→全peer仅本地单次native材料化→完整effective一次事务commit→真实backend activation→
原graph执行。所有peer绑定前零sandbox/provider/tool执行；disarmed正式BackendProtocol保Composite权限/能力语义，不走已弃用callable。
缺观察、peer失败、SQL回滚或authority丢失均不得执行任何peer。current authority的失配只用原fence收口。
同Run resume/takeover已有值同bytes验证不覆盖；首次effective尚缺且无执行事实可绑定，started/HITL缺值typed拒绝。
请求身份仍唯一 `request.model_dump_json().encode("utf-8")` 对stored原TEXT UTF8 bytes；JSON解析等价不授权。

后继完整scope/retry目标的retry admission仍scope锁内验证parent/origin两阶段齐备，缺阶段409 run_retry_conflict，不从新路由补父NULL；
合法新Run复制origin两阶段，再由worker重算比较。scope key/head/generation、native checkpoint/terminal原子晋升/retention DAG目标保持。
正式发布仍owner machine/runtime/schema→固定artifact→BFF全部Chat/Scheduled消费者→Web→协调激活；不新增3.0 run_scope_busy。
Conversation最终释放未决只阻最终GC及完整scope/retry/native/retention目标闭环，不阻P3B独立片。
当前无新的HTTP机器源变更；原D0的3.0 contract-check是历史证据，R64当前HTTP4的静态复验由Root后继执行，
不证明候选library接口或第二阶段已实现。依赖primary证据、精确版本/hash、待验供应链与RED门见TECH顶节。


## 历史 AGENT-PROFILE-P3A-R25：内部持久gate实现候选，HTTP3不变（2026-10-01）

基线main a37e8f1；当前代码新增Run内部freeze_or_verify_static_recipe真实facade/adapter，非外部API。
factory同plan在任何preflight/System前await事务commit；request比较唯一无参数model_dump_json原UTF8 bytes，
不接受JSON等价替代；漂移用原build lease映射现contract_incompatible/false，authority丢失不借新lease发terminal。
HTTP-only不读取worker/private设置；HTTP/Redis/protocol/generated/Failure值域未修改，不发布HTTP4或3.0 scope busy。
SQL为fresh canonical变化，旧schema明确拒绝，未引入兼容补值。worker离线/安装wheel通过，真实PG43矩阵仅collect待Root。
P3B有效native policy与两阶段完整身份、正式HTTP4 required retry parent及消费者协调激活仍按下方设计，不因本片通过降义。

## 历史 AGENT-PROFILE-P3-D0：静态持久子门与正式两阶段协议（2026-10-01）

当前main `7e902c08296cacdacfe810ccbb4a6233d1b2ca7b` 已验P2；唯一HTTP机器源仍3.0.0，SQL尚无profile列。
本轮只四docs；TECH顶节P3A精确22路径为拟授权，不是实现或4.0发布。

P3A只新增内部 RunProfilePort.freeze_or_verify_static_recipe(request, lease, StaticRecipeBinding)：
当前factory的同一个PreparedFeaturePlan bytes/fingerprint在任何外部preflight/System前持久freeze/verify；
不接收caller上传profile、不在HTTP加载worker/private配置，不新增网络字段、状态码、scope busy、Redis envelope或fallback。
损坏/漂移typed错误在初次build/resume沿已有contract_incompatible/false终态；失效lease不冒权写终态。
HTTP-only读取其自身业务配置/public JWKS，schema setup只能做schema gate，不为计算recipe导入factory/worker或读private model配置。

唯一请求身份序列化式为 `request.model_dump_json().encode("utf-8")`（不传任何dump参数），
与现 `postgres_run_dispatch.py:43,76,97` 写入/claim所用 `request.model_dump_json()` 完全同源。
锁内读取的原 `request_json` TEXT 直接 `.encode("utf-8")` 后逐byte比较；不先parse再dump、
不使用profile的canonical_json，不按dict/Pydantic对象相等或JSONB等价授权，也不新增helper。
字段重排、额外whitespace、等价JSON转义/表示、显式默认与省略默认差异、未知字段，即便解析后对象等价仍typed拒绝；
缺失/非TEXT/非法UTF8同样拒绝。recipe自身的canonical编码与请求原TEXT身份是不同边界，不能混用。

上述六类原TEXT负向变体须各自RED→GREEN：身份不等不持久profile、不触发任何preflight/System/backend/provider。
首次claim已RUNNING但尚无执行事实仍允许freeze，不把内部phase当外部已执行证据；错误终态仅原build fence。

完整4的profile不是P3A static SHA：pre-System static与post-route effective两个持久阶段共同组成身份。
所有peer实际main/GP/catalog prompt、工具覆盖/schema/顺序/exclusion、GP选择及middleware source/选项必须一次绑定，
commit前零sandbox/provider/tool执行。当前native库还缺已验证的实际输出观测接口，P3B先解决该Agent/library边界，
不复制resolver、用静态集合冒充结果或仅验证第一个peer。动态route revision/health/凭据不纳摘要，授权照常逐次重验。

正式4 retry admission仍required retry_of_run_id，origin两阶段任一缺失/损坏/未知版本→409 run_retry_conflict，
不从当前路由填原父NULL；合法新attempt复制原origin冻结事实，worker新结果不等→contract_incompatible/false。
同Run takeover在首次有效bind前崩溃且零执行可完成第二阶段；已有HITL/native执行而缺第二阶段拒绝恢复。
绑定/比较权威为scope active_run+Run owner/generation+锁后DB clock；旧generation相同摘要亦不可写。
原failure.retryable不是profile完整资格保证，不为缺profile篡改failure tuple。

P3A不要求BFF/Web repin HTTP4，但canonical schema必须fresh安装，旧schema明拒，禁止隐式升级或旧数据补recipe。
P3B/完整4发布仍按下方owner machine/runtime/schema→artifact→BFF所有Chat/Scheduled与其他sender→Web→协调激活；
不得HTTP-only替worker冻结、不让BFF代造digest、不先半激活required字段。proof/Failure3机器值域仍不变。
Conversation最终引用释放未决只阻最终GC/完整4发布，不阻本独立持久子门。D0实际验证与历史Root证据见CURRENT。

## 历史 AGENT-PROFILE-P2-R24：内部装配候选，wire 不变（2026-10-01）

基线`9dcaa34a3664668c3ad2da6adcc71f271ea96224`。已实现生产manifest与factory/worker共享静态计划；metadata/source/
policy验证在全peer外部preflight前，当前Skill/MCP与System授权仍照常重验。插件空批准/二次枚举前load拒绝、late registry
mutation与绑定metadata漂移均为内部失败，不加公开错误码/字段。HTTP仍3.0.0、RunRequest/Redis/failure tuple未修改。
`assembly_recipe_fingerprint`不发布、不持久、不作retry身份；P1完整codec未降级。正式4两阶段目标保持下节裁决，
main+全部peer的effective-native后置持久绑定仍未实现。本片26路径（Root补一个原生contract测试隔离fixture），
不把该fixture修复称contract机器源变化或4.0发布。实际离线与wheel证据见CURRENT；Root最终验收仍待进行。

## 历史 AGENT-P2-D0-R24：内部装配证明与最终 profile 边界（2026-10-01；仅设计）

当前已提交 P1：`ec65d04f9915580eb57629126fffffc20f4c4033`，Root离线1588/6/192与build/wheel已验。
HTTP OpenAPI仍3.0.0，P2不修改机器源/provenance/generated、RunRequest/Redis、202/409或failure tuple。
`PreparedFeaturePlan`、`RuntimeAssemblyPolicy`与`assembly_recipe_fingerprint`只属于Agent内部装配：
静态来源/政策/选择验证失败在现factory错误边界处理，不新增公开错误，不公开fingerprint或secret配置。
该fingerprint有独立domain tag，不冒充未来持久`runtime_profile_digest`，不调用完整codec用空native项补齐。

TECHNICAL_DESIGN的P2放置表是本片唯一拟写入集；metadata与真实build共用plan，不新增网络selector。
先全peer静态prepare，再保持当前Skill/MCP preflight→System route→backend/model/native的顺序与授权重验。
DeepAgents0.6.6 harness会在route后改变prompt/tools/GP等；P2只证明批准recipe/source/policy集合。
R24 Root已裁决正式4采用两阶段：pre-System且全部外部preflight前冻结static recipe envelope；route后仅本地model
构造，任何sandbox/provider执行前另绑定`effective_native_policy_digest`，覆盖main+全部peer实际prompt、工具
override/exclusion、GP与middleware source/政策。retry/resume/takeover须核继承身份相等，route revision/health/
凭据不入摘要。此内部持久绑定/Run SQL是独立后继，P2不发布字段或声称完整Run profile已生效。

生产第三方plugin批准集合默认空；在lazy bootstrap/ep.load/call前仅metadata枚举，拒unknown/重复/同key冲突；
未来扩展须按显式有序identity/dist/version/source清单，late registry mutation拒绝。runtime-only middleware callable
不在静态prepare执行，P2默认拒绝不支持声明；后继若支持须绑定真实后置输出。精确负向counter=0见TECH验收矩阵。
完整4选型已决，后继事务/恢复细化门尚待实施，不新增用户产品决策。Conversation删除/expiry最终引用释放仍独立
未决；只阻最终释放/完整发布，不阻P2。机器4与BFF全normal Chat/Scheduled+retry、其他sender/Web仍须协调发布。
本D0只验证文档/manifest与只读机器版本，没有代码/contract/服务测试；下方P1候选措辞仅为历史。

## 历史 AGENT-PROFILE-P1：内部纯配方交付（现已提交 ec65d04）

在 D0 已提交基线75701413上，P1实现纯canonical profile/摘要、显式包来源descriptor与现build共用选择plan。
完整字段形状只属于内部配方，见TECHNICAL_DESIGN的P1段落；ValueError是内部失败，不新增wire错误码。
HTTP OpenAPI/provenance/generated、RunRequest/Redis/required字段、202/409、failure tuple和当前preflight顺序均未改。
没有digest网络传输/持久freeze/授权gate；完整生产manifest与worker装配仍待后继。不把P1交付称4.0 artifact发布或激活。
当时交付待Root复验；现P1已由Root验收并提交。完整scope/native/retention与协调consumer发布门仍维持D0裁决。

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
正式4执行profile按TECHNICAL_DESIGN的R24两阶段冻结：static recipe envelope在任何外部preflight/System前；
实际effective_native_policy_digest在route后、本地model构造后且任何sandbox/provider执行前绑定全部peer。
两阶段继承值须严格比较，不以recipe冒充最终descriptor；不纳secret/route revision/health/当前授权，也不是本P2新wire字段。
typed terminal outcome在同scope事务
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
