# kokoro-agent 技术设计

## AGENT-PROFILE-P3A-R25：静态Run配方持久实现候选（2026-10-01）

基线main `a37e8f1e308286d922212f2d5365634fd3ff2c21`，Root已通过r2设计门；本片只批准22路径，
20既有+2新（postgres_run_profiles.py、test_run_profiles.py），无新目录/依赖/机器契约/lock改动。
实际代码已把同PreparedFeaturePlan的bytes/fingerprint接入Run行持久freeze-or-verify，commit后才进全部peer preflight/System。
精确请求身份仍为 `request.model_dump_json().encode("utf-8")` 与stored原TEXT UTF8 bytes，无parse等价授权。
窄adapter同连接Run行锁后读DB clock，核tenant/request、owner/generation/nonterminal/expiry；已有值重算摘要逐byte比较、
matched零更新；无执行事实的首次claim（包括RUNNING）可freeze，已有counter/usage/sandbox事实而缺值拒绝。
迟到authority错误不再借新adopt；mismatch保留原build fence，以现唯一finalize_terminal和contract_incompatible/false收口。

SQL只增现Run成对bytes/digest+8MiB/check，schema gate查精确column与CHECK catalog定义；无迁移、旧schema补值或第二真源。
已登记新adapter包源码，原codec/selector/manifest机制不重写。HTTP-only导入测试不加载worker/factory/plan/manifest。
source/wheel实际230资源、115依赖、12动态边及三Feature指纹一致；具体全门见CURRENT。

**验收边界：** worker默认离线1649 passed/6 skipped/234 deselected，完整门exit0；新增真PG文件与schema矩阵共43用例
仅collect、尚未执行。PG的catalog exact CHECK表现、事务/并发/fault矩阵由Root在自有隔离schema实际验证后决定放行，
本段不把Python/fake GREEN冒充真PG已过。P3A当前只Run fence、不实现scope；P3B真实单次native输出、全部peer执行前有效
政策绑定及完整4 scope/retry/native/retention仍硬门。Conversation最终引用释放未决不阻本片，但不允许永久免GC假闭环。

## 历史 AGENT-PROFILE-P3-D0：持久静态配方与完整两阶段后继门（2026-10-01）

**当前基线 `main 7e902c08296cacdacfe810ccbb4a6233d1b2ca7b`，P2 已由 Root 验收提交。**
Root 1627 passed/6 skipped/192 deselected（83.20s）、完整离线链与安装 wheel 的229资源/115依赖/12动态边证据见 CURRENT。
下方 P2 候选/D0/早期4.0各节是历史与目标记录；本节明确当前事实、下一最小片和完整目标，不把计划记作实现。
本 D0 仅四文档；SQL、Python、机器源未动，源码须 Root 通过本门后另行授权。

### 当前事实与切片裁决

- `execution/runtime_profile_plan.py:161–169,227–387` 已返回唯一 PreparedFeaturePlan/recipe_bytes/fingerprint；
  `agent_factory.py:275–294` 已在全部 peer preflight 前 prepare，但尚未持久存储或比较。
- `agent_factory.py:135–180,223–251` 仍逐 peer resolve System → 创建 sandbox → 本地 model/native constructor。
  因此尚不满足“所有 peer 的实际政策绑定完成后才允许任一 sandbox/provider 执行”。
- 安装 DeepAgents 0.6.6 `graph.py:538–559,577–650,654–739` 在真实 constructor 内解析 harness、改写工具/提示词、
  物化 middleware、选择 GP；现返回 graph，不返回完整有效政策记录。P2 native_recipe 明示 post_route，不是最终结果。
- `database/schema.sql:9–54` 的 Run 没有 profile 列；`infrastructure/schema.py:113–132` 只查缺表，
  并未验证 profile 列/约束 drift；`worker/supervisor_control.py:453–481` 的 interrupt fingerprint 也会 adopt 后 build。
  `supervisor_execution.py:67–77`、resume、reclaim 和 fingerprint 必须走同一 factory 持久 gate，不能漏辅助入口。

**下一最短可验收片 P3A：只把已验 static recipe 做真实 Run 持久 freeze/verify。** 它是正式两阶段的第一阶段，
不是另起3.0 scope/active 状态机，不提前发布4.0，不把 static fingerprint改名成完整runtime_profile_digest。
采取现 Run 行内两列及窄 repository 方法；所有真实 factory build 在第一次外部 preflight/System 前 commit。
HTTP admission 只存请求，不读取 worker/private 配置。现有完整4 scope/native/retry门保留，未实现部分明确不激活。

**P3B 是执行前有效政策绑定片，不能和 P3A 的通过相混。** 当前上游没有已验证的完整 observer API；实际材料化边界
必须先由 Agent owner 确认可行实现（需要库 owner 的显式 observation/materialization 接口时，走独立依赖/source审批）。
不复制 harness resolver/selector、不运行两遍构造取样、不 monkeypatch 全局 create_agent、不遍历 graph闭包/repr猜描述。
无需为这个后继技术门拖延 P3A SQL/事务实施。P3B 准入应以所有peer的真实构造输出证明，不是“批准集合”代替实际选择。

### 第8节放置表：P3A

| 项 | 结论 |
| --- | --- |
| Owner | Agent Run 持久执行配方，当前唯一 writer；Root 设计放行、Git、独立复验与提交。 |
| 当前事实 | P2完整source/codec/同plan已验；Run持有tenant/request/lease/terminal，无profile字段，无scope表；HTTP3、SQL-first。Root提供clean基线，writer不操作Git。 |
| 目标 API | 内部 frozen dataclass StaticRecipeBinding(canonical_bytes, fingerprint) 与 RunProfilePort.freeze_or_verify_static_recipe(request, lease, binding)，返回 frozen/matched；失配/损坏与失去authority是不同typed错误，不返回可忽略bool。 |
| 两个位置 | 采用现Run行、既有domain/run模型/窄port、新相邻 infrastructure/postgres_run_profiles.py；淘汰新profile表（无独立生命周期/查询）、把SQL塞497行codec或继续膨胀lease协调器。 |
| 粒度 | 1个职责聚焦生产adapter、1个真实PG测试文件；没有新目录/服务。现fake只实现相同语义，不新增生产内存repo。 |
| 依赖 | factory消费同PreparedFeaturePlan并向Run窄port写；infra依赖domain。domain不import worker/codec/psycopg；HTTP不import factory/manifest或解析private设置。sources只显式补新模块资源，不重写已验codec/selector。 |
| SQL/事务 | 唯一canonical DDL添加可空成对recipe bytes/digest；同Run事务校验trusted tenant/canonical request/owner/generation/锁后DB clock；无网络持锁。旧非空schema拒启动，不迁移/补默认。 |
| 删除项 | 无第二selector、optional gate或“repo没有方法就跳过”；不保留旧schema运行fallback。下方完整scope目标不删减。 |
| 验证 | tests-only RED；新PG双连接/fault矩阵；原默认全门、HTTP-only导入、source/wheel闭包及Root主树独立复验。D0未运行这些代码门。 |

### P3A 事务、入口与失败语义

1. 每次 build 先调用现 prepare_feature，精确使用其 recipe_bytes/fingerprint，校验canonical/domain tag和大小。
   调用持久gate且commit成功后，才执行现全peer `_preflight`；不得从持久bytes反序列化出另一套Feature/selector。
2. 在同连接显式transaction中，以request内受信tenant+run_id取 Run `FOR UPDATE`；锁后重读完整请求原TEXT身份，
   严格按下述唯一序列化式逐byte比较。取锁后单独 `clock_timestamp()` 验 owner、generation、nonterminal、非NULL且未到期租约。
   当前片没有scope，只有真实Run fence；将来scope落地时此adapter必须整体改为scope→dispatch→Run，不留Run→scope路径。
3. 两列均NULL表示本fresh-schema Run尚未到达第一阶段，不表示旧数据兼容。仅无已开始/暂停执行证据的合法首次build可绑定；
   takeover若崩于首次freeze之前且尚无执行事实，可首次绑定；已有outbox/event/usage/sandbox事实却缺binding则损坏拒绝。
   检查现Run durable/event counters、usage totals、sandbox binding；正常HITL一定已有执行事实，缺binding不得补写。
   这只是已执行事实的损坏防线，不凭计数器证明外部没有副作用；“NULL之前零外部调用”由唯一factory顺序测试保证。
4. 两列已存在时重算保存bytes的SHA256并严格验证domain/canonical/上限，逐byte及digest比较当前binding；完全相同只返回matched，
   不改updated_at、不重写或重选。部分NULL、未知domain、非canonical、digest错误/政策漂移一律typed incompatibility。
   同一Run跨新lease generation继承原bytes；不把generation写入配方，不因takeover刷新冻结值。
5. profile不匹配/损坏归已存在的 `contract_incompatible/false`，由现唯一finalize_terminal收口；typed错误优先于build默认assembly_failed。
   lease失效不持久任何profile、更不冒旧generation发terminal；`_fail_terminal` 对typed authority loss直接返回，
   profile mismatch携带原build fence且只用该fence收口，不再从可变_leases/_control_lease借到另一次adopt的authority；错误或取消发生在commit前则rollback，commit ACK丢失时新连接原值verify。
   fingerprint辅助入口遇失配也不继续读native或执行：返回其既有stale结果/日志，后续真实resume仍经过同gate，不能捕获后绕过。
6. profile内只留已批准无secret静态配方；错误/日志不dump bytes、request、URL、工具schema或模型对象。
   事务不延长lease，不持锁做preflight/model/sandbox。commit之后租约再失效仍由现执行/owner效果gate负责；P3A不声称native write fence已实现。

P3A未改变HTTP3请求/返回/strict failure值域，不需要假发HTTP4。它有canonical schema部署变化：必须fresh owner schema，
runtime旧schema明确拒绝，不热patch现有数据。完整4将复制origin的已冻结两阶段事实，现P3A不新增retry入口/lineage列。

唯一请求身份序列化式为 `request.model_dump_json().encode("utf-8")`（不传任何dump参数），
与现 `postgres_run_dispatch.py:43,76,97` 写入/claim所用 `request.model_dump_json()` 完全同源。
锁内读取的原 `request_json` TEXT 直接 `.encode("utf-8")` 后逐byte比较；不先parse再dump、
不使用profile的canonical_json，不按dict/Pydantic对象相等或JSONB等价授权，也不新增helper。
字段重排、额外whitespace、等价JSON转义/表示、显式默认与省略默认差异、未知字段，即便解析后对象等价仍typed拒绝；
缺失/非TEXT/非法UTF8同样拒绝。recipe自身的canonical编码与请求原TEXT身份是不同边界，不能混用。

首次claim后内部执行状态可以已是RUNNING；这不等于已有run.started/outbox/native执行。首次freeze资格以实际Run
持久字段及执行事实判定，不以phase==RUNNING直接拒绝。正常claim且counters/usage为0、sandbox未绑定必须允许首次freeze；
已开始/HITL执行而缺binding的损坏场景仍拒绝。所有失配终态保持原build generation，不借新的adopt authority。

### 正式两阶段及重试/恢复裁决（P3B/完整4硬门）

| 时点/入口 | 必须成立 |
| --- | --- |
| 第一阶段 | 全peer静态recipe在任何System/Platform/Storage/MCP preflight前，当前Run lease/fence事务freeze/verify；完整4加scope.active_run核对。 |
| 第二阶段 | 所有peer完成当前System路由和仅本地model构造，采用真实native材料化一次输出；main、每peer及其GP/catalog子agent的实际prompt、实际工具有序schema/description/source、工具覆盖/exclusion、GP enable/override、middleware顺序/source/无secret选项组成effective_native_policy canonical bytes/digest。全部齐备一次commit，不逐peer先提交后启动。 |
| 执行屏障 | effective绑定commit前任何peer均不得创建/重连外部sandbox、调用provider或执行tool/graph；后置构造所需backend必须为真实延迟绑定且未执行的已登记能力，不用fake实例推断。若实际constructor有I/O则该实现不满足门，先调整owner边界。 |
| 参数排除 | route revision/health/endpoint/凭据、当前授权、actor/assertion不纳digest；当前System/Platform/Storage/MCP权限每次重验。路由引起的实际有效政策变化必须被第二阶段捕捉，不能因route排除一起漏掉。 |
| 同Run恢复 | frozen static严格比较；effective已绑定则严格比较且不覆盖。崩溃在首次effective bind前且尚未执行，允许同Run在当前fence首次完成第二阶段；有native head/HITL/已执行事实而effective缺失是损坏，拒恢复。 |
| 正式新Run retry | admission在scope锁内验证最近父/origin、原输入/精确baseline/窗口与两阶段完整；缺任一阶段即409 run_retry_conflict，不把NULL当相等或从新路由补原父。不会把原failure.retryable改false掩盖资格差异。复制原origin bytes/digests，再在worker重算当前两阶段并严格比较。 |
| takeover/迟到绑定 | 同scope active_run、sameRun owner/generation、锁后DB clock有效才能写/verify；旧generation即便计算结果相同也零写。读取定位不授权，scope→有序dispatch→有序Run锁后重新核身份，active/HITL不得被新normal/retry覆盖。 |

未来Run字段 `effective_native_policy_bytes BYTEA`/`effective_native_policy_digest TEXT` 成对可空，仅代表尚未首次绑定；
完整执行身份是static+effective两阶段版本化tuple，不用一个static SHA冒充完整profile。
保留完整4已有native baseline/head原子写、terminal晋升/active释放及引用感知retention设计；本节不替换为简化执行状态机。

### P3A 精确拟授权文件集（22路径；本D0不修改它们）

以下绝对路径仅是下一代码片范围，2新文件、无新目录；若实现需要额外路径先报Root，不偷扩。

| 绝对路径 | 唯一职责 |
| --- | --- |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/database/schema.sql` | Run静态bytes/digest成对约束，唯一DDL |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/domain/run/models.py` | StaticRecipeBinding与typed incompatibility/authority错误 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/domain/run/repositories.py` | RunProfilePort窄方法 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/domain/run/repository.py` | 组合新窄port，不新增wire |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/infrastructure/postgres_run_profiles.py` | 新：同连接Run锁/DBclock/freeze-or-verify |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/infrastructure/postgres_run_repository.py` | 唯一facade装配/委托 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/infrastructure/schema.py` | 新列/约束精确catalog drift gate，无DDL副本 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/agent_factory.py` | prepare后且preflight前必须持久gate |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/execution/failures.py` | typed mismatch优先映射既有contract_incompatible |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/worker/supervisor_execution.py` | profile authority错误不借新lease发terminal；失配只用原失败build fence收口 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/execution/runtime_profile_sources.py` | 只登记新增生产adapter，延续闭包验证 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/support/fakes.py` | fake相同freeze/verify，不宽松自动忽略 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/agents/test_factory.py` | 所有feature/peer外部counter0、先commit及漂移 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/execution/test_supervisor.py` | initial/resume/reclaim/fingerprint均到同gate与错误收口 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/http/test_http_main.py` | HTTP-only仍不加载worker/private配置 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/contract/test_canonical_database_schema.py` | 成对/长度/无FK与唯一canonical断言 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/integration/database/test_schema_installation.py` | fresh+精确profile drift/非空拒绝 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/integration/database/test_run_profiles.py` | 新：以下独立真PG故障矩阵 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/TECHNICAL_DESIGN.md` | 本门及实际验收 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/API_CONTRACT.md` | 内部API/HTTP3不变与4依赖 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/DATA_MODEL.md` | 新列语义/事务/生命周期 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/CURRENT.md` | 真实RED/GREEN与Root状态 |

### RED与真实PG故障矩阵

先新增测试看实际失败，不用fake绿色代替PG；PG由Root授权后复用既有实例，用自有随机schema/独立连接、finally只回收自有资源。
每类独立case，保存连接PID/阻塞barrier/结果计数；不依靠固定sleep猜并发，不放宽timeout/skip/default门。

| 场景 | 必须观察 |
| --- | --- |
| unit source/顺序 | 新port缺失RED；所有3 Feature与第二peer坏recipe时preflight/System/backend/provider counter=0；已提交才preflight。source/wheel新增模块缺失failclosed。 |
| fresh schema/drift | 同库另一owner schema不动；本schema fresh安装；缺列、错类型、错null/default、缺/改变CHECK各自被verify拒绝；非空安装不迁移。 |
| 首次freeze与新连接 | 真实claim后bind；关闭连接/重建repo严格读回原bytes/digest；same值verify零更新。 |
| 同值双连接 | 两连接同Run/fence并发，一次frozen一次matched，唯一bytes/updated_at，无重复事实。 |
| 异值双连接 | 独立连接阻塞barrier后争同Run，恰一胜一incompatible；不能last-write-wins。 |
| 锁等待后过期 | A锁Run，B等待且租约在等待中到期；释放A后B依据新DB clock拒绝，零写；不注入Python now冒真PG证据。 |
| generation takeover | A算完计划，B合法reclaim换generation；A晚freeze/verify均authority拒绝；B相同值匹配，漂移值拒绝，不覆盖。 |
| terminal/cancel竞争 | terminal先commit→freeze拒；freeze先commit→terminal保留binding；取消不回滚已提交profile且无后续旧owner写。 |
| rollback/cancellation | UPDATE后显式异常/SQL错误/实际task取消分别rollback；其他连接看不到半bytes/半digest。 |
| commit ACK lost | 已commit后模拟调用方没收到返回，新repo新连接verify matched；不另造一个freeze事件或更新时间。 |
| tenant/request串用 | 相同run_id但另一tenant/request canonical不一致、owner错/generation错/paused lease均零写，错误不泄漏内容。 |
| 请求TEXT身份负向矩阵 | 对已claim Run的原request_json分别只改字段顺序、whitespace、等价转义、显式默认/省略默认、未知字段；即使可解析等价也逐例typed拒绝、profile零写及preflight/System/backend/provider计数0；未改原字节正例通过。 |
| 初始崩溃与缺失 | 正常首次claim/RUNNING但无执行事实必须可freeze；claim后freeze前崩溃合法reclaim可首次bind且外部counter0；已有started/HITL/usage/sandbox事实而NULL、部分NULL/坏digest/未知domain拒绝。 |
| resume/fingerprint | 真Run暂停/adopt后相同配方通过，变更后preflight/native调用均0；原终态/固定outbox不重复；fingerprint失败不成为绕过入口。 |
| bounded payload | 超8MiB拒绝且无SQL写；合法最大边界/UTF8/canonical/未知tag拒绝逻辑一致，保存bytes不进日志。 |

下一实施实际命令：现原离线全链（lock/sync/Ruff/Pyright/contract/generated/默认pytest/build）不减；定点
`uv run --frozen --offline pytest -q tests/unit/agents/test_factory.py tests/unit/execution/test_supervisor.py tests/unit/http/test_http_main.py tests/contract/test_canonical_database_schema.py`；
Root授真PG环境后 `uv run --frozen --offline pytest -q -o addopts='' tests/integration/database/test_run_profiles.py tests/integration/database/test_schema_installation.py`。
执行前确认DATABASE_URL为Root指定实例；缺设施明确未验，不以skip算通过。wheel必须真正安装并从/tmp导入新adapter与来源闭包。

### 实施、发布与未决

P3A四doc门→Root精确授权22路径→tests-only RED→代码/真PG/离线/wheel→停写manifest→独立审查/Root复验→提交。
之后P3B实际native输出接口及全peer本地构造/延迟sandbox/持久effective gate→完整4 scope/native/lineage/terminal事务，
HTTP4机器、fresh schema/runtime同门→owner artifact先发布→BFF所有normal/retry/Scheduled producer精确repin→Root协调激活。
P3A没有4.0消费者breaking；未来required retry_of_run_id仍是HTTP4 breaking，禁止缺省/trace fallback。旧schema不得热运行P3A。
技术未决仅P3B真实材料化输出接口（Agent/上游library owner），不是重开两阶段产品决定。Conversation最终引用释放仍由
BFF/Agent/产品决定，阻最终GC/完整发布，不阻P3A；本片profile与Run同寿命，没有新孤立表/永久免GC。

## 历史 AGENT-PROFILE-P2-R24：来源 manifest 与同一装配计划候选（2026-10-01）

实现基线为 Root 已提交的 `main 9dcaa34a3664668c3ad2da6adcc71f271ea96224`；本片仍由 Root 独占 Git/提交。
现在已实现生产来源登记、静态 `PreparedFeaturePlan`、真实 factory 必填计划消费和 worker 同一 runtime policy。
P2 只计算 `assembly_recipe_fingerprint`，没有 Run 持久 freeze、effective-native 后置绑定、scope/native/retention 或4.0激活。

- `runtime_profile_sources.py`：22来源组、232资源条目/229唯一资源（本包151、DeepAgents48、LangChain27、Swarm3），
  12条显式动态边、115个固定runtime distribution版本及extra/marker依赖闭包。有限清单从包resource读取，不在请求期递归扫描；完整本地静态import
  闭包由测试核。composition覆盖worker/执行/生成client依赖；catalog声明值进入实际选择投影，不把未选prompt资产
  当成当前prompt。general与web-researcher资产仅按当前受信选择登记；第三方叶SDK在明确包/版本边界收口。
- `runtime_profile_plan.py`：不可变policy/feature/peer计划；全部peer先校验guard/source/工具冲突、再外部preflight；
  真handoff实例只构造一次且随plan传递。tool/subagent materializer不另选，实际绑定工具metadata与prepare快照比较。
  GP/空tools继承、MCP当前授权、0预算关闭和原preflight顺序保持。backend custom缺批准来源/政策时显式拒绝，
  当前没有部署第三方custom批准项，不按任意引用导入或读取其YAML来填充摘要。
- `agents/native_profile.py`：固定0.6.6的基准模板及policy来源适配，不选harness、不创建图；第三方批准集合为空，
  初始metadata与上游bootstrap再次枚举均在load/call前闸住，后者上游吞错也转为明确失败，避免安装变化窗口执行插件。
  registry首次仅接受干净bootstrap，之后比较有序key+对象身份；真build前后重验。对象身份仅进程内核对，不hash。
  runtime-only middleware callable不在prepare调用；本片拒未支持者，后置实际输出绑定仍属独立后继。
- worker policy由main同一config快照构造，budget供guard、recursion供现Supervisor→executor链；dependencies校验
  policy与真正model/sandbox设置一致，凭据/URL/namespace/绑定对象不进入JSON。没有第二selector或optional-plan fallback。
- 当前recipe根仍为`domain_tag/feature/runtime/implementations/native_recipe`；implementations内为有序sources、
  dynamic_edges和dependencies。native_recipe保存真实filesystem/todo schema与模板、task模板、GP、注入序、policy keys
  和空批准plugin集合；每peer另存已选GP/catalog的基准task描述。它们明确`materialization=post_route`，不是最终有效descriptor。

Root额外批准现 `tests/contract/test_deepagents.py` 的逐case registry/bootstrap snapshot/finally恢复：原生契约探针
注册测试profile后原先跨case泄漏，正式source gate不能把这些fake key加生产白名单。保持原断言/执行，不skip或放宽。
故精确候选为**26路径＝原25＋该既有fixture**；6新文件、无新目录。下方文件表已纳入该补授权，精确补充路径为
`/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/contract/test_deepagents.py`。

验收证据见CURRENT同名节。完整两阶段Run身份、缺第二阶段失败的retry资格、SQL原子绑定与全peer后置顺序仍待独立门；
Conversation最终引用释放只阻最终释放/完整发布，不阻该foundation。下方D0措辞为已批准设计历史，不覆盖本节候选事实。

## 历史 AGENT-P2-D0-R24：生产来源登记与实际装配计划（2026-10-01；仅设计，待 Root 复核）

**当前基线：** `main ec65d04f9915580eb57629126fffffc20f4c4033`，Root 已提交并推送 P1；
Root 独立离线门 1588 passed / 6 skipped / 192 deselected、static/build exit0、wheel 四生产资源一致。
P1 已实现纯 profile v1 codec、包资源读取与 tool/subagent 选择计划；不是“仍待验候选”。下方 P1/D0 的基线、
RED 和候选措辞均为当时历史记录，不覆盖本节。机器仍 HTTP3，SQL 没有 runtime profile freeze。

### 裁决：P2 能交付什么，什么尚不成立

本片交付**生产来源 manifest + preflight 前完整静态装配计划及其内部 fingerprint + 真 build 消费同一计划**。
它不交付“所有模型路由后的最终 descriptor 已在 preflight 前确定”。两者不能混称：

- 当前 `agent_factory.py:AgentFactory._build_feature` 先对全部 peer `_preflight`（Skill resolve/load、MCP resolve），
  再各 peer `build_deep_agent` 内 System resolve → sandbox → tools/guards/subagents → native constructor。
- 已安装 `deepagents==0.6.6` 的 `graph.py:538–559,577–650,654–739` 在获得模型后选 harness profile；
  它可改 prompt、tool description、工具 exclusion、GP 和 middleware。`profiles/_builtin_profiles.py` 还会惰性加载
  builtin 与 entry-point plugin；只记录 `RESERVED_TOOL_NAMES` 或空 native descriptor 会漏掉真实执行政策。
- P2 的 `assembly_recipe_fingerprint` **只标识经批准的静态 recipe、完整来源与 native policy 集合**，不是未来
  Run 的 `runtime_profile_digest`，不调用完整 profile codec 补造空字段，也不写 Run/事件/日志当授权证据。
  `canonical_profile/profile_digest` 的完整 v1 语义保持；P2 的内部准备记录具有独立 domain tag
  `kokoro-agent:assembly-recipe:1`，不是第二状态机、第二 selector 或第二公开协议。
- **R24 Root 已裁决正式4采用两阶段，不再把完整实际 descriptor 声称为 pre-System 已知。**
  第一阶段在全部外部 preflight/System 前冻结 static recipe envelope；第二阶段在 route 后仅作本地 model/政策构造，
  在任何 sandbox/provider 执行前，以同 scope/lease/generation fence 原子绑定 `effective_native_policy_digest`。
  第二阶段必须覆盖 main 与全部 peer 的实际 prompt、tool description override/exclusion、有序实际工具面、GP
  和 middleware 实现 source/无 secret 政策；两阶段一起才表达完整 Run profile，不降义为只冻结批准 recipe 集合。
  retry/resume/takeover 必须核继承值相等；System 当前 route revision/health、授权与凭据不入摘要且照常重验。
  本地 model 构造不等于 provider 推理请求；该顺序调整、实际输出提取、持久绑定及 Run SQL 是独立后继，P2 不实现。
  当前逐 peer resolve/build 的结构尚不满足“全 peer 绑定完成前零 sandbox/provider 执行”，不得称已达成。
- 本 P2 严格只交付前置 foundation，保持现 preflight 顺序，不提前 System I/O、不静态执行 runtime-only callable、
  不覆写上游 harness；原D0为25路径，实施期仅按Root授权补一个原生测试隔离fixture。完整生产 manifest/共享计划有独立价值，但不是完整 Run profile 发布。

### 第8节放置表

| 项 | P2 最小实现决定 |
| --- | --- |
| Owner | Agent 拥有本地执行 recipe/代码来源；FeatureCatalog 只登记 Feature；System 仍拥有实时模型路由，Platform 拥有当前 Skill/MCP 授权，Storage 拥有 delivery。Root 唯一审查/提交人。 |
| 当前事实 | `agent_factory.py:76–99,255–302`；`tools/toolset.py:97–115,139–167`；`agents/subagents.py:66–91,143–164` 为现选择/装配；`execution/runtime_profile.py:224–282,412–497` 为 P1 codec/source/projection。无生产 source manifest、PreparedFeaturePlan 或完整 native recipe。 |
| 目标职责/API | `prepare_feature(feature, runtime_policy, manifest, toolbox, subagent_catalog, delivery_available) -> PreparedFeaturePlan` 同步、无 owner I/O，保存有序 peer/tool/subagent/handoff plan、显式 recipe bytes/fingerprint。factory 内消费该同一对象，不再次按声明分支筛选。 |
| 位置比较 | 采用 `execution/runtime_profile_sources.py`（显式资源登记）、`execution/runtime_profile_plan.py`（静态计划/纯 recipe）与 `agents/native_profile.py`（固定上游版本 metadata 适配）。淘汰继续扩497行 codec混入工厂/包manifest、FeatureCatalog发网络、worker/main堆全部规则及新profile服务/目录。 |
| 粒度 | 三个新生产文件各一变化原因；无新目录。既有工具模块仅提取各自 metadata 单一常量/声明供真实 StructuredTool 与 plan 共同读，不另建通用工具运行器。内部不可变记录用 frozen/slots/kw_only dataclass；不复制 wire DTO。 |
| 依赖 | sources依赖P1包资源 API；plan依赖现工具/子代理 planner与 native metadata adapter，不依赖 WorkerDependencies/repository/provider。factory/worker向下装配；tools/subagents不反向依赖profile。native adapter是上游版本专属边界，不 introspect callable/闭包/图执行状态。 |
| 数据/API | HTTP3/Redis/RunRequest、SQL、幂等、lease/terminal/native saver保持原状。新增对象仅单次build/进程配置内存，fingerprint不持久、不回填NULL、不成为retry身份。 |
| 删除 | 删除factory的第二份handoff筛选、build内部重复生成的选择结果；工具description/schema声明移为唯一来源后旧内联重复文本删除。无optional-plan fallback、空descriptor、猜source或旧新selector并行。 |
| 验证 | 下方RED矩阵、现factory/toolset/dependencies回归、source与真实wheel隔离导入、完整离线门；PG/Redis/真实模型由对应后继/Root执行，不以本片替代。 |

### 实际 owner 与顺序：不能只是多加一个无消费者字段

1. `worker/main.py` 用已验证的同一 AppConfig 构造不可变 `RuntimeAssemblyPolicy`（放 plan 模块），
   包含 run_token_budget、recursion_limit、三个模型执行布尔、显式 toolbox选项、backend政策及 native登记。
   WorkerDependencies新增必填policy/manifest，不存第二份可独立漂移的run_token_budget；factory的guard budget读policy。
2. recursion_limit当前真实链为 `config.py:247 → worker/main.py:211 → supervisor.py:97 →
   supervisor_execution.py:213 → execution/run_agent.py:99–105`。main给Supervisor传**同一policy.recursion_limit**，
   不是再次读默认值；测试捕获Supervisor参数并让现invoke_once收到该值。不修改executor `_config` 或trace规则。
3. 每次 `AgentFactory.build` 在任何peer `_preflight`之前同步prepare所有peer；缺登记、工具名冲突、未知子代理、
   无效guard政策（例如ask_user result-review）一律先失败。`tools/guards.py`提取纯policy校验，prepare与真build共用。
   现“所有peer preflight完才建任何backend/model”保持；之后网络授权仍每次重验，不因fingerprint相同跳过。
4. tool/subagent materializer接收必填已选plan；native handoff由真实 `create_handoff_tool(agent_name=target)` 在prepare
   按有序边生成，读取安全metadata并将**同一真实工具实例**交build，不创建dummy request/namespace/backend/model。
   handoff加入主tool面后再按现catalog选择子代理；保持GP继承parent tools/model、catalog空tools省略即继承、
   显式tools只选已存在者、GP不进入declared授权集合与主/子守卫顺序，绝不把`tools=()`解释为禁用所有工具。
5. native GP/文件/todo/task 的基准schema、description模板、生成规则和policy来源进入 **NativeRecipe**，不是伪造
   最终ToolDescriptor。原生GP模板直接取安装包常量，task模板按同一已选GP+catalog names/descriptions投影；
   filesystem真实顺序为ls/read_file/write_file/edit_file/glob/grep/execute，todo/task及其注入位置按已锁源码登记。
   真constructor仍由现官方调用创建；后置harness有效结果按上述已裁决两阶段门由独立后继绑定，P2不声称已完成。

### 静态准备记录的精确边界

`PreparedFeaturePlan`保留：feature（key/entry_agent/有序agents/handoffs）、每peer的既有ToolSelectionPlan与
SubagentSelectionPlan、真实未绑定handoff工具、RuntimeAssemblyPolicy，以及canonical recipe bytes/fingerprint。
业务对象/工具实例只供后续materialization使用，不进入JSON。每次build构造一次，不作全局mutable缓存。
recipe的显式JSON字段为`domain_tag/feature/runtime/implementations/native_recipe`：feature使用实际声明及已选
工具metadata、GP+catalog声明与继承标记；runtime使用上节批准白名单；implementations包含有序sources、动态边与
固定依赖版本。native_recipe的filesystem/todo/task_template/general_purpose为安装包真实基准schema/description/prompt，
另存injection_order/policy_keys/approved_plugins；materialization固定为`post_route`。这些不是最终tool descriptor，
其批准builtin/plugin选择及生成来源由对应manifest条目覆盖。未知键/对象/secret拒绝；数组保序、set字段显式排序、UTF8与数值规则复用P1。
此对象不是传输DTO；完整`profile_version=1/feature/runtime`输入不会接收`native_recipe`或代填implicit_tools。

实际工具source绑定采用显式登记的工具/工厂身份（known core实例或受信声明提供source_id与已登记factory），
不能只用tool.name冒认同名实现；匹配对象可作进程内identity检查，但不hash对象/地址。memory/MCP/deliver的
静态schema与description原本在本模块，提升为唯一声明后真binder和prepare共同读；web/core已创建的真实实例
只经P1安全metadata读取，不重新造实例。native/handoff顺序及subagent筛选仅由现计划与官方工具factory决定。

### Manifest：显式闭包、无secret、无运行时扫描推断

manifest记录稳定source_id、package/symbol、distribution实际版本、逐文件相对路径与原bytes SHA256。
登记表由源码维护、运行时只读枚举路径，不递归glob整仓、不Git/inspect/`repr`/callable地址/model_dump。
资源路径闭包应按以下能力拆登记项；共同guard/native装配为共享项，未选择的本地工具/子代理资产不污染当前计划。
跨distribution闭包使用多个P1 `ToolImplementationSource`，不把langchain文件冒充kokoro_agent资源。

| 登记组 | 必须覆盖的实际来源与边界 |
| --- | --- |
| local assembly | `agent_factory.py`、`swarm.py`（仅peer场景）、`policy.py`、`agents/definition.py`、`features/definition.py`、新plan/source/native适配及P1 codec；选择算法 `tools/{toolset,toolbox,registry}.py`、`agents/{subagents,subagent_catalog}.py`。Feature/prompt实际值直接来自已选对象，未选catalog内容不作为本轮prompt。 |
| guards/skill adapter | `tools/{guards,middleware,permissions}.py`、`hitl/{__init__,input,request,presets}.py`、`skills/{backend,middleware}.py`，及其实际引用的本地规则/协议文件。Run/lease/证明/客户端实例不hash；这些边界的实现资源可以登记，不能漏掉影响guard或tool行为的helper。 |
| core/memory/web | `tools/ask_user_question.py`及hitl；`tools/memory.py`；已选`tools/web_fetch.py`、`tools/web_search.py`及实际provider实现所在模块；选中prompt资源（如`prompts/web-researcher.md`）按bytes登记。不得只hash工具入口而漏HTML提取、限制或schema代码。 |
| MCP固定wrapper | `mcp/{tools,config,servers,egress}.py`和hitl相关闭包；schema来自ListToolsArgs/DescribeToolArgs/CallToolArgs同源声明。动态server/tool目录、URL/凭据/Resolve响应不hash，当前授权仍走preflight。 |
| delivery | `tools/deliver.py`、`clients/{storage,storage_delivery,storage_transport}.py`及实际本地引用闭包；只在实际声明且client可用时选中。不得为拿metadata造伪StorageClient/backend/run。 |
| backend | `sandbox/backend.py`及选中`archive.py`/`workspace.py`/`docker_backend.py`/`e2b_backend.py`/`custom_backend.py`的行为闭包；native `deepagents/backends/{protocol,state,composite,utils}.py`以及所选local_shell/sandbox实现。custom factory与teardown必须各有显式来源与已审无secret政策，不读取custom YAML全量当摘要。 |
| native | `deepagents/graph.py`、`_models.py`、`_tools.py`、`_excluded_middleware.py`、`_messages_reducer.py`、`_subagent_transformer.py`、filesystem/subagents/skills/patch_tool_calls/summarization及其本地依赖；`langchain.agents.middleware.todo`、官方graph assembly；peer的`langgraph_swarm/{handoff,swarm}.py`。明确版本0.6.6/0.1.0及当前安装langchain版本；不靠版本号替代源码bytes。 |
| model/native policy | `model/factory.py`；DeepAgents `profiles/_builtin_profiles.py`、harness/provider注册与合并规则、实际builtin policy模块（含Anthropic与OpenAI Codex）。对其完整批准政策集合登记，而非提前猜某个System路由。未知entry-point/plugin/后注册mutation先failclosed；不执行未知plugin再尝试hash它。非内置扩展由受信装配显式提供source与政策登记，禁止按名字任意信任。 |

**插件与 registry 的可执行约束（R24）。** `agents/native_profile.py` 是唯一上游适配边界：

1. 生产第三方 plugin 的批准集合默认 **空**。在任何可能触发 lazy builtin bootstrap 的入口、`ep.load()` 或
   plugin callable 之前，先用纯 distribution/entry-point metadata 枚举两个组
   `deepagents.provider_profiles`、`deepagents.harness_profiles`；不 import plugin、不调用 registry lazy accessor。
   unknown、重复 entry identity、同注册 key 多来源或 builtin/plugin 同 key 冲突立即失败；失败时 bootstrap、load、
   plugin call 及 owner/provider/sandbox side-effect counter 必须全为0，不能先执行再检测，也不依赖上游吞错跳过。
2. 未来批准扩展须另经 Root 来源审批，提供显式**有序**清单：group/name/value 的完整 identity、distribution
   规范身份/实际版本、声明注册 keys、已登记的逐资源 source digest；不能仅按工具名或 entry-point 名放行。
   纯枚举的集合和 multiplicity 必须与批准清单精确匹配，再按批准顺序组织 recipe/允许装配；metadata 返回乱序
   不改 fingerprint 或调用顺序，批准顺序变化须改变 fingerprint。未批准第三方安装存在即拒，不做静默忽略。
3. builtin keys/source 同样显式登记。校验后建立封闭 registry 快照与来源身份；之后任何新增、覆盖、删除、重排或
   未登记 mutation 均在使用前失败；真 factory 使用时再次核封闭状态，不能拿一次启动校验长期放行。
   adapter 负责禁止装配窗口内晚注册，并检测直接 registry 漂移；不新增另一套 harness selector，实际选择仍由上游完成。
4. runtime-only middleware callable 的结果不是静态 metadata。P2 默认拒绝无法由批准静态声明完整描述的 callable，
   不能为生成 fingerprint 在 prepare 执行它；未来允许的动态扩展须在第二阶段本地 materialization 后把实际输出
   的 middleware source/政策绑定到 effective digest，并仍在 sandbox/provider 执行前完成。不得 hash callable 地址、
   闭包/对象 dump 或固定空输出。静态声明支持范围与当前锁定 builtin 集合必须逐项测试，不为纯片硬编码 fake 值。

上表是实现必须展开的**来源覆盖契约**，不是声称所有资源清单已经交付；P2源码manifest必须给出真实存在的
有限逐文件清单及distribution引用。现包`__init__.py`和本地import/re-export/helper亦计入闭包；验收以静态import
边界检查与显式动态边清单判缺漏，第三方叶依赖以登记版本/对应包资源边界收口，不在请求期任意追import。
变更未登记行为文件必须使coverage RED，不能通过“登记整个仓库”掩盖未选能力隔离。native私有API只集中适配
已锁版本并受wheel/升级测试约束，不复制上游执行loop、profile resolver或工具selector。

backend policy使用明确白名单：kind；local_shell timeout/max_output/inherit_env/virtual-mode；workspace local/S3形态；
docker已批准image标识/ttl；e2b已批准template标识/timeout；custom受信factory+teardown来源与部署提供的非敏感policy_id。
policy_id由该白名单canonical值确定（custom需显式登记），不是固定`default`忽略配置变化；部署地址、workspace实际路径、
S3 key/endpoint、API key、连接URL、容器/session ID与custom原始配置均排除。带凭据或URL形态的标识不得进入白名单。
需要新增环境政策字段或新的custom配置语义时另向Root报精确路径，不在本片扩AppConfig/读取secret文件。

### P2 拟放行精确文件集（本 D0 只写四文档）

以下每项均为绝对路径；不改未列源码、SQL、contract、lock、Supervisor/executor、依赖版本或目录。

| 文件 | 必要变化 |
| --- | --- |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/execution/runtime_profile_sources.py`（新） | 显式生产source登记、依赖组与coverage边界，复用P1资源读取。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/execution/runtime_profile_plan.py`（新） | RuntimeAssemblyPolicy/PreparedFeaturePlan、一次静态prepare、domain-tagged recipe fingerprint；不生产最终runtime_profile_digest。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/agents/native_profile.py`（新） | 固定上游版本的原生模板/政策/source adapter；不创建fake native实例。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/execution/runtime_profile.py` | 仅提取现严格canonical JSON编码窄复用入口；完整v1字段/语义不降级，不新增空descriptor兼容。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/agent_factory.py` | preflight前prepare；全部peer真build消费同一plan；保现网络与native顺序。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/worker/dependencies.py` | 必填typed policy/manifest，删除重复budget配置真源；不把clients dump进profile。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/worker/main.py` | 同一config快照构建policy/source；Supervisor与factory使用同一budget/recursion值。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/tools/toolset.py` | materialize必须消费prepare给的计划，保授权/重复名；不重新选择。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/tools/toolbox.py` | metadata/plan从同一实际toolbox读取，保缺metadata failclosed。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/agents/subagents.py` | bundle消费同一已选plan，GP/空tools继承语义显式保留。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/tools/guards.py` | 抽现policy非法组合纯校验供prepare与build共用；不提前创建绑定guard。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/tools/memory.py` | schema/description唯一静态声明由真binder与recipe共读，namespace不入。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/tools/deliver.py` | 同上；无伪run/backend/client，绑定及副作用规则不变。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/mcp/tools.py` | 固定三wrapper metadata唯一声明，解析/远端授权与调用不变。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/execution/test_runtime_profile_sources.py`（新） | 生产登记覆盖、源码闭包/缺文件/版本/插件未知 failclosed、资源变化。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/execution/test_runtime_profile_plan.py`（新） | 全Feature静态recipe/计划/无secret/政策变化/选中隔离/guard顺序。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/agents/test_native_profile.py`（新） | 原生模板/handoff/GP及模型后置差异诚实边界；受控原生组件无provider执行。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/execution/test_runtime_profile.py` | P1严格codec回归，不能把assembly fingerprint误当完整profile。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/tools/test_toolset.py` | 必填共享plan消费、metadata与真binder相等、现授权/重复名回归。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/agents/test_factory.py` | 唯一WorkerDependencies测试装配点；真实factory plan identity、全部peer预检、现native行为。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/worker/test_dependencies.py` | worker实际policy→factory/Supervisor/调用参数同源、禁secret注入。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/contract/test_deepagents.py`（Root补授权） | 仅逐case native registry/bootstrap snapshot与finally恢复，保留所有原生契约断言。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/TECHNICAL_DESIGN.md` | 本实施门与验收后状态。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/API_CONTRACT.md` | 纯内部fingerprint与HTTP3/最终4发布门。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/DATA_MODEL.md` | 零持久化/生命周期与freeze欠项。 |
| `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/CURRENT.md` | 当前提交、真实RED/GREEN/Root证据，不预填成功。 |

原D0共25路径；Root实施补授权后为26路径：14生产（3新）、8测试（3新）、4docs；无新目录。这是跨真实owner声明所需集合，不以文件数证明完成。
若落实native API或custom政策发现必须动本表外文件，先给确切路径/原因，Root调整卡后才写，不加fallback绕过。

### 必须先 RED 的验收矩阵与先后

| RED | 必须证明的GREEN |
| --- | --- |
| 未登记core/custom factory/teardown、资源缺失/版本不符、登记重复 | 在任何peer Skill/MCP/System/sandbox/model调用前失败；无client请求，无空descriptor/repr替代。 |
| unknown plugin、重复entry identity/注册key、builtin/plugin同key | 在lazy bootstrap/ep.load/plugin call前纯metadata拒绝；所有副作用counter=0，不能只断言最终异常。 |
| metadata枚举乱序、批准清单顺序变化、late registry新增/覆盖/删除 | 枚举乱序保持批准序列和fingerprint；批准顺序改变则fingerprint变化；任何late mutation在真build使用前失败。 |
| runtime-only middleware callable被静态执行或以空descriptor登记 | prepare调用counter=0并拒不支持声明；本片不声称后置输出绑定已做，不用静态执行绕过拒绝。 |
| 源码/选中资源/无secret政策或harness集合变化 | assembly fingerprint变化；source/wheel读取同一资源结果一致；无Git目录仍成功；只改secret/URL/run/lease不改变。 |
| 另写tool/subagent/handoff selector或挂载顺序漂移 | factory消费同一个PreparedFeaturePlan及子plan；真工具metadata/顺序/重复名与计划匹配；GP与catalog空tools继承不变。 |
| 第二peer非法guard/未知source，但第一peer已preflight | 先全部prepare/validate再任何I/O；失败顺序spy为0 calls，现全部peer preflight后建model/backend的测试继续绿。 |
| recursion/budget仅入配方但执行仍用默认值 | 非默认值经main同一policy同时送factory guard与Supervisor，existing invoke_once实际config仍相同；0预算关闭保持。 |
| native policy按System route改变实际descriptor | 测试明确静态recipe相同并不证明最终descriptor相同；记录有效差异，不以fake model固定默认描述通过完整freeze门。 |
| 包漏资源/closure漏本地helper/隐式entrypoint | installed wheel隔离导入所有生产登记来源，逐文件与source核bytes，移除资源/增加未登记行为边/未知plugin必须失败。 |

顺序：Root通过本D0 → tests-only RED → metadata单源/manifest与plan → factory/worker装配 → 全默认离线门 →
Root独立hash/源码审查/重跑与wheel证据 → 单片提交。此后按R24已裁决两阶段实现完整profile与scope事务freeze，
再native fence/head/retention、机器4与消费者协调；Conversation最终释放未决只阻最终释放，不阻本片。
拟聚焦命令：`uv run --frozen --offline pytest -q tests/unit/execution/test_runtime_profile.py tests/unit/execution/test_runtime_profile_sources.py tests/unit/execution/test_runtime_profile_plan.py tests/unit/agents/test_native_profile.py tests/unit/agents/test_factory.py tests/unit/tools/test_toolset.py tests/unit/worker/test_dependencies.py`。
随后原 lock/sync、Ruff format/check、Pyright、contract-check、failure generator-check、全部pytest、wheel/sdist；
wheel验证以临时target安装、隔离工作目录真实导入，不能只读zip目录。schema/wire/lock字节由Root核，不运行基础设施。

### R24 设计门、实际验证与后继依赖

本轮仅收敛四文档。原四hash独立评审为P0=0/P1=2；本修订逐项处理插件有序/零副作用边界与完整实际profile
两阶段语义，关闭情况由Root复核裁决，不把文档整改自称代码验收。三设计现在一致：P2无SQL/wire变化、25路径
仅前置foundation；完整4保持实际策略冻结目标，并新增独立后置绑定门。Python/contract/SQL实现均未改。

未决产品项仍为Conversation删除/expiry最终引用释放；仅阻最终释放/完整发布，不阻P2。两阶段选型已决，
后继Agent owner须先给Run schema/事务精确门：两个阶段的原子身份、缺失第二阶段时的失败/retry资格、全peer本地
materialization与并发恢复/回滚验收；未绑定不能冒充相等或自动以当前政策补值。Root协调System当前路由授权边界，
不需要把route revision变成冻结字段；后继获独立文件/SQL授权才实现，不把其文件暗加本P2集合。
本次实际验证仅UTF-8/围栏/尾空白、25路径清单及四文档hash、HTTP机器版本只读核对；没有运行代码门、schema检查、
pytest/build或设施/provider请求。P2的RED/GREEN与wheel闭包证据全部待Root放行代码后取得。

## 历史 AGENT-PROFILE-P1：纯配方实施记录（现已提交 ec65d04）

基线 `main 757014139cce9e6eb73a1b62e9420917a1e984a0`（D0 已由 Root 审查提交）。P1 只使用下方
批准的 11 路径，新增 `execution/runtime_profile.py` 与对应 unit 测试，无新目录/依赖/SQL/机器源。
`plan_toolbox`、`plan_toolset`、`plan_subagents` 产不可变有序选择；现工具 materialization 与 native subagent
bundle 调同一函数。profile_tools/profile_subagents 直接按同一 plan 投影，未选项不参与 fingerprint。
未知子代理校验随规则移入共享 plan，缺工具整项过滤、catalog 顺序、GP 覆盖/guards/declared 权限不变。
MCP 当前授权/resolve 和 factory preflight 顺序未改；固定 wrapper 名与实际 materialization 精确比较，漂移失败。

纯 `canonical_profile/profile_digest` 输入是显式完整内部 v1 白名单（不是 wire model）：root 为
`profile_version/feature/runtime`；feature含key/entry_agent/handoffs/有序agents，agent含key/prompt/tools/
implicit_tools/mcp/subagents/delivery/model/backend/permissions/pause_tools。backend为kind/policy_id/implementation；
工具descriptor为name/description/input_schema/return_direct/response_format/implementation/options。
implementation含source_id/package/symbol/distribution/version及有序resource path/SHA256；options仅批准的
fetch_allow_private/search_provider/policy_id。runtime含run_token_budget/recursion_limit/三model布尔策略/toolbox/
delivery_available；预算0沿现配置表示关闭，recursion_limit仍正数。除JSON Schema自身外，各对象拒未知/缺字段。
`pause_tools`仅frozenset作集合排序，其余tuple/list保序，null保留；拒非有限数/孤立surrogate/绑定对象，无trim。
内部配方字段不是新增HTTP/Redis字段，不将schema文本中合法的属性名当凭据对象进行猜测。

ToolImplementationSource只接受不可变、显式登记的包资源闭包；implementation_descriptor核登记identity、
重复/缺失/非法路径，从安装包读取原bytes与distribution版本，无Git/inspect/网络。P1并未发布完整生产manifest；
完整handoff/native implicit来源覆盖、preflight前完整装配和持久freeze仍在后继，不以fixture descriptor冒充它们。
ToolboxProfileOptions只由现build_toolbox明确记录fetch/search业务选项，key/URL不进入；直接构造且未给metadata的
内部toolbox仍可按现行为build，但profile_toolbox拒绝为它猜profile。metadata与实际挂载名称/顺序不一致也拒绝。
新纯函数不作为admission/lease/native/terminal gate，未半激活4.0。真实factory与现MCP/Skill preflight回归由现测试覆盖。
实际RED/GREEN、架构门失败返修与完整离线证据见CURRENT的P1记录；Root独立复跑前仅为候选。

## 历史 AGENT4-D0：实施起点与分阶段门（2026-10-01）

本节以 Agent `main 224d0f19ff2199c38b95f621015ea7856f589454` 为当前基线，优先于下方标明历史基线的实施记录。
HTTP machine 仍为 3.0.0；`64665cb0` 已交付唯一 `finalize_terminal`，`224d0f19` 已将全部 worker RunRequest 通知
收敛到 durable canonical consume。现有 terminal 的 usage/outbox/Chat identity/seq/cleanup 已同事务提交，
提交后才发布 Redis；没有持久 scope、origin/retry、baseline/head 或 runtime profile，现 saver 也无 run-bound fence。
因此本轮不是补第二终态器，不另建 advisory admission/3.0 scope 状态机，不将 BFF FIFO 当作 Agent 执行互斥。

BFF `e7a325ce232f4be052aa498020bb24e217de4cfa` 已由 Root R18 验收内部 Chat terminal-gated FIFO 与
非法 post-terminal source 阻断；其证据属于 BFF 单仓，不证明 Agent scope、Scheduled 同 session 或跨 owner 组合完成。

### 两级门与唯一产品未决

- **实施子门**：每片先有本仓三设计一致的目标、精确文件集和 RED；Root 放行后执行，不等完整 4.0 全部完成。
  下一片 P1 只实现下面的纯 profile 编码/选择规划与现 build 同源重构，无 SQL/wire/持久 freeze/新运行状态。
  Conversation 生命周期不阻断 P1；后继 scope/native 各片按本节与下方正式目标独立评审，不以本轮文档授权代替源码派工。
- **完整 4.0 发布门**：正式机器/runtime/canonical schema 一致、全入口 scope-first、profile freeze、native fence、
  terminal head/active 原子性、bounded 引用感知 GC 和最终生命周期释放均有真实 owner 验证。此前阶段提交不激活 4.0，
  不发布只多 required 字段而执行器仍旧的 artifact，不混用 Agent3/BFF4。
- 唯一产品未决仍为 Conversation 删除/expiry 对在途取消、上下文保留/恢复窗口、执行记录及 native 引用释放的语义。
  该决定阻断最终释放/完整生命周期发布，不阻断独立 P1 或已收敛部分的 RED/设计。通知契约、幂等重放、drain、
  引用重验与删除竞态由决定后的 owner 工程设计完成；本轮不新增 DELETE/tombstone/expiry 默认策略。
  活跃/引用保护不是永久免 GC；Run TTL 只清无引用事实，不能自行释放 latest/committed head。

### P1 精确放置表：纯编码与共享选择规划

| 项 | P1 结论 |
| --- | --- |
| Owner | Agent 执行配方；单一 Agent writer，Root 审查、Git 与最终验证。BFF/System/Platform 不维护第二 profile。 |
| 当前事实 | `tools/toolset.py:build_toolset` 合流 core、memory、configured web、固定 MCP wrapper、可选 deliver；`agents/subagents.py:catalog_subagents/build_subagent_bundle` 根据声明及真实可用工具过滤。当前无 profile 模块；FeatureCatalog 只是 registry。 |
| 目标职责 | 纯函数校验显式 profile v1 白名单输入并输出 canonical bytes/digest；工具与子代理选择产出不可变有序计划，真实 build 与 profile 投影消费同一选择函数/计划，不复制筛选条件。 |
| 两个位置 | 采用既有 `execution/runtime_profile.py` 承接编码/显式 source descriptor；选择规则保留在现 toolset/subagents，toolbox 承接无 secret 配置元数据。淘汰 FeatureCatalog 编码、factory 混合编码与网络以及新 profile 服务/目录。 |
| 粒度 | 只新增一个生产模块与一个专用测试文件；既有能力文件增加纯规划并让现构建复用，不搬目录、不造通用 DTO/registry/helper。内部值用 frozen/slots/kw_only dataclass。 |
| 依赖 | profile 可读可信 Feature/Agent 声明及纯选择结果；选择模块不反向依赖 profile 编码。无 repository、HTTP/provider、环境/Git读取或 native saver；不调用工具、不创建虚构 namespace/backend/client 来取 descriptor。显式包资源读取可校验已登记来源，测试注入在 tests 内。 |
| 数据/API | 不修改 HTTP3、Redis envelope、SQL、RunRequest、digest 持久化、admission/lease/terminal 或当前 preflight 顺序；新 digest 仅纯函数结果，不成为运行授权 gate。 |
| 删除 | 被抽出的旧内联选择分支随真实 build 改用计划而删除；不留旧/新两套 selector。现授权、MCP resolve、工具绑定、middleware 与 native graph 路径保持。 |
| 验证 | 下面精确 RED 矩阵、现 factory/toolset 回归及本仓离线门；无 PG/Redis/provider 需求。Root 实测前只称候选，不将 P1 称完整冻结 profile。 |

P1 允许文件仅以下绝对路径（本 D0 不修改；后继须 Root 明确授权）：

1. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/execution/runtime_profile.py`（新增）：PROFILE_VERSION=1、白名单完整输入校验、canonical 编码、摘要及显式 ToolImplementationSource 值/资源摘要；不接受以缺字段/default 拼成“完整 profile”。
2. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/tools/toolset.py`：纯有序来源选择计划与现 build 的 materialization；保留实际工具名冲突校验、动态 MCP 当前授权与绑定。
3. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/tools/toolbox.py`：进程工具顺序与显式无 secret 选项投影；不转储 provider 对象/闭包/凭据。
4. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/src/kokoro_agent/agents/subagents.py`：纯已选且工具齐备的 catalog 计划，现 bundle 按计划绑定真实工具/guards；general-purpose 覆盖及 declared 权限语义不变。
5. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/execution/test_runtime_profile.py`（新增）：编码/白名单/source/选择投影纯测试。
6. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/tools/test_toolset.py`：同源计划与实际 materialization、重复名/顺序/delivery 条件回归。
7. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/tests/unit/agents/test_factory.py`：现真实 factory 的主/子代理工具集合与计划一致、授权守卫及当前 preflight 行为不变；使用现测试 doubles，不请求 provider。
8. `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/TECHNICAL_DESIGN.md`、`/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/API_CONTRACT.md`、`/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/DATA_MODEL.md`、`/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/docs/CURRENT.md` 仅 P1 当前态与实测记录。

**同源与分割边界。** 工具计划记录实际合流次序与启用条件，由 `build_toolset` 绑定当前 request/lease/backend/client；
profile 只投影该同一计划的静态业务选择，不将绑定实例纳入摘要。子代理计划按 catalog 原顺序选择，缺任一工具则整项过滤，
保留每项 source/description/prompt/声明工具顺序；现 `catalog_subagents/build_subagent_bundle` 只按此结果构造 native 对象。
P1 不另建 digest 专用 selector，也不为读取 schema 伪造远端 owner 返回。handoff/native implicit 的完整 descriptor、
全部登记源码依赖闭包、worker 策略装配和包内来源覆盖仍须后继完整 profile 装配片交付；P1 的 fixture 完整 profile
只验证纯编码契约，不冒充生产入口已生成完整 profile。缺源/未登记项明确失败，不用 repr、Git SHA 或空 descriptor 兜底。
后继才扩 `agent_factory`/`worker dependencies/main`；此历史P1计划按R24修正为preflight前静态计划与
route后实际native政策两阶段，持久freeze/verify属独立后继；P1不接gate，也不证明完整实际descriptor已在System前确定。

| P1 先 RED 的行为 | GREEN 验收 |
| --- | --- |
| profile 输入无完整白名单/未知类型、NaN/Infinity、孤立 surrogate | 明确拒绝，无默认补齐；不 trim/Unicode normalize；对象排序、数组保序、frozenset 字符串排序、null 保留。 |
| prompt/model/options、tool schema/source/order、已选子代理内容或过滤结果变化 | digest 必须变化；相同完整输入稳定，未选 catalog 项变化不污染本次选择。 |
| 凭据/route/URL/namespace/lease 对象误入输入 | 不存在可接收这些对象的宽泛 model_dump 路径；动态数据不进入白名单 profile，禁 repr/闭包转储。 |
| selection 与真实 build 两套分支 | 精确比较工具次序/名称、delivery 开关、web provider 缺席、缺工具子代理、duplicate name 与 GP/declared 语义；实际 build 使用同一 plan 函数。 |
| source 未登记/资源缺失/文件内容变化 | fail-closed 或 digest 正确变化；仅登记的包资源，源码与 wheel 资源读取验证，无运行时 Git/网络。完整生产 manifest 覆盖仍留后继，不宣称 P1 覆盖所有 native 工具。 |

拟命令（P1 后由 Root 按任务卡执行；本 D0 未运行）：`uv run --frozen pytest -q tests/unit/execution/test_runtime_profile.py tests/unit/tools/test_toolset.py tests/unit/agents/test_factory.py`；
随后 `uv lock --check`、`uv sync --frozen`、`uv run ruff format --check .`、`uv run ruff check .`、
`uv run pyright`、`uv run kokoro-agent-contract-check`、`uv run pytest -q`、`uv build --wheel --sdist`；
wheel 资源读取只用自有临时安装验证，不启动服务。DB/schema/contract 字节保持测试或 hash 验证，非 integration 声明。

### 正式 scope/native 后继门的补充

scope PK 仍为 `(tenant_id, namespace, session_id)`；namespace 已含可信 subject，actor/assertion 轮换不换 scope。
现目标未增加 scope_generation：有效执行 authority 是 scope.active_run_id＋该 Run owner/generation＋expected native head。
run_id-only（cleanup 则 cleanup_id→run_id）无锁预读仅定位，scope→dispatch→Run 锁后重读；跨 scope/同组按完整 key 排序。
cleanup claim/complete/reschedule、receipt GC、展示 Chat 写与 terminal evidence replay 都在入口矩阵中，不漏子行入口；
有效历史 terminal 重放核原 attempt 身份，不要求它仍是当前 active。删除后 session 是否可复用及其 ABA 防护由生命周期方案明确。
SQL-first 安装须覆盖列/类型/default/CHECK/UNIQUE/index 漂移断言；现 `verify_agent_schema` 只查缺表，不足以通过 4.0 schema 门。

## AGENT-DURABLE-INGRESS-P0：备用入口收敛（2026-10-01）

本片只收敛现 3.0 worker 的备用 `dispatch(RunRequest)` 入口，不改 HTTP/Redis wire、DDL、
scope、retry、retention 或模型执行。正常 serve 已通过 `_consume_request` 以 Redis frame 中的
`run_id` 回读 PostgreSQL `run_dispatch` 的 canonical `RunRequest`，再以 `claim_dispatch` 在同一事务
完成 `pending -> claimed` 与 Run lease 建立。当前缺口是 `SupervisorControlMixin.dispatch ->
_on_request -> try_claim`可跳过该 durable intent。

收敛后两条调用路径共用 `_consume_request`：无 pending intent 的通知不持久 user message、
不建 Run、不 build；Redis 伪造 envelope 不参与业务语义；真实 pending intent 只有一个
consumer 赢得 claim/start，重放不重复执行。resume/steer/cancel 继续走现有 control 路径。
`try_claim` 仍有测试与底层 fixture 用途，本片不扩大为 repository 全面删除。

## 历史实施记录：AGENT-TERMINAL-ATOMIC/P0（2026-10-01，已提交 64665cb0）

基线 `main dd5afc3528fe3a835756bc3ff55dfacaa8ca76d3`，起始clean；只修已有HTTP3.0终态持久化，
不是另一条3.0兼容实现。以下局部设计与原4.0目标并列标明范围，完整Agent4设计门仍未通过。
Root 已据 unit/真实 PG RED 放行现 source/tests 与窄文档替换；无 DDL/机器源/Git/设施写权限。

| 放置门 | 本片明确范围 |
| --- | --- |
| Owner/当前事实 | Agent Run唯一terminal writer；原dd5afc35基线的try_mark_terminal先commit再usage/outbox/Chat，Root真实probe已复现terminal=true却无terminal outbox、无reclaim；cancel已有两outbox原子但Chat后投，NACK另有无payload终止。 |
| 目标职责 | 一个typed finalize_terminal协调器替代所有无payload terminal原语；最终已观测usage、terminal/outbox/Chat session seq/cleanup同连接事务，commit后网络。 |
| 两案与粒度 | 采用现postgres_run_leases.py协调，events与Chat提供窄同cursor primitive；淘汰新terminal服务/目录/通用UoW及复制Chat SQL。扩现run models/ports，内部值dataclass，无新源码文件。 |
| 依赖 | worker/execution传业务outcome；infrastructure协调同owner连接，domain无cursor/native类型；Chat.append_on_cursor仅package-internal复用唯一SQL。 |
| 数据/API | 现表/索引/HTTP3机器字节/公开字段不变；复用Run、usage_segment、outbox、Chat event/message/sequence、control、cleanup。不存在scope/head故本片不伪造、不宣称同scope串行。 |
| 删除 | try_mark_terminal、fence_and_mark_terminal、cancel_with_delivery_barrier、_claim_terminal及各port/façade/fake/caller；generic stage_critical_frame禁止terminal写；删除execute_active_effect全部层和唯一live caller，无alias/default/fallback。 |
| 验证/阶段 | 本节入口矩阵tests-only RED→唯一coordinator生产替换→Root真实PG/Redis/HTTP与完整门；通过本片仍不发布Agent4或宣称FIFO/retry/GC实现。 |

### typed接口与所有入口

现 `domain/run/models.py` 新内部值均使用 frozen/slots/kw_only dataclass：
`RunUsageSegment(input_tokens,output_tokens)`（generation 来自 authority 当前 lease）、`RunTerminalOutcome(payload,usage)`、
`TerminalAuthority`（精确lease执行者/持久cancel command/持久NACK receipt的具名tagged值），
`TerminalCommitResult(status,retained_frames,lease)`；status明确committed/replayed/lost/deferred，
不以bool混淆ACK未知与竞争失败。safe_failure只承接现strict closed tuple；usage缺席表示该入口没有可结算的
新模型段，不等于补零。实际字段已落当前源码，待 Root 代码审查，不新增wire model或数据库Row穿透。
唯一port为 `finalize_terminal(run_id, authority, outcome, delivery_snapshot) -> TerminalCommitResult`；
传入身份仅用于同事务与已存request/command/receipt精确核验，不让调用者任意强抢owner或generation。

| 现入口 | 替换后的明确动作 |
| --- | --- |
| run_agent.invoke_once自然完成 | stream context退出/drain，收集该段usage；构造completed outcome，调用finalize回调；仅committed/replayed发布既有持久frame。删除claim→record→emit分离序列。 |
| invoke_once执行/投影异常 | 现run_failed_payload安全归码，收集当前已观测usage，failed outcome同一finalize；事务异常不被重新归码成另一个terminal outcome。 |
| supervisor_execution._start_run/_fail_terminal | build失败使用当前或合法adopt的lease、failed outcome、usage=None；首次模型调用前失败不伪造用量；resume build失败同入口。 |
| supervisor_control._on_resume及serve/_guarded_control_apply兜底 | 现invalid decision/恢复失败仍安全失败终态；过期无pending interrupt依旧pause，不误改terminal；统一_finalize调用。 |
| _on_cancel | 可信持久command作为authority；delivery snapshot在锁内重验，receipt applied与cancelled terminal outbox同txn；取消控制CAS赢后才取消本地task/清理/发网络。没有当前worker可靠模型段时usage=None，保留已持久段，不杜撰跨worker未上报用量。 |
| _terminate_contract_incompatible | 同一coordinator的quarantined disposition：同txn核持久rejected receipt与原rejected fence，terminal/cleanup与superseded私有terminal audit outbox原子写；不新增公开Chat、不恢复Redis、不推进poison consumer，delivery不阻塞该隔离终止。 |
| recovery._republish_outbox/_reconcile_run_receipts | terminal已在同txn有Chat，仅校验/读取固定terminal identity、复用seq并发Redis；nonterminal critical保留现投影恢复。禁止为缺失terminal Chat补写而假装原子。 |
| execution.events.emit/live | 不再经generic emit/stage写terminal；live仍reserve→独立fenced Chat append→事务外Redis，删除持锁effect。 |

### usage、锁和唯一事务

- 沿现 `(run_id,lease_generation)` usage_segment PK：一次invoke段一个generation，pause先结算本段，adopt/resume
  使用新generation；终态段在finalize事务结算。重复同段数字一致只读，漂移抛UsageIdentityConflict并零mutation。
  `add_usage`只允许非terminal新pause段；terminal后仅既存同段相同数字只读，首次新段拒绝、漂移conflict，aggregate不变；共享现唯一cursor级usage SQL，不复制算法。
  completed wire token_usage来自锁内结算后的累计值；失败/取消wire仍严格现3.0形状。callback尚未上报的远端消耗
  本片不声称精确获知；无新metering协议，也不把缺席usage写为零覆盖旧段。
- 事务前校验typed outcome及strict failure；Run锁后DB clock重验租约。当前无scope表，锁序是Run→Chat
  identity/sequence→outbox/usage/control/cleanup；delivery journal写也先Run锁，snapshot锁后重读并比较，避免
  锁子行再回锁Run。4.0后再在此统一加scope/dispatch/head边界，本片不加空占位scope。
- 同Run锁内分配固定event index/durable seq、timestamp与usage totals后，才调用现纯 `project_chat_fact` 构造
  完整projection（无I/O）；不在事务前无锁猜source_index/累计usage。`postgres_chat_repository.py`提升窄
  `append_on_cursor`复用 `_append_projection/_next_seq/_save_message`，events提供同cursor outbox写入，
  leases协调全部提交；不把cursor暴露到domain port。cancel receipt不产生Chat fact，保留receipt index在terminal前。
- delivery barrier不止比journal/queued outbox：每个succeeded delivery必须有匹配稳定event identity/source_index
  的Chat delivery事实，内容精确一致；started/变化/缺Chat返回deferred且无terminal。外部Storage与补投不在锁内。
- 正常可投影terminal事实、最终usage、outbox、Chat terminal seq/identity、control receipt（适用时）、cleanup同txn成功或
  全rollback；cleanup网络commit后。稳定terminal event_id由run identity确定，receipt ID由run+command确定。
  已有胜者按原身份读回，失败竞争者不改payload/seq/usage/generation。相同命令重放回原结果，不能将自然完成
  胜者改成cancel；terminal已commit但ACK未知先查同Run已存事实，不盲重执行模型或产生新terminal。
- outbox可能被现receipt GC消费：重放先核Run+Chat固定事实，仍保留的outbox才列retained_frames供网络重发；
  已确认消费的frame不重新插入。命令结果和usage精确比较仍用现持久表，不要求永久保留outbox、不改retention。
  未提交崩溃留下非terminal lease，可按现reclaim恢复；提交后崩溃HTTP已可读terminal，Redis靠queued outbox恢复。
- Chat的message/event各有kind计数器；本片只证明terminal Chat在finalize返回前已提交，之后调用者启动下一Run时
  old terminal event.seq < new started event.seq。不声称当前3.0已阻止并发同scope admission；4.0 scope门仍待实施。

### 精确拟放行文件与RED矩阵

以下均以 `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/` 为根，当前不授权源码。
生产现文件：`src/kokoro_agent/domain/run/{models,repository,repositories}.py`；
`infrastructure/postgres_run_{leases,events,effects,context,repository}.py`、`infrastructure/postgres_chat_repository.py`；
`execution/{run_agent,events}.py`、`worker/{supervisor_execution,supervisor_control,supervisor_context,supervisor_recovery}.py`；
`tools/middleware.py`只更新被删claim_terminal引用注释。若实际构造签名需改worker/main或其他生产文件，先报告扩集。
复用现project_chat_fact与schema，不改projection wire、DDL、contract、generated、lock或业务fail tuple。

现测试精确集：`tests/support/fakes.py`；`tests/unit/execution/{test_control_commands,test_deliver_event,test_invoke,
test_r0_fault_matrix,test_steering,test_subagent_hitl,test_supervisor}.py`；`tests/unit/tools/test_memory.py`；
`tests/acceptance/test_http_ingress.py`、`tests/integration/database/{test_delivery_outbox,test_run_outbox_filter}.py`；
`tests/unit/infrastructure/test_postgres_run_context.py`、`tests/contract/test_postgres_adapters.py`。
首轮在现acceptance/delivery_outbox/invoke/supervisor加入旧缺陷RED，fakes最终删除旧terminal/effect方法而不是兼容alias；
不得先改fakes假装生产原子。Root批准RED后改生产及所有11个实际旧terminal调用测试文件。

| 必须RED的行为 | Root真实/单元GREEN断言 |
| --- | --- |
| 自然、异常、build、恢复build、invalid resume | 每入口terminal/Chat/outbox/cleanup同时出现；first-frame失败保持index0，不伪造started；safe failure tuple不变。 |
| pause多段/重复终态 | 第1段pause、第2段resume terminal累计正确；同generation相同用量幂等、漂移全rollback；terminal后首次新段拒绝且aggregate不变；cancel不覆写既存usage。 |
| Chat写后outbox/cleanup故障、usage后Chat故障 | 真实PG故障注入，全表/计数器rollback，Run仍非terminal；新连接可恢复，不靠mock commit。 |
| commit后ACK/Redis丢失、进程重建 | 新repo读回固定terminal identity/Chat seq，零重复usage/outbox/seq；HTTP已见，Redis queued按序补发。 |
| delivery/cancel/natural/旧generation竞态 | 真PG双连接barrier；started/变化/缺delivery Chat不得terminal；receipt→terminal顺序与单赢家，过期拒写。 |
| live/terminal | terminal先赢则durable live拒；live先Chat commit则event seq先live后terminal；Redis无锁迟到允许。 |
| finalize完成后新Run与重放 | old terminal event.seq < new started event.seq；重放seq不变；不混比message seq，不冒称scope FIFO。 |
| NACK | 缺receipt/伪造rejected_seq零mutation；合法quarantine保留原fence，只有superseded私有terminal audit，无新公开Chat/Redis；delivery started/缺Chat不永久defer；重放按receipt+Run+audit核对且不推进poison consumer。 |

拟命令（仅Root拥有运行授权）：`uv run --frozen pytest -q tests/unit/execution/test_invoke.py tests/unit/execution/test_supervisor.py`；
真实资源显式使用Root自有fixture运行 `uv run --frozen pytest -q -o addopts='' tests/acceptance/test_http_ingress.py
 tests/integration/database/test_delivery_outbox.py tests/integration/database/test_run_outbox_filter.py`；随后lock/frozen sync、
Ruff format/check、Pyright、contract checker/generator drift、默认pytest、wheel/sdist及真实owner全部门。

Root已裁决NACK为同一finalize中的quarantined disposition：终止与私有audit同txn，重放核receipt+Run+audit，
不要求公开Chat、不推进毒化流；不是第二terminal API。NACK在同txn核(run_id,durable_seq,event_id)匹配
被拒outbox，缺失/漂移零mutation；同txn将被拒及fence后仍open帧supersede，私有audit稳定幂等且retained_frames恒空。
reconcile_receipts删除提前写fence路径，仅报告候选，finalize失败不得留下独立fence。与natural/cancel按Run锁first-winner，
既有赢家不可覆盖，delivery barrier不阻断quarantine。正常可投影terminal仍必须Chat同txn。局部三设计已收敛，
本片已按设计门、tests-only RED、生产替换及 Root 实测逐阶段通过，最终证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。retention最终释放未决不阻独立P1，完整4.0发布仍待验；任何发现必须改DDL/HTTP机器的情况
先报告，不偷拆兼容路径。后继唯一writer按tests-only RED→生产→Root集成逐阶段放行。


### 本片 source 审查收敛（实现候选，非完整 owner 验收）

- normal/cancel 在同 Run 锁内先核与 outbox `(run_id,durable_seq,event_id)` 精确匹配的 rejected receipt；已有合法 rejection 返回 lost，错 identity 不阻挡正常 winner。reconcile 只报候选，不独立写 fence。当前没有新 receipt ingest API；并发边界以各自已持久 receipt 和统一 Run 锁事务观察点定义。terminal 先提交则 NACK 不覆盖赢家。
- `verify_terminal_frame(frame)` 是 Run port 的只读验证，不是第二终态 API：持久 Run terminal/fence、保留 outbox、固定 event identity/index/time 与 tenant/namespace/session/run/source_index/canonical Chat ID/payload 精确一致后才可 recovery 发布。孤儿或漂移 fail-closed，绝不补写 terminal Chat。outbox 已 GC 不重建；重复 finalize 只返回仍 queued 原帧。
- `run.started` 的 outbox 已写而 Chat 失败时，同 finalize cursor 按 immutable index 复用 Chat.append_on_cursor 先补 started 再写 terminal；后继 recovery 复用原 Chat seq，不形成 terminal→started。delivery barrier 仍要求真实 journal 与完整作用域/canonical identity 的 Chat 事实，不把 queued 当完成。control receipt 不投影 Chat。
- live 两步 reserve→fenced Chat 不合并；Run context 与 Chat active fence 都先取得 Run 锁、再读数据库 clock_timestamp 判断到期，删除锁中网络 await。Chat adapter 不再接收只供过期检查的本地 clock；测试显式更新自有 Run 到期事实。
- 除原入口文件，实际扩现 `tests/conftest.py` 同 schema Run/Chat fixture、`tests/unit/infrastructure/test_postgres_run_context.py` 锁后 DB-clock RED、execution/INDEX 与 R0-FAULT-MATRIX 两窄说明。Root 独占真实 PG/Redis/HTTP 与 Git；无其他 owner/DDL/contract/lock 更改。

职责收敛：postgres_run_leases 保持唯一 finalize 同连接事务编排；postgres_run_events 承接 delivery_ready_on_cursor、terminal_chat_on_cursor 与只读 verify_terminal_frame，façade 的只读验证指向 events。events 不 import leases，不复制 SQL、不新增模块，也不放宽现 800 行门。

本片 delivery GC 收敛：reconcile 先取得同一 Run 锁；active Run 的已 ACK delivery.created 保留原 event_id/index/time/payload，consumed watermark 仍推进，非 delivery 正常 GC。terminal 后按最终 consumed 水位重扫而不依赖本轮推进，避免 ensure 重建第二 delivery/Chat。没有新表/API/ledger、没有永久跳过 Run purge、没有退回本地时钟或弱化 barrier。真实测试同时覆盖 natural/cancel、持真实 Run 行锁的 GC↔ensure 与 GC↔finalize、最终 ACK GC 后 replay 不重建；本片上述矩阵已由 Root 执行，最终实测证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。

最后两处边界：add_usage（含 pause 段）先锁 Run，再通过 context.database_now 读取数据库时钟校验 active expiry；terminal 只允许已存精确 segment 重放，不接受新段。quarantined replay 同事务严格核 private audit kind/payload、NULL index、timestamp=terminal_at、durable_seq=Run counter 且越过 rejected fence；身份/内容漂移返回 lost，不补写、不公开、不换 winner。Root 已跑 true PG RED（usage 一例/private audit 六例），本片上述矩阵已由 Root 执行，最终实测证据见 CURRENT 的 terminal 历史记录；已由 Root 提交为 64665cb0。

## AGENT-RETRY-DESIGN：原消息正式重试目标（2026-09-30，文档候选，未放行实现）

基线 `main f3be3b97dd67df69ed3c6cb88c59f3bc2db97703`、初始工作树 clean。当前 HTTP artifact
3.0.0 只有失败事实，没有正式原消息 retry：`_persist_user_message` 给同 message_id 换 run_id 会被
Chat immutable identity 拒绝；native Human ID 按 run_id 派生，在同 session 中会重复追加 Human。
只换 stable Human ID 仍会继承失败 attempt 的 AI/tool/private state。下列是替换目标，不是已实现能力；
此前各片的“无 schema/契约变化”仅适用于其历史范围。

### 放置与方案裁决

| 项 | 本片决定 |
| --- | --- |
| Owner | Agent 的 run 能力唯一写 admission、attempt lineage、native locator/lease、执行证据；BFF 唯一写原 user 与新 assistant/run/outbox 的 Product 关系。此阶段 Agent writer 只改四现文档，Root 提交。 |
| 当前事实 | `protocol/control.py`、HTTP `ingress.py`、`postgres_run_admission.py`、`supervisor_execution.py`、`run_agent.py`、`checkpoints.py` 是真实链路；唯一 SQL 在 `database/schema.sql`。现 native saver 无 run lease 写入 fence，执行/恢复 config 只有 thread_id。 |
| 目标职责 | 继续 `POST /v1/runs`，typed `retry_of_run_id` 纳入 canonical fence；同一 logical user 多 attempt，恢复完整 pre-turn native baseline，新 attempt 独立 lease/events/approvals/usage。 |
| 两案/目录 | 采用既有 run 模块与 native checkpoint DAG；淘汰 retry 新服务/模块及 messages 截断：后者遗漏 files/todos/路由/子图/pending tasks，并可能重复副作用。scope 协调另设执行表，淘汰复用 chat_session：后者是可查询展示 projection，生命周期不应控制执行锁。 |
| 粒度 | 现 `domain/run/models.py` 增 locator/lineage 值；既有 admission/dispatch/lease/event 文件承接事务。拟新增 `infrastructure/run_checkpoints.py` 窄 run-bound saver adapter 与 `execution/runtime_profile.py` 纯 profile 编码模块，分别承接 native 原子写和无 secret 配方指纹；无新目录、通用 helper 或第二 runtime。 |
| 依赖 | worker/Factory 传入已认领 Run 与完整 locator；adapter 委托已安装 AsyncPostgresSaver 的公开 API，不复制序列化/SQL。protocol 不导入 native 类型；HTTP root 不加载 Feature、模型或 worker 私钥。 |
| 数据/API | 新执行 scope 行、dispatch lineage/baseline、Run attempt head/profile；详见 DATA_MODEL。只新增 required nullable lineage wire，不把原关系放 trace；owner artifact 4.0.0 后消费者串行切换。 |
| 删除 | 替换 run-scoped native Human ID、重复保存 user 的路径、只有 thread_id 的执行/恢复 config、无 lease fence 的生产 checkpoint 写入；不放宽 assistant/system/tool immutable identity，不删除旧 attempt 证据。无 old-data/trace fallback。 |
| 验证 | 本节矩阵先 RED，后纯 native＋真实 PG/Redis/HTTP。机器/schema 未同步前文档门未通过；Root 最终复跑，owner 单门不等于浏览器/真实 provider 全链。 |

### 输入身份、并发与权限

`retry_of_run_id=null` 是 normal；非空字符串指向最近失败 attempt，不是始终指 origin。
Agent 从父 dispatch 解析 `origin_run_id`，首 attempt 的 origin 等于自身。admission 在同 scope 行锁内：
先验证当前调用授权并处理同 run_id/canonical fence 的 receipt replay；新 attempt 再验证父与原逻辑轮均 latest、
同 tenant/subject/namespace/session、原 message_id/content、feature_key、requested_model_label 与完整有序
selected_skill_source_refs 一致。父必须有 4.0 延续的 strict Failure 合法 tuple、terminal=true、terminal outbox 与 Chat terminal 已同事务持久，
无 active/paused/HITL；不信任客户端 retryable、phase、trace 或 Redis。
同 scope 普通发送与 retry 共用占位/CAS；pending dispatch 也算 active，两个不同 key 的 retry 只接纳一个。

当前 actor/assertion 可更新，不参与“同旧 actor”限制；新 attempt canonical identity 冻结当前 actor/assertion，
replay 仍核原 envelope 不变。BFF 当前 IAM 权限和 Agent 服务身份 gate 先执行，worker 的 System/Platform/Storage
当前授权继续重验；旧 token/proof、批准结果、signed URL 和 lease 从不复制。当前 MCP 是 Feature 静态声明，
不是已发布 launch 选择字段。正式4按R24两阶段：worker在全部外部preflight/System前冻结Feature/Agent/子代理、
本地tool schema/source、MCP声明、模型选项及批准native政策集合的static recipe envelope；System route后仅本地
model/政策构造，sandbox/provider执行前另绑定全部peer实际有效native政策。完整Run profile包含两者，不以静态
recipe替代最终prompt/tool override/exclusion/GP/middleware source。请求模型label冻结，route revision/health与
凭据不入摘要；当前授权每次重验。retry/resume/takeover对两阶段继承身份严格比较，漂移失败关闭、不改选。
retry claim/createRun在scope→parent/origin dispatch→Run有序锁内复制原冻结事实与baseline/window；两阶段SQL、
绑定/比较事务及缺失有效政策时的retry资格由独立后继门实现，不能把NULL当相等或恢复时重选。当前代码尚无持久gate。

### 原生 baseline、失败上下文与真实入口

2026-09-30 只读核验安装源码：DeepAgents 0.6.6、LangGraph 1.2.2、checkpoint 4.1.1、
checkpoint-postgres 3.1.2。`AsyncPostgresSaver.aget_tuple(config)` 有 checkpoint_id 时精确取值，缺失时取最新；
`Pregel._first` 从完整 channels 应用新输入，且会丢弃所选 checkpoint 的 unfinished tasks。
所以选择的 **pre-turn baseline 必须 quiescent**（next/tasks/interrupts 均空，无 pending writes），不是按失败 phase
限制 retry。failed attempt 的后继 checkpoint 可有 AI/tool/interrupt；它们留作证据，绝不作为 retry baseline。
不随意把 run_id 放 checkpoint_ns：非空 namespace 会触发 native 子图路由；根 namespace 是 `""`，子图沿 native 原值。

1. normal admission 在 scope 锁内持久化原 user、dispatch、原 logical turn 与 baseline locator；baseline 指向最后
   committed quiescent head。首次无 head 时记录 explicit empty，并在同连接事务经 native `empty_checkpoint()`/`aput`
   写 fenced genesis，保存真实 checkpoint_id；不以“缺字段/None”代表空而误取 failed latest。
2. scope 保存 `committed_user_seq`。normal 的 native 输入从 Agent 自有不可变 user rows 取
   `committed_user_seq < seq <= 本轮原 user seq`，按 seq 排序，每条 Human ID 确定性取
   `native-input:sha256(tenant,namespace,session,message_id)`。因此失败后用户说“继续”仍能看见原问题，
   只补 Human，不补 failed AI/tools/private fields。snapshot 未包括的这组输入边界随 origin dispatch 冻结；
   retry 使用 origin 的同 baseline/同 seq 边界，稳定 user 只出现一次。不按浏览器历史或 trace 重建。
3. retry admission 拷贝 origin 的 baseline locator/seq 边界，复用原 user row（run_id 为 origin），不重新分配
   user seq/time/content。native 正式入口携带精确 baseline config 与上述稳定 Human 输入，由原生 graph 生成分支。
   normal/retry 都不得读取 thread 全局“最新 checkpoint”；所有通道及原生父链由 native saver 保留。
4. 每次 native `aput`/`aput_writes` 先在同 PG transaction 验 scope active_run、Run owner/generation/DB-clock expiry，
   再委托 native saver 写入；root checkpoint 写同时更新该 attempt head。子图写保留 native namespace/parent map，
   只更新其 native 行，不冒充 root head。parent 必须为本 attempt 当前 head 或其固定 baseline，不接受失败兄弟分支。
5. interrupted attempt 的 resume/fingerprint/takeover 只读该 attempt 已持久 head 与 native child mapping，仍走原
   Command resume 与当前批准校验；retry 是新 Run 的 normal input，不是旧 Command resume。首次捕获/保存之后崩溃
   只重读已保存 baseline/head，绝不重捕获 scope 最新；尚无 attempt head 则从固定 baseline 首次执行。
6. 自然完成且 native context 已退出、pending native writes 已 drain 后，以精确 head 的完整 snapshot 验 quiescence。
   typed terminal outcome 进入单一 scope/fence 事务：最终 usage 段幂等入账、delivery barrier、terminal CAS、固定
   terminal outbox、Chat terminal事实（session seq/identity）、cleanup intent；仅成功同时晋升 committed head/user_seq，
   失败/取消不晋升；该事务释放 active。commit 后仅发布 Redis，固定终态重放复用原 Chat identity/seq。HITL 保持 active，不允许新 normal/retry 穿过。
   现 finalize_terminal 已消除先 terminal 后 payload 的窗口；4.0 在它的同连接事务追加 scope/head/active，不恢复旧终态原语。

执行锁不是仅 admission/native 的局部约定；逐入口 SQL 顺序见 DATA_MODEL「入口锁矩阵」。

| 执行入口 | 共同前置及结果 |
| --- | --- |
| claim/adopt/renew/pause/reclaim、cancel/terminal | run_id-only 先无锁读 locator；scope-first 后重新读 dispatch/Run 的完整身份、lease/generation/state，失配不写；reclaim 候选不提前锁 Run；取得所有前置锁后单独读取 DB clock 重验 expiry，禁止复用等待锁前的 Python now |
| event/receipt/chat/tool/usage/control/sandbox | 同前置再锁各自事实行；已终态允许的 evidence/reconciliation 仍核原 attempt fence，不被新 scope active 身份替换 |
| native write、GC | 相同 locator 重验和 scope-first；native 另核 head/parent CAS，GC 另核引用与 retention。跨 scope 按完整 scope key 排序，无 Run→scope 反序 |

真实必须修改的消费链是 `supervisor_execution._start_run/_spawn_agent/_guarded` →
`execution/run_agent.invoke_once/_config`，以及 `supervisor_control._on_resume/_interrupt_fingerprint`
和 reclaim 的 `_start_run`。完整 locator 是独立 typed 参数，不塞 trace；只传 snapshot.config 而 `_config`
仍丢 checkpoint_id 不算实现。Factory/WorkerDependencies 必须装配 run-bound saver，所有子图沿它写入。

**原生/原子连接底层 API 定点通过，生产 adapter 未验。** Root R3 在同现 PG5432 的自有随机数据库，
经生产 `apply_agent_schema`/drift 安装后实际 exit0，9场景；结果
`/tmp/kokoro-retry-native-pg-spike-r3-result.json`，临时 driver `/tmp/kokoro-retry-native-pg-spike.py`。
已安装 DeepAgentState Delta 的根/子图 × empty/已有历史四场景证明 exact fork、完整先前 messages、原Human唯一、
failed后继排除、baseline channels/pending不变、旧失败子图证据保留、另一连接restart精确locator读取。
explicit genesis raw channels/pending为空，native投影messages=[]为默认值，不是genesis缺失。
同连接公共 `AsyncPostgresSaver.aput/aput_writes` 加 test-only scope/head 的 application abort、后置head CAS lost、
SQL statement abort、实际task.cancel四rollback和success commit均过；数据库已删除、remaining=false、cleanup=[]。

因此采用每次 write 独占连接的外层 `conn.transaction()`、委托原生公共API、同commit head CAS的底层可行性已证实；
淘汰先查lease再另连接写saver，禁止复制私有 `_cursor`/SQL。此spike不含生产adapter、Run lease/generation、HTTP、
完整DeepAgents middleware或provider；真正scope/lease/head fence、两连接锁序矩阵、HITL/profile/GC仍待实现与验收。
此前Memory子图首次误把child.marker当root.marker的失败，以及PG前两次fixture安装失败（prepared多语句、继承
role search_path=pg_catalog）均保留；后者改用生产installer/显式public，每次自有库均已清理，不据此重写历史PASS。

生命周期仍有未决项：Run TTL 不能释放 scope 的 latest/committed 引用；候选为上下文随 Conversation 保留、
BFF 可靠通知删除，由对应 owner 决定释放。该方案正待用户裁决，本文不创建 DELETE API，不以永久跳过 purge
冒充 retention 闭环；后继 purge 必须保护 active/被引用 scope。未决项阻断最终引用释放/完整 4.0 发布，不阻断上方 P1 实施子门。

旧外部副作用、workspace/store、交付 artifact 不由 native fork 撤销；old journal/artifact/evidence 保留。
新 Run 不复用旧 approval/usage/journal 成功记录，按当前权限及工具的正式幂等契约重新调用；可能重复外部效果，
不宣称 exactly-once/free。若能力不具备所需重入语义，由其现正式权限/幂等失败，而非隐藏或删除旧证据。

### Root 已裁决的 terminal、live 与 profile 实施边界（2026-10-01）

`RunTerminalOutcome`、`NativeLocator`、`AttemptExecutionState` 使用 `domain/run/models.py` 内
`@dataclass(frozen=True, slots=True, kw_only=True)`；新内部值不机械复制 Pydantic wire model。
locator 为 thread_id/root namespace/checkpoint_id，attempt state 为 origin/parent、baseline、seq window、head、
profile digest；terminal outcome 为 completed/failed/cancelled、strict safe failure（适用时）、最终 usage 段及 expected head。
namespace/config 转换只在 execution/native adapter；`domain/run/repositories.py` 定义真实窄 port，facade 负责组合。

- `invoke_once` 在 stream context 退出后准备 outcome，不先调用无 payload 的 terminal CAS。usage 段使用现
  Run/generation 段身份精确幂等比较，与 terminal staging 同事务；不能成功 CAS 后 usage 失败再改发 failure。
  delivery barrier 必须确认delivery Chat事实已持久，不以queued outbox存在冒充完成；未满足时保留active，
  不发布terminal，沿既有delivery journal/outbox/Chat收敛后重试。
  当前 cancel、failure/build-failure 已统一进入 `finalize_terminal`；4.0 扩展同一收口，重复 terminal 只回原持久身份。
  `postgres_run_leases.py` 负责typed terminal事务协调，`postgres_run_events.py` 提供现outbox写入的同cursor窄协作，
  不各自提交。锁内取得固定 index/时间/usage 后由现 `project_chat_fact` 构造ChatProjection；同连接复用 `postgres_chat_repository.py`
  唯一 `_append_projection/_next_seq/_save_message`，沿已实现窄package-internal `append_on_cursor` 给协调器，
  不复制SQL、不把cursor放入domain port。`RunTerminalOutcome`只含业务值，无native/DB类型。
  锁序scope→dispatch/Run→Chat→native→outbox/usage/cleanup；先锁Chat identity/sequence，再取native及后组，
  delivery检查需要的事实锁也遵该序。terminal Chat/outbox/usage/head/active全成功或全rollback。
  Chat session seq必须在释放active的同事务获得；Run outbox的durable_seq/index/fence只是per-run，不能证明
  跨run顺序。禁止先释放active再补投terminal Chat，否则下一normal user/start会插到旧terminal前。
- 当前 `execute_active_effect` 及其 Protocol/façade/infra/live 调用已删除，不恢复通用持锁网络 API。保留 `reserve_event_index` → 另事务 `append_fenced(active)` 核 Run 并提交 Chat(seq)
  → 无锁 Redis publish 的两阶段事实写入，不为本片强并 reserve/Chat 事务、不新建 ledger。terminal 先赢则
  fenced append 拒绝；live Chat 先提交则 HTTP Chat replay 的 seq 是 live→terminal，即使 Redis 字节相反。
  Agent 必须拒绝 terminal 后新增 durable live；terminal Chat在同事务已持久，commit后不再补分seq。
  固定终态重放核原generation/identity并返回原Chat seq，不允许下一attempt篡改。其他nonterminal critical仍可
  沿现outbox/Chat恢复；live Redis可丢，依现Chat/native恢复。
  BFF 正式 AG-UI 来源是 Agent HTTP Chat replay，不读取 Agent Redis；现 BFF 有 seq/watermark、ID/digest去重/gap，
  BFF e7a325ce 已验收非法 post-terminal source 的显式 block（非静默丢弃、无新网络 schema）。
  这不替代 Agent scope fence，也不证明 Scheduled 同 session 或整组合；不承诺终态后绝无迟到 Redis 字节。
- **当前/P2/完整4分层：** P1 `execution/runtime_profile.py` 已有 `PROFILE_VERSION=1`、白名单投影和编码，
  不是尚待创建模块。P2 `runtime_profile_sources.py` 负责生产来源登记，`runtime_profile_plan.py` 负责静态recipe，
  factory/worker共用PreparedFeaturePlan；不写Run。完整4另以scope/lease/generation事务实现R24两阶段：
  static recipe envelope在全部preflight/System前freeze/verify；route后仅本地model构造并获取main和所有peer
  实际政策，任何sandbox/provider执行前绑定/比较`effective_native_policy_digest`。不可把下表模型后置实际值
  放进第一阶段当成已知；完整profile身份须包含两阶段，retry/resume严格相等，不重算覆盖继承值。
- 编码为 `json.dumps(profile, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)`
  后 UTF-8 编码并 SHA256（小写 hex）。仅白名单 JSON 标量/对象/数组；不做 Unicode/字符串 trim，拒绝孤立 surrogate、
  非有限数及未知类型。对象 key 排序，原有 tuple/list 保序；只有 frozenset 语义字段先按字符串排序，null 保留。
  profile version 纳入对象；升级视为不兼容，不把旧 digest 重算覆盖。

| 正式4 profile 来源（P1 codec字段与后继实际绑定） | 纳入/排除规则；后置实际值归第二阶段 |
| --- | --- |
| Feature | key、agents 的声明顺序、entry_agent、handoffs 有序对全部纳入。 |
| Agent | key、prompt、tools 的实际顺序/descriptor、mcp 顺序、subagents 顺序、delivery 声明与实际挂载布尔、model 的 provider/name/effort/thinking（或 null）、backend、permissions 全四字段、pause_tools 排序。 |
| SubagentCatalog/build_subagent_bundle | 按当前声明和实际可用工具确定最终集合，纳入每项 name/description/system_prompt/source/tools 及工具 descriptor；缺工具被过滤的结果也进入集合差异。禁止只 hash 名称或把所有未选 catalog 项误作本次选择。 |
| ProcessToolbox/toolset | 按实际合流顺序纳入 core、memory、configured web、固定 MCP list/describe/call、可选 deliver、Swarm handoff 与 native implicit 工具 descriptor；纳入 fetch_allow_private、search 是否启用及 provider 名、delivery client 可用性；namespace/run/lease 绑定不纳入 descriptor。MCP 只冻结静态声明及固定本地 wrapper，不冻结动态远端工具目录/凭据/授权。 |
| tool descriptor | name、description、输入 schema、return_direct/response_format、稳定实现来源与本地无 secret 行为选项；schema 按同编码规则，保留数组顺序。禁止 repr、callable 地址、闭包全量转储或含 secret 的工具 model_dump。 |
| WorkerDependencies/运行策略 | run_token_budget、Supervisor recursion_limit、ChatModelSettings.disable_streaming/openai_reasoning/litellm_enabled，以及已选 backend 的无 secret 执行策略标识纳入；这些值由 main 显式传递，不隐读环境。 |
| 请求/动态 owner | requested_model_label 与 selected_skill_source_refs 仍由 canonical dispatch 独立冻结；不hash整请求。System route revision/health、当前授权与凭据、IAM actor/assertion、service proof、signed URL、旧审批排除并重验；由路由选择产生的实际native prompt/tool override/exclusion/GP/middleware source明确纳入第二阶段，不因排除route而漏掉其有效结果。 |
| 资源/部署 | 所有 API key/secret、base URL、连接 URL、workspace 路径、容器/session ID、checkpointer/store/client 实例、进程调度并发/heartbeat/日志配置排除。sandbox 的部署凭据/地址/运行实例不作 recipe；backend 实现与策略变化须由稳定实现来源/无 secret 策略标识反映。 |

实现来源采用显式 `ToolImplementationSource` descriptor（内部 dataclass），不是网络发现或运行时 inspect 猜测。
P2已由 `execution/runtime_profile_sources.py` 持有版本化本地实现清单，复用P1 codec：tool factory模块限定名/符号及包内批准源码文件
字节 SHA256（含明确列出的本地依赖闭包）；native implicit 工具纳入固定 distribution 版本及相应实现文件摘要。
从安装包资源读取，缺源/未登记工具失败关闭；开发源码与 wheel 均验，禁止运行时访问 Git 或把 commit 当内容指纹。
`tools/toolbox.py` 在创建 web/memory 工具时同时产生 descriptor/无 secret 选项；`tools/toolset.py` 复用同一选择规划
提供实际工具序列，`agents/subagents.py` 复用同一纯选择规则，避免 profile 和 build 两套过滤逻辑。
`worker/main.py` 显式装配策略与源码 descriptor；sandbox 非默认定制实现也须登记稳定来源和无 secret policy 标识，
禁止偷偷 hash 任意 custom config。当前已有P1 codec与共享选择plan；生产manifest/worker静态装配已由P2提交7e902c0，
两阶段持久字段及有效native输出绑定属独立后继，均不因本轮文档修订视为落地。

### 后续精确写入集与验收矩阵

本门只有本文、API_CONTRACT/DATA_MODEL/CURRENT；下列均是 **拟派工范围，不是本轮授权**。
以 `/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro/apps/kokoro-agent/` 为绝对根：

- 机器：`contract/openapi/v1/openapi.json`、`contract/provenance.json`、`contract/README.md`；现生成器
  `scripts/generate_failure_models.py`、`src/kokoro_agent/chat_contract_check.py`、`contract_check.py` 与
  `protocol/run_failure_generated.py` 仅按 source digest 再生/校验，不手改 failure 值域；proof source 不变。
- 输入/执行：`src/kokoro_agent/protocol/control.py`、`interfaces/http/ingress.py`、`interfaces/http/server.py`、
  `domain/run/models.py`、`domain/run/repository.py`、`domain/run/repositories.py`、`domain/run/scope.py`、`agent_factory.py`、
  `features/catalog.py`、`execution/protocols.py`、`execution/run_agent.py`、`execution/events.py`、
  `execution/runtime_profile.py`（P1已存在）、`tools/{toolbox,toolset}.py`、`agents/subagents.py`、
  `worker/main.py`、`worker/dependencies.py`、`worker/supervisor_recovery.py`、
  `worker/supervisor_context.py`、`worker/supervisor_execution.py`、`worker/supervisor_control.py`。
- 数据：`database/schema.sql`；`src/kokoro_agent/infrastructure/{schema.py,checkpoints.py,run_checkpoints.py,
  postgres_run_admission.py,postgres_run_dispatch.py,postgres_run_leases.py,postgres_run_events.py,
  postgres_run_context.py,postgres_run_repository.py,postgres_run_effects.py,postgres_run_sandbox.py,postgres_chat_repository.py}`，其中 run_checkpoints.py 为拟新 native adapter；另一个拟新源码文件为上述 runtime_profile.py。
- 测试：现 `tests/unit/http/{test_ingress.py,test_server.py}`、`tests/unit/execution/{test_supervisor.py,
  test_control_commands.py}`、`tests/unit/agents/test_factory.py`、`tests/unit/infrastructure/test_postgres_run_context.py`、
  `tests/contract/{test_agent_runnable.py,test_chat_response_envelopes.py,test_canonical_database_schema.py,
  test_repository_schema.py,test_postgres_adapters.py}`、`tests/integration/database/test_schema_installation.py`、
  `tests/acceptance/test_http_ingress.py`、`tests/support/{chat.py,fakes.py,deepagents.py}`；拟新增
  `tests/unit/execution/test_retry_checkpoints.py` 与 `tests/integration/database/test_retry_checkpoints.py`，
  分别负责 installed native DAG 行为与真实 fenced persistence，不新建 tests 目录。
  实际 `rg -l 'RunRequest\(' tests --glob '*.py'` 另检出 8 个现构造文件，补入窄允许集：
  `tests/unit/tools/test_toolset.py`、`tests/unit/clients/test_storage_delivery.py`、
  `tests/unit/execution/{test_execution_proof_supplier.py,test_scope.py}`、
  `tests/contract/{test_execution_proof_artifact.py,test_platform_transport_http.py}`、
  `tests/integration/database/{test_run_outbox_filter.py,test_delivery_outbox.py}`；
  另有 decoded run.request fixture 的 `tests/contract/test_public_contract.py`，一并补 explicit null/缺失拒绝断言，
  不扩大为全部 tests 授权。

| RED/保留行为 | GREEN 必须证明 |
| --- | --- |
| 同 message_id 新 attempt 当前 identity conflict；同 session native Human 重复 | 原 user row/seq/time/content/run_id=origin 不变，assistant/system/tool 身份不放宽；一 logical Human 一次 |
| failure 后仍含 AI/tool/private/pending 后继；仅改 Human ID 不够 | 初始装配失败、post-start partial、tool 后失败、HITL 后恢复 build 失败，全都从同 pre-turn baseline；next/tasks/interrupt 空，完整 native fields/delta/subgraphs 不泄漏 failed tail |
| failure 后 normal “继续”、连续多次 failure | 顺序补齐尚未 committed 的 stable Human，仅一次；成功 retry 后下一 normal 接新成功 head，不重复旧 user、不从旧 attempt latest 续跑 |
| typed lineage 缺失/错形状/自引用/跨 scope/非 latest/非 retryable | strict machine/runtime/fence 一致；不同 key 竞争、normal-vs-retry、同 key ACK lost 都按同 scope receipt 结果；无第二 Run |
| capture/genesis commit 前后、native put/head CAS 前后、terminal staging 前后崩溃 | 真 PG rollback/恢复；baseline 不漂移，旧 lease/过期/取消的 native 与 event 写同时被 fence，子图及 pending writes 无漏口 |
| terminal CAS后未落outbox、usage异常、Redis阻塞/迟到 | typed outcome单事务零半终态；commit后发布、固定critical身份恢复，保留reserve→fenced Chat：terminal先赢拒append，live先commit则HTTP seq先live后terminal；Redis可迟到，无持锁网络，原generation终态重放seq不变 |
| terminal Chat插入后outbox/head失败、release-vs-normal barrier | 同连接Chat/outbox/usage/head/active全rollback；old terminal Chat先commit再允许新user admission，同event流old terminal.seq < new run.started.seq；message/event不同kind计数器不跨流比大小；Redis失败时HTTP已可见，重放seq不变 |
| 锁等待跨expiry、预读后generation变化 | 取得scope/Run锁后DB clock重验；旧Python now不授予写入，scope-first全入口无反序 |
| retry profile 原子继承 | NULL origin、catalog漂移、claim后回滚、同key replay 均不产生空/新选 digest；正常 Run 首次 freeze 独立覆盖 |
| actor/assertion 更新、撤权、MCP/profile 漂移、外部效果 ACK 未知 | 重验当前授权，旧批准失效；冻结业务选项不被当前 UI/catalog 改写；journal/artifact/usage 跨 attempt 独立且旧证据可读 |

拟命令：`uv run --offline pytest -q tests/unit/execution/test_retry_checkpoints.py tests/unit/execution/test_supervisor.py`
与既有 HTTP/contract 定点；Root 分配自有 PG schema/Redis logical DB 后运行
`uv run --offline pytest -q -o addopts='' tests/integration/database/test_retry_checkpoints.py tests/acceptance/test_http_ingress.py`；
空 owner schema 运行 `uv run --offline kokoro-agent-db-apply-schema`，再跑 schema drift。完整 lock/frozen sync、
Ruff/Pyright/generator/contract/default pytest/build 仍按本仓门；Root 最后执行真实 BFF 原 user→新 assistant/run→
Agent DAG→AG-UI/reload 的端到端，不拿纯 probe 或 22 项历史 acceptance 代替。

实施依赖：先完成上方 D0 三设计一致性；Root 可单独放行 P1 纯编码/共享选择，不等待生命周期产品决定。
正式执行主线仍为 scope admission/原 user/baseline 原子持久化与全部写入口锁序，再接完整 profile 装配/freeze、
native/start/resume/takeover，扩展现 finalize_terminal 的 head/active 原子性；引用感知 bounded Run purge、native reachability
及生命周期决定后的引用释放同属发布前必验阶段，不永久跳过。每个源码片仍须自己的文档子门、RED及Root派工。
各片 tests-only RED 后 GREEN，可审查提交不代表可激活服务；全 owner/schema/真实 PG Redis HTTP 门通过后才
发布 4.0 artifact，再按既定消费者顺序协调切换。未决产品生命周期不以降低 retryable 或 NULL profile fallback 绕过。

## AGENT-FAILURE3-GRANULARITY 实现候选（2026-09-30）

当前提交 `da056b0103cced10188cdc1f5baef841d8333889` 已由 Root 验收 Agent HTTP 3.0：
默认纯门 1518 passed / 6 skipped / 174 deselected，真实 PostgreSQL/Redis/HTTP acceptance
22 passed。当前受管 3310 仍运行旧 2.0，BFF/Web 尚未协调消费 3.0；单仓 owner 验收不等于
产品切换完成。新 Python 标准门发现 `execution/events.py` 806 行与
`execution_proof_contract.py` 804 行两个超过 800 行的职责粒度违例；当前候选已按下表完成职责拆分。

| 放置项 | 已裁决目标 |
| --- | --- |
| Owner | Agent execution 能力继续唯一拥有执行失败归码；Agent execution-proof checker 继续唯一验证本仓 proof artifact。没有新业务 owner、进程或传输边界。 |
| 当前事实 | `execution/events.py:658-703` 同时保存失败归码表、`failure_code`、`run_failed_payload` 与事件发射/映射；`execution_proof_contract.py:487-500` 同时保存具名 negative metadata 的 exact 校验与其余 schema/vector 编排。 |
| 目标职责 | 新 `execution/failures.py` 只拥有完整失败分类与 payload 构造；现 `execution_proof_negative_specs.py` 在 immutable named-negative policy 旁唯一拥有公开给 checker 的 `json_exact` 与 `validate_named_negative_metadata`。调用方行为与异常保持不变。 |
| 目录方案 | 失败归码采用 execution 包内同级模块，淘汰继续挤入 events.py 或下沉 System client：前者混合变化原因，后者会让外部 owner client 决定 Agent Run 语义。negative metadata 扩现 negative specs，淘汰新建通用 helper/第三模块，因为它只服务具名 spec policy。 |
| 粒度 | `failures.py` 有三个共同变化的对象且被初次执行、恢复、Chat/acceptance 测试复用，值得独立文件；negative metadata 只有一个紧邻 spec 的校验函数，默认扩现文件，不再建目录。 |
| 依赖 | `failures.py` 仅依赖已生成 `RunErrorCode`/`RunFailedPayload`、typed `ModelResolutionError`、预算/递归异常；`events.py` 不直接、相对、延迟或 `as` 别名导入/re-export failure owner。`run_agent.py`、`supervisor_execution.py` 与直接测试从新 owner 导入。proof contract 无别名直接导入并调用 `json_exact` 与 metadata validator；negative specs 不导入 contract，避免循环。 |
| 数据/API | OpenAPI 3.0、failure code/retryable 合法 tuple、proof schema/vector/metadata、SQL、Redis、outbox、Chat JSON、lease/CAS/单终态 fence 全部原字节/语义不变。当前 provenance owner inventory 不列两个被拆模块，因此不改摘要掩盖移动。 |
| 删除项 | 源码门必须从 events.py 删除完整失败归码块，从 proof contract 删除原 metadata helper 与 `_json_exact` 实现；不保留 import alias、fallback、双实现或重复 comparator，也不以压缩 tuple/空行凑低行数。 |
| 验证 | 本门先运行现行为 baseline，再用文件归属与 callable/exact-bool-int 断言产生非 collection RED。源码门后需相关行为、Ruff、Pyright、generator/checker、默认 pytest、build、Root 标准 139→137 且无新增项，以及 Root 自有完整 22 HTTP acceptance。 |

失败分类保留 typed owner error 优先：`ModelResolutionError` 的已验证 code/retryable 先于调用方普通
assembly default，未知 owner code 仍为 `internal_error/false`，非法 bool/tuple 仍为
`contract_incompatible/false`；TokenBudget、GraphRecursion、普通错误和显式 assembly 默认不变。
初次执行、恢复、取消竞争、lease、终态 CAS、safe sentinel 与唯一 publish 断言均不改。

唯一 `json_exact(actual, expected)` 随 negative metadata 移入现 policy 模块，并作为 checker 的
具名窄协作者公开；checker 直接导入调用，不复制 comparator。它必须使用 JSON **类型精确**比较，
不能用 Python 宽松相等令 `true == 1`。validator 仍只剥离当前 stage 对应的
`raw_json_base64url` 或 `encoded_segment`，
精确核对 name/stage/component/error_kind/optional metadata/difference 后返回 stage；未知名字、错误
payload key、额外/缺失字段和 bool/int 漂移继续失败关闭。当前 `events.py` 为 752 行、proof contract
为 769 行；这来自完整职责移动而非压缩。Root 标准与真实 HTTP 仍待主控独立复验。

## AGENT-RUN-EVIDENCE-INITIAL-CURSOR 实现候选（2026-09-30）

Run wire index 从 0 开始；原 ingress/server 默认 after_seq=0 且 exclusive 过滤，
导致没有 run.started 的首个 index=0 终态在初始 evidence 查询中消失。Root R2 真实两例
已通过 outbox/Chat/Redis 安全属性断言，但 HTTP terminal=False；这不是重复 terminal。

采用现 Run evidence 独立具名 EvidenceAfterSeq（integer/int64，minimum/default=-1，
maximum=9223372036854775807），
省略或 -1 表示尚未看见事件，后续仍使用最后已见 index 的 exclusive cursor。
空页 next_seq 原样回显输入；-1 空页之后用同 cursor 可读取迟到的 index=0。
淘汰修改共享 AfterSeq、重编号 Run index 或伪造 START；Chat seq 从 1、AfterSeq=0 不变。
实现只扩既有 interfaces/http/ingress.py、server.py 与 OpenAPI 参数/EvidencePage；不新建模块。
未发布 HTTP artifact 3.0.0 的机器源、provenance 与 failure generated source digest 已同步；
failure 模型内容、proof/SQL/lease/取消语义不变。既有定点 RED 已转为纯测试 GREEN；Root 自有
PG/Redis 两例 terminal=True 仍是验收门，消费者只在固定 owner commit 后 repin。


## AGENT-FAILURE-CONTRACT 实现候选（2026-09-30，待协调发布）

基线 `main 58b59cf7`；文档门和 RED 已经 Root 放行，工作树现实现 HTTP artifact `3.0.0`。
System client 验证 HTTP/code/retryable 而不作 status 掩码；现有初次/恢复入口共用 execution/failures.py
归码，typed 模型错误优先于普通装配默认。Run payload 仅 code/retryable，Chat 投影持久保存
status/code/retryable；没有 raw异常类名/原文。当前受管组未加载候选，BFF/Web 尚未切换。

| 放置项 | 已批准并实现的本仓边界 |
| --- | --- |
| Owner | Agent 唯一拥有执行失败事实；System 拥有模型路由/可用性；BFF 拥有 Product/AG-UI 安全投影，Web 固定消费 BFF。 |
| 唯一机器源 | 既有 `contract/openapi/v1/openapi.json` 手审 schema-first：基础 Failure 唯一定义 code、retryable 和合法 tuple；RunFailure 引用基础，ChatFailure 组合基础及 status=failed。不导出所有内部事件。 |
| 新文件/两案 | 新 `scripts/generate_failure_models.py` 为薄 CLI，委托既有 chat_contract_check.py 的唯一 profile 编译/生成校验；新 `src/kokoro_agent/protocol/run_failure_generated.py` 是只读纯 wire，带生成 header/source digest。采用既有 protocol 目录，淘汰 generated/agent_run_failure.py 所需向内依赖例外及单失败 sidecar 新目录。 |
| 依赖 | 生成物仅依赖 stdlib/Pydantic；protocol 零向内依赖门原样保留。events.py 使用生成模型，protocol/__init__.py 窄导出；其余事件仍由现 Pydantic 定义。业务归码在 execution/failures.py，不进入生成物。 |
| 调用 | clients/system.py 先严格验证 owner HTTP/code/retryable tuple，删除 status 掩码；初次 supervisor_execution 与恢复 supervisor_control 共用唯一 typed 归码。domain/chat/projection.py 持久序列化生成 ChatFailure，不丢 retryable。 |
| 删除 | 旧手写 failure enum/model、原异常类名/原文 wire、已知模型错误被 catch-all 覆盖、失败 _Terminal 的失效诊断字段；不删除 AG-UI 标准 RUN_ERROR.message。 |
| 状态/数据 | 单终态 CAS、lease generation、取消传播、outbox 顺序/幂等与清理不变；无 DDL、无新进程/目录/owner/依赖。旧 JSON 与切换边界见 DATA_MODEL。 |
| 验证 | schema/runtime 合法 tuple、生成完整 bytes/header/hash、初次/恢复/取消/lease、secret sentinel、真实自有 PG/Redis 持久重放；不以文案或纯 fixture 代替真实跨 owner 结果。 |

分类及严格 HTTP tuple 只按 [API_CONTRACT](API_CONTRACT.md) 本片表实现。模型 unknown 继续不准入，
不自动 retry、不切 fallback provider；retryable 仅是经验证的失败属性，不保证再次请求成功或免费。
RunFailure 只含 code/retryable；ChatFailure 只含 status/code/retryable。BFF 后继按固定 code 生成脱敏
AG-UI 标准 message，Web 按 code 本地化，不解析 message，不接收 Agent 私有异常诊断。

本片生产触点：clients/system.py、protocol/events.py、protocol/__init__.py、execution/failures.py、
domain/chat/projection.py（均在 src/kokoro_agent）。worker/supervisor_execution.py 与 supervisor_control.py
保持现调用、取消/lease/CAS；安全归码在共享 payload 构造中完成，无需复制两份分类。
机器/检查涉及 contract/openapi/v1/openapi.json、contract/provenance.json、contract/README.md、contract_check.py、
chat_contract_check.py 与 execution_proof_contract.py 的 owner inventory；proof schema/vector/direct digest 不变。
生成 --check 比较全部再生 bytes，checker 验证唯一 decoded 映射、strict models、generated direct digest 与 aggregate。

发布为 HTTP artifact `3.0.0`，URL 仍 `/v1`：Agent 固定机器/实现 commit → BFF 严格 repin 并发布
Product/AG-UI → Web 固定消费 → Root 自有 fixture fresh/组合验收。无兼容双读；当前本仓实现候选纯门已过，
固定提交/独立复验、真实持久门和跨 owner 消费者切换仍待 Root。

## W3 OAuth 成功响应扩展边界（2026-09-30）

既有 `clients/platform_tokens.py` 是 IAM OAuth consumer 的唯一解析/缓存边界；本片不新增模块或契约。
按 [RFC 6749 §5.1](https://www.rfc-editor.org/rfc/rfc6749#section-5.1) 忽略未知成功响应成员，
包括 IAM 返回的 `expires_at`；Pydantic 丢弃扩展，不保存、不加入 repr，也不用于授权或缓存有效期。
已知 `access_token`、`token_type`、`expires_in` 与可选 `scope` 仍严格校验；Bearer、token 语法、
整数 TTL >5、提供 scope 时精确相等不变。缓存只按 exchange 开始的 monotonic + expires_in 计算。
1 MiB 响应预算、deadline、拒重定向、secret-free 错误、单飞取消、credential 代际均保持。
此例外只属于 OAuth 成功响应，不放宽 credential 文件、Platform Proto、Skill/ZIP 或执行 proof。

## W3 Run-bound Skill metadata 生命周期返修（2026-09-30）

文档门基线 `534d3f80`：API_CONTRACT/DATA_MODEL 的 frozen Run refs、同 session checkpoint、SQL owner 不变。
SDK 原生 SkillsMiddleware 将 metadata 缓存在 session checkpoint；新 Run 必须重新加载当前选择，不能复用上一 Run 的能力。

| 放置项 | 决定 |
| --- | --- |
| Owner/文件 | Agent 唯一 writer；新增既有 `skills/middleware.py` 的 `RunSkillsMiddleware(SkillsMiddleware)`，factory 负责装配。 |
| 两案/粒度 | 采用 skills 目录承接 metadata 生命周期；淘汰 tools/middleware（工具授权职责）、backend 内解析或复制原生 loader。一个普通文件，不新增目录/owner。 |
| 公开接口 | 重写官方 `before_agent`/`abefore_agent`：复制输入 state，移除 skills_metadata/skills_load_errors，委托父类公开 hook；成功更新显式包含 load_errors（无错误时 []）。原生解析、prompt 与私有 state schema 全部继承。 |
| 生命周期/依赖 | 每次 graph entry 使用本 Run 的 backend 刷新，即使 refs 相同也重验；HITL Command resume 继续原 checkpoint 节点，不换 thread_id/checkpointer，不引入 metadata 第二来源或 Run 标记。失败/取消向上传播，不使用旧 metadata 调模型。 |
| 装配/删除 | factory 使用公开 middleware 参数装配唯一子类，skills=None 禁用重复默认实例；保留现有 guard 链。空 refs backend 返回空目录且不创建 Skill client。 |
| 验证 | 真实生产 Factory＋DeepAgents＋同 InMemorySaver：[]→A、A→B、A→[]、同 refs 新 Run、旧错误清理、HITL resume/guard、失败与取消；检查模型 prompt 和 private checkpoint state。无新 API/SQL/generated/依赖。 |

## W3 typed Skill reader 实施当前态（2026-09-29）

本片基于`dd34a48`，沿下文已批架构完成v4固定消费、run-bound typed source、signed GET与ZIP、只读backend，删除旧name双轨。
本节与新增文件放置补充覆盖下文历史实现描述；详见[当前事实/边界](CURRENT.md)与[实际验收](ACCEPTANCE.md)。
`clients/skills.py`不保留任何包bytes缓存，所有访问重新Approved/GET/ZIP。backend逐包glob/grep，不聚合所有展开包；
批download最多128路径/累计128MiB，grep最多1000条/1MiB输出，超限明确失败且不返回部分结果。
当前Storage v2 GET只允许空required_headers，HTTPX公开transport避免AsyncClient签名URL INFO日志/cookie jar；
显式connect/pool3s、idle/write10s、总30s且受签名expiry限制。Platform v4激活和安装产品链仍是独立owner门。

## 已批准设计：W3 typed Skill source 执行选择与包读取（历史起点与目标）

本节区分已落地的 launch 输入片与其余目标，不把下文 W1E 历史叙述当当前实现。设计起点 `7dfcfa936d0b51244683ffd66d16ea937fe510a6` 原无 Skill 选择；现在 Agent-owned HTTP OpenAPI `2.0.0` 和 `LaunchBody`/`RunRequest` 要求显式 `selected_skill_source_refs`，现有 dispatch/Run canonical JSON 与 run_id fence 承接顺序和空数组，错形状 400、漂移 409。真实 PostgreSQL/Redis roundtrip 尚待隔离验收。worker 已有按已认领 Run 固定 tenant/lease 的 IAM token、fresh execution proof 和六个具名 Platform Connect sender；但 `agents/definition.py` 的 `Agent.skills: tuple[str]`、`agents/music.py` 的 `"music"`、`clients/skills.py` 的 name/scope/hash 协议与 `skills/backend.py` 的按 name 缓存仍是旧 Capability 模型。标准产品路径未调用 Skill RPC、未读 Storage 包；新字段不等于 Skill 执行已闭环。无声明的基础 Chat 由显式空数组运行，不能将已声明 Skill 的空列表/404 当成功。BFF 普通 Chat durable outbox 与 Scheduler 发送方尚未提供必填字段，旧 payload 会在 Agent 入口 400，必须由 BFF 紧接消费修复。

| 放置项 | 决定与理由 |
| --- | --- |
| Owner | BFF 拥有用户对话中显式选择与当前 IAM session 准入；Agent 独占 Run 输入快照、claim/lease、只读 `/.skills/` backend 和执行；Platform 独占 exact-revision `SkillSourceRef`、安装、当前可见性/授权与包引用；Storage 独占 bytes/scan/GET 签发。 |
| 两案 | 采用 BFF 给 Agent *精确 typed ref*、Agent 在 admission 冻结选择并在每次读取重新向 Platform 验当前授权；淘汰 name/display/`DiscoverVisibleSkills.query` 推导、BFF/Web 直读 Storage、旧 Capability grant 或以一次 Resolve 缓存长期放行。 |
| 位置/粒度 | 复用 `protocol/control.py` 的 RunRequest、`interfaces/http/ingress.py` 的 LaunchBody、现有 dispatch/Run JSON、`agent_factory.py`、`clients/skills.py`、`skills/backend.py`、`worker/platform.py`；以后由单一 Agent writer 按输入 fence→owner client→ZIP/backend 的业务切片修改。拒绝新顶层 `platform/`/`ports/`、第二组 wire/生成物或把业务规则塞进 worker 入口。 |
| 依赖 | Agent 只用固定 Platform generated RPC 与由该 RPC 授权的 Storage-signed GET；BFF 的选择不能代替 Platform 当前决策，proof 不能代替 IAM/Storage 检查。禁止读取 Platform/Storage SQL、复制其 ORM/Skill owner 状态。 |
| 数据/API | 已用既有两张 Run 表的 `request_json` 机制与 Redis Run wire 承接必填字段，未新增 SQL 表/列、Redis key、签名/包体持久事实；HTTP 机器契约是 pre-launch breaking `2.0.0`，URL 仍 `/v1`。BFF 尚须固定消费；见另两份设计。 |
| 删除 | 一次 cutover 删除 name selector、scope/name/content_hash grant、按名称回空和隐藏失败、旧 `CapabilitySkillBackend` 的授权性缓存与旧注入/测试路径；`/.skills/` 只读路由可保留但从 typed source 与当前授权重建。MCP 属后续独立片，不借此片造假接线。 |

**唯一选择与持久 fence。** 第一版只接用户对该 Run 显式选择的 `source_ref`，不隐式注入 `Agent.skills` 静态 name；`music` 的旧 name 声明在 cutover 时删除，不从其字符串猜已安装 Skill。BFF public Chat 对话选择是唯一用户选择 owner；BFF 必须在当前 IAM session 中把 Platform 已发布的 exact ref 传到 Agent launch，不能让浏览器自报 tenant/actor/subject。Agent HTTP ingress 对 `selected_skill_source_refs` 做 strict typed 语法、数量/重复/顺序限制后，结合受信 `ExecutionIdentity` 写入同一 canonical `RunRequest`；对授权只做形状预检，真正当前授权在执行时由 Platform 裁决。空数组是明确“本 Run 无外部 Skill”；不能回退为 deployment/default Skill。Agent 静态 Feature/Agent 只决定是否支持这一执行面，不拥有用户所选资源身份；将来如需静态 exact ref，必须先设计由 Agent 在 **admission 前** 合成并冻结完整有效集合的独立版本化配置，不在 worker claim/resume 时按可变 Feature 配置补选。

同一 `RunRequest` 的 canonical JSON（含 refs）进入 dispatch intent/fence 与 claim 的 Run 行；同一 `run_id`、相同身份但 refs 漂移必须 409，旧 lease generation/consumer 不得用新选择重跑。Worker/recovery 从已持久请求读 refs，不能从当前 BFF 页面、Feature 目录、Redis 临时消息或安装列表重新选择。现有 PG `request_json TEXT` 足够容纳有界字段；实施前测试最大 JSON 行大小/原请求 digest 和两表同值，必要上限在 Agent-owned wire 明确，不另建并行表。当前基础旧 Run 不做 silent decode alias；clean-slate 代码片要同时更新 fixture 与发送方。

**读取状态机。** 对每个冻结 ref，先用已认领 Run `for_run(LeasedRun)` 的 sender 调 `SkillSourceService/ResolveVisibleSkill`；逐次发送前重新校验 DB-clock lease 与 fresh proof，核响应 exact `source_ref`、active exact revision、`content_digest`、`manifest_identity` 与包资产引用的存在/类型。`Resolve` 只是当时快照，不是长期 grant。首次实际读取及每次再次访问 `/.skills/` 均调 `GetApprovedSkillPackageReference`，并核其 ref 所属 asset/digest/manifest 与本 Run 已解析的同一身份；Platform 在该调用重验 IAM/安装/Skill 与 Storage 当次 CLEAN/对象健康。拒绝、停用、移除、跨 tenant、当前感染/未知、依赖失败时**不使用旧包缓存**。若同一 Run 内复用已校验 ZIP bytes，也必须先取得本次新的 Platform 当前授权且资产/hash/manifest 逐项不变；更换版本不自动跟随，必须由新 Run 选择新的 exact ref。

GET 只按 Platform `GetApprovedSkillPackageReferenceResponse.transfer_reference` 发起：method 必须 `GET`，expiry 为未来 UTC，URL/host 受 worker ObjectStore egress allowlist 约束，禁 userinfo/fragment、禁 redirect、禁复用 IAM/Platform bearer/cookie；只转发 owner 批准且校验过的 `required_headers`。Stream 有总 deadline、idle timeout、状态 200、identity content encoding、32 MiB 压缩字节硬上限，边读边 SHA-256；与 `content_digest` 精确一致后才解析。ZIP 按 Platform **当前 v4** `contract/execution-operations/v4/zip-profile-v1.json` 限制 128 entry、单 entry 16 MiB、总展开 128 MiB、manifest 16 KiB、路径/CRC/ZIP32/flag/重复/特殊文件规则；`manifest.json` 的 raw 解压 bytes 算 `zip-v1:sha256:<hex>`，并校验 `skill_id`/`revision` 与 Resolve 的 exact revision、`entry=SKILL.md`。未全部验证前不向 DeepAgents 暴露任何文件；合法相对路径保留原始 entry bytes 到只读 backend，`aread` 对非 UTF-8 文本显式报错、`adownload_files` 仍传原字节，不把二进制强制解码/重编码；不执行包内脚本、不写 host/sandbox。

**只读路径身份（唯一裁决）。** 从已严格解析的 `SkillSourceRef.value = "skill:" + SkillId.value` 取具体版本的 `SkillId.value` 原始 ASCII bytes，做**无填充 RFC 4648 base64url**，唯一根目录为 `/.skills/<encoded_skill_id>/`；不使用 display name、series ID、安装 ID、scope 或 revision 数值。SkillId 最大 191 bytes，编码后最长 255 个 `[A-Za-z0-9_-]` 字符；编码是可逆的一一映射，且不含 `/`、`.`、`..`、`\` 或 `%`，同一 exact revision 在重试/lease takeover 中路径恒定，不同 SkillId（包括同 display name 的不同版本）不得互相覆盖。backend 只接受与冻结 ref 表精确匹配的 canonical encoded segment，不做大小写折叠、URL 解码或按 name 的反向猜测。ZIP 内 entry 必须先经 v4 profile 验证为合法 UTF-8 NFC 相对 POSIX 路径，再按**已验证的路径段**附加到此根目录；绝不把未验证 entry、绝对路径或 `..` 传入通用 join/normalize，目录 entry 不映射为文件。该路由是内存中的只读虚拟路径，不写宿主文件系统，亦不因显示名碰撞增设 alias。

签名 URL/headers/proof/token/ZIP bytes 只在短期进程内存在，不写 Run/Chat/tool journal/日志或跨 Run 缓存。未知 Platform ACK/超时、GET 中断或签名过期：无副作用读取可保留同一 frozen ref 和 logical request_id 有界重试；每次新 Platform send 取新 token/lease/proof/JTI、GET 取新授权 reference，绝不重复用过期签名。失败在模型/工具继续执行前关闭该 Skill 能力，并由既有 Run 失败/恢复机制记录稳定错误；取消传播，不伪造成功/空包。外部签后 lease takeover 的短时在途窗口由 IAM/Platform 合同限制，不将进程内锁冒充跨 owner 原子撤销。

**未决 owner-first 门。** Platform `6a09913a96c686b316bfe707b823d039e625607a` 的 v4 machine artifact 为 `inactive/routable=false`，Agent 当前仅 pin v3 `5b6eb2c`，新 GET transfer/ZIP profile 尚未被 Agent 固定。Agent 已发布 launch 字段，BFF 当前 public Chat→Agent launch 尚无选择字段；先 Agent owner 契约，再 BFF consumer。Platform Source 目前要求 installed+enabled，BFF 的个人 Publish/个人列表不自动安装；必须由 Platform/BFF owner 发布真正安装/选择产品链，不把 ACTIVE 列表当可执行授权。Storage 包退役、Platform v4 激活及六 owner 真组合另有 Root 阶段门。以上任一缺口未过，不标 Skill runtime active。

### W3 reader 实施放置补充（2026-09-29）

新增 `clients/skill_package_transport.py` 仅负责 `get(PackageTransferReference, content_digest) -> bytes`：
独立无身份 HTTPX pool、固定 ObjectStore origin、GET/期限/header/大小/摘要校验。与现有 Artifact PUT 生命周期不同，
不复用 Storage service secret；`storage_object_origin` 可单独配置给读取，写入仍要求 Storage URL+secret+origin 齐全。
新增 `skills/package.py` 仅负责 `validate_package(bytes, skill_id, revision, manifest_identity) -> Mapping[str, bytes]`：
纯内存 ZIP32/profile/manifest 验证，无 socket/host 写入。继续采用现有 package，不新建目录；淘汰在 factory 内解析 ZIP
或把 GET 混入 Artifact PUT。`clients/skills.py` 负责 run-bound Resolve/Approved、响应身份核验和每次访问授权；
`skills/backend.py` 只负责 canonical 路径及 DeepAgents 只读接口。两新文件各有独立网络边界/纯解析变化原因。

文档核验基线 Agent `dd34a4800b4ce0cc61eb80dd715e528b9d4517da`。BFF `571b51de`、Web `1dc211bb`
已消费 frozen refs/[]，Root `772208ba` 已验证真实普通 Chat worker 与浏览器恢复（System/model fixture）；
上文“BFF 尚未提供字段/真实 Run roundtrip 未验”为旧阶段描述。当前本片开始时非空仍明确失败，
Platform v4 inactive/安装启用产品链与 Storage 退役仍独立 owner 门，不用本片代码门代替产品真组合。

## W3-AGENT-PLATFORM-V3-PIN：机器消费前置（2026-09-29）

Agent 当前候选只消费 Platform owner `5b6eb2c` 的 `kokoro.platform.v1` Proto 和完整
`contract/execution-operations/v3/`；owner 独占 RPC/operation/binding 事实，Agent 独占 Run、
lease、proof 签发与 Python consumer。旧 Agent v1 投影与 owner 唯一 v3 runtime 不同版，
故已在既有 `contract/platform/v1/` 替换只读 vendor，在既有 `generated/` 再生客户端与
24-request projector，并在既有 `execution/` 只保留一个 request-binding 算法。
与在 `clients/skills.py` 复制投影或建立第二 contract 根相比，此位置保持来源、生成物、调用计算三种变化原因分离。
删 Agent v1 vendor 和运行 alias；Platform owner 冻结的历史 v1/v2 不动。六个已批准出站 RPC 的
请求字段未因 Proto 的 Catalog Product context 增补而改变，但必须逐项用 v3 raw vectors 证明同值。
本片不接产品 adapter，不改 SQL/HTTP/Redis/RunRequest，不获取 bearer 或执行真实 owner RPC；
typed source/connector 选择、Storage 包体、MCP credential 与真实 IAM/Platform 组合随后独立闭环。
下文 W1E 的 v1 pin、worker 尚未装 signer、静默 fallback 等段落记录历史阶段，不覆盖本节当前态。


## W2-REAL-MODEL-AGENT-WRITE：General state 工作区写入（2026-09-29）

General Chat 的静态 `GENERAL_AGENT` 显式选择 `Permissions(filesystem="workspace_write")`，
承接已经声明的 `delivery=True`。此前它继承全局 `read_only`，原生 `write_file` 在创建作品前
即返回 permission denied；本片仅修正此静态能力声明，不改变 `Permissions` 全局默认。
其他 Agent（包括 Music）保持原默认只读；复用 General 的 Feature 沿用其明确声明。
Run wire 不接受 permissions/filesystem/agent/backend 参数，用户或模型不拥有放权入口。

仍使用原生 DeepAgents `state` backend，未切换宿主 shell，也未新建 workspace adapter。
`agent_factory._with_native_skills` 创建同一个 `CompositeBackend`：默认 StateBackend 承载
会话工作区，`/.skills/` 继续路由只读 `CapabilitySkillBackend`；同一实例交给原生文件工具和
正式 `deliver`。工作区写权限不授予 Skill package mutation、外部主机访问或 BFF 用户授权。

组件证据使用真实 AgentFactory/DeepAgents v3 loop、原生 write_file/read_file 和正式 deliver，
仅模型、System resolver、Storage facade、RunRepository/checkpoint 使用测试替身。它证明同一
state 文件视图、字节/hash 和可信 Run/lease/tool_call_id 传递，以及 Skill 写拒/默认只读/wire
不可放权；不等于真实 PG/Redis/System/provider/Storage/Product 浏览器集成已通过。
本片不改变 API_CONTRACT、DATA_MODEL、HTTP/Proto/SQL、生成物或依赖。

## W2-F2-S4 Agent→Storage 作品种类（2026-09-28；当前运行态与下一代码门）

**当前运行态。** Agent `96dafec038ab6a0397ce58bc638bb576c59f1328` 已固定 Storage
`d5cfc442c675e32363ae767f5ec662a9e0d9eaea` 的 v2 Proto/生成客户端；标准 worker 装配独立
Storage Connect 凭据和受控 ObjectStore origin，普通 Chat 可调用 `deliver`。已认领 Run 的可信
`tenant/subject/session_id` 与有效 lease 决定 conversation scope，工具使用七个批准的 Storage RPC，
按稳定 command 与冻结 journal 恢复；CLEAN 后 Final Artifact 才有成功回执。`delivery.created` 是
按 `(run_id, tool_call_id)` 稳定去重的 critical outbox 帧，投影到 Agent Chat，并在终态屏障前落账。
取消的 control receipt、终态帧、fence 和 cleanup intent 在 Agent 同库事务内提交。

Root W2-F2-S3 真纵切 `5d3b29c0f66aef366820f0eb` 已证明生产 Storage client + 已 claim Run +
真实 Connect/MinIO/ClamAV 的 FINAL、生产 `DeliverResult` journal、critical event/Chat/terminal 顺序、
重放不双发与本人签名 GET 原字节；还覆盖过期 lease、EICAR、跨 conversation/tenant 负例。
它没有运行完整 worker/模型生成，也没有证明 BFF 当前用户私有授权、Product Library 或 Web 展示。

**已确认缺口。** 当前 `StorageDeliveryClient` 用 MIME 选择发给 Storage 的 `CreateArtifactRequest.kind`，
并验证 `CreateArtifactResponse.kind` 与请求一致，却在 `DeliveryReceipt` 返回时丢失该权威值；
`DeliverResult`、`DeliveryCreatedPayload` 与 Agent Chat `delivery` 投影均无作品种类。BFF 不能从
MIME、路径或扩展名重新推断最终作品种类。下一代码门只沿既有 `clients/storage.py` /
`clients/storage_delivery.py` → `tools/deliver.py` → `protocol/events.py` /
`execution/events.py` → `domain/chat/projection.py` 链路增加**必填** `artifact_kind`；
不用新建通用 artifacts 包，不修改 Storage/IAM/BFF/Web owner 或 Agent SQL 表列。

| Storage `CreateArtifactResponse.kind` | Agent `artifact_kind` |
| ------------------------------------- | --------------------- |
| `DOCUMENT = 1` | `document` |
| `CODE = 2` | `code` |
| `IMAGE = 3` | `image` |
| `AUDIO = 4` | `audio` |
| `VIDEO = 5` | `video` |
| `DATA = 6` | `data` |
| `ARCHIVE = 7` | `archive` |
| `OTHER = 8` | `other` |

`OTHER` 仅对应 owner 明确返回的 8；`UNSPECIFIED = 0`、未识别数值、缺字段或与冻结
CreateArtifact 请求不一致一律失败关闭，不能降级为 `other` 或 `document`。MIME 仍仅用于选择
**出站请求**种类；通过 owner 校验的响应值才是交付回执及后续事件的来源。Finalize 响应本身不带 kind，
因此在同一次恢复/完成路径保留已验证的 CreateArtifact 回执种类，再与 Final Artifact 的 ID、digest、
CLEAN 状态一起构造 `DeliveryReceipt`。已 FINAL 但本地 workspace 消失时，仍以原冻结意图和稳定
owner command/receipt 恢复同一值；不得重读已变化的文件或发新命令制造第二件作品。

成功的 `DeliverResult` journal JSON、critical `delivery.created` payload、Chat 投影均须保存同一严格值；
重放用原 event ID/seq/index/timestamp，不把旧的缺 kind 成功结果补猜成有效事件。数据和事务边界见
[DATA_MODEL](DATA_MODEL.md)，字段/错误契约见 [API_CONTRACT](API_CONTRACT.md)。Agent 事件源变更时，
必须保持 `contract/provenance.json` 的 `source_files` 包含
`src/kokoro_agent/protocol/events.py`，重算 `combined_sha256`，并让现有
`kokoro-agent-contract-check` 通过；本**文档门**不改源码、机器契约或摘要。
下一代码门需 RED→GREEN 覆盖八值、未知/0/缺失、owner kind 不匹配、FINAL→journal 崩溃恢复、
重放不双发及真 Storage 纵切的 receipt→journal→outbox→Chat 同值断言。

## W1E authenticated transport 当前切片（2026-09-28）

本节覆盖下文历史阶段的“worker 尚未装配/静默 fallback”描述。保留 owner、六个首批 RPC、typed selection
与 Storage/MCP 凭据缺口决定；不扩展产品面或 owner schema。

| 放置项     | 当前决定                                                                                                                                                                       |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Owner/基线 | Agent worker credential/proof/client owner；`cf3d9ef` clean 起点，Root 独占 index/commit。                                                                                     |
| 目录       | 复用 `clients/` 放独立 credential-file、OAuth token、generated Connect I/O；`worker/platform.py` 是 worker-only resource composition，淘汰新协议层/全局 token 单例。           |
| 资源       | main context 创建 signer、direct bounded PG lease reader、HTTPX OAuth pool、pyqwest Connect pool；关闭先收割 token tasks，再关闭自有网络池。                                   |
| Run 边界   | `for_run(LeasedRun)` 原子固定受信 tenant 与 fence；`RunPlatformClient.send` 复制 Proto，固定 logical request_id，逐次取 token/project binding/读 lease/sign/fill tag100/send。 |
| API/SQL    | 只消费六个既定 owner generated RPC；Agent HTTP/Redis/canonical SQL 无变更，不保存 bearer/proof/credential。                                                                    |
| 删除       | 删除 declared Skill 静默空能力、MCP outage→YAML fallback；保留旧 name Protocol 仅作为未迁移的显式注入边界，不假称 typed consumer。                                             |
| 失败       | credentials/token/lease/projection/Connect 各自稳定错误；取消传播；无自动重试、无 stale token；Feature 全 peer 预检在所有 sandbox 前。                                         |
| 验证       | unit/architecture/contract/full pytest，owned OAuth/Connect HTTP + 真 Ed25519 + 独立 PG lease；真实 IAM/Platform production 组合仍未验。                                       |

配置七项 all-or-none：`KOKORO_AGENT_IAM_BASE_URL`、`KOKORO_PLATFORM_BASE_URL`、
`KOKORO_AGENT_PLATFORM_CLIENT_CREDENTIALS_FILE` 和四项 worker signer descriptor。无任何配置时保持基础 chat；
只配置部分项启动失败。凭据 JSON 数组逐项 exact 字段 `tenant_id,generation,credential_ref_version,client_id,client_secret,resource,scope`。
`generation` 是正 safe integer，version 非空字符串，resource/scope 固定；拒绝重复 member、重复 tenant/client、符号链接/FIFO、
非 owner/0400/0600、超限和读中修改。父目录仍是部署受信 secret-mount 边界。

当前尚无 source_ref/connector/connection 的产品选择到 Run 持久快照；因此标准 CLI 创建 factory，但 name-only 产品路径
不会猜 ID 发 RPC。六 RPC sender 在 worker composition 的网络/PG 测试中实发；与真实 owner 授权激活是两个不同门。

## W1E Agent→Platform consumer（2026-09-27；生成与离线 projector 已落地，runtime 尚未接线）

| 放置项     | 当前事实与裁决                                                                                                                                                                                                                                                                                                                                                  |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Owner      | Agent 唯一写 Run/lease、worker proof 签发与本仓 consumer；Platform 唯一写 Skills/MCP Proto、typed ID、operation/binding/receipt；IAM 唯一写 token/current authorization；Storage 唯一写包体/Artifact。                                                                                                                                                          |
| 当前事实   | 本仓已固定 Platform Proto/operation artifact，并生成 Connect client 与 24-request offline projector；`clients/{skills,mcp}.py` 仍仅是 Protocol，`WorkerClients` 默认 `None`；`Agent.skills/mcp` 为名称 tuple；A2c supplier 独立存在，worker 不装 private signer。`agent_factory.resolve_declared_skills` 和 `tools/toolset.py` 仍有静默降级/部署定义 fallback。 |
| 目标职责   | worker 用受信 canonical Run+lease 与 typed 声明调用固定 Platform RPC；每次真实 send 先计算该请求 exact binding，再让 run-scoped supplier 以数据库时钟即时签 proof。Skill package 仍只读渐进获取，MCP 每次真实工具执行前重新授权。                                                                                                                               |
| 目录方案   | 采用既有 `generated/` 保存可再生 Proto/Connect/projector，`execution/platform_request_binding*.py` 终止严格值域与复用唯一 JCS；未来 `clients/` 终止 transport。拒绝新增空 `platform/ports` 层、第二 runtime 或跨 owner DTO/SQL 副本。                                                                                                                           |
| 粒度与依赖 | 当前切片只做 owner artifact pin、离线 typed projector 与 contract gate；后续代码片再做 worker token+signer 生命周期、Skill/MCP adapter 与真实边界测试。业务层只见窄本仓对象，不导入 generated Proto、Connect response 或 Platform 数据模型。                                                                                                                    |
| 数据/API   | Agent `database/schema.sql`、本仓 HTTP/Redis contract 不变；proof/nonce/Platform receipt 不落 Agent 库。Platform `kokoro.platform.v1` 是唯一 RPC 事实；IAM/Platform 各自保持 owner schema 与事务。                                                                                                                                                              |
| 删除项     | 名称即 Skill/MCP 身份、已声明 Skill 空列表继续执行、MCP 部署 YAML 作 Platform 授权 fallback、旧 Capability wire/shared-token/active env 引用；不删除 Agent HTTP ingress 仍使用的 `KOKORO_INTERNAL_SECRET_AGENT`。                                                                                                                                               |
| 验证       | 本片三设计一致、Markdown 与 `git diff --check`；实现片须 owner artifact drift、Ruff/Pyright/pytest/build、真 PostgreSQL/Redis+IAM/Platform/Storage HTTP、取消/超时/重放/撤权及拒绝时零下游副作用。                                                                                                                                                              |

**源与 shape 前置。** Platform ADR-002 §3.3/§13 已裁决 Agent 的 Skill 声明保存 typed
`SkillSourceRef`，而不是由 Platform 新增 name→ref RPC。当前 `Agent.skills` 和
`SkillClient.resolve(selectors: Sequence[str])` 只有 name，无法组成
`ResolveVisibleSkillRequest.source_ref` 或 `GetApprovedSkillPackageReferenceRequest.source_ref`。
声明来源（含 BFF→Agent launch 上游）须先供应经 owner 列表/安装选择的 typed source ref，并在本仓
静态 Agent/Feature 声明与 Run admission 边界保持类型；不得把 `DiscoverVisibleSkills.query` 的模糊结果
当作精确解析，也不把 display name、series/skill/installation ID 当作 source ref。此上游形状未闭环前，
已声明 Skill 的 Platform consumer 不放行。

MCP 同样需 owner 可见的 active `McpConnectorId`，并在适用时明确 `McpConnectionId`/
`McpServerId` 与 tool selector 的不同身份；当前 `Agent.mcp`、`McpClient.resolve(selectors,
identity, namespace, deployment)` 只有名称/部署配置，无法组成精确 `GetMcpConnector`、
`GetMcpConnection`、`ListMcpConnectorCapabilities` 或 `AuthorizeMcpTool` 请求。
声明来源及受信执行策略须先固定 typed connector/connection 选择与每次工具调用参数映射；
不从 URL/provider/display name 猜 ID，不把本地 YAML 或既有 MCP egress allowlist 当作 Platform 授权。

**首批出站面。** Skill 装配先按 typed ref 调 `SkillSourceService/ResolveVisibleSkill`，懒读包时
调 `SkillSourceService/GetApprovedSkillPackageReference`，再按 owner 授权 read reference 由 Storage
读取内容并核 digest；本仓不负责安装/发布 mutation。MCP 在 typed connector 已可用后，装配阶段
按需读取 `McpConnectorService/GetMcpConnector`、`McpConnectionService/GetMcpConnection`、
`McpAuthorizationService/ListMcpConnectorCapabilities`；`mcp_call` 每次实际远端调用前必须
`McpAuthorizationService/AuthorizeMcpTool`，绑定 exact connector/tool selector、原始 typed arguments
bytes 的 SHA-256、approval ref presence 与 idempotency key，取得短期 `McpInvocationGrant` 后才连接/执行。
具体 MCP server/connection/credential 映射仍需在当前声明 shape 与 Platform response 中逐字段证明；
若当前 RPC 无足够受信数据，向 Platform owner 提交具名 gap 并在此边界 fail closed，不发明临时 wire。
`DiscoverVisibleSkills`/列表类 RPC 属浏览/选择面，不是已声明能力的精确解析；Agent 不调用
Skill catalog workload 六操作或 global `RegisterMcpServer`。

**固定 artifact 与传输。** 当前 consumer 从 Platform commit
`ee25c1f4d6df08be183ca10f7f5e852e0b21f641` pin `contract/proto/kokoro/platform/v1/platform_runtime.proto`
及 `contract/execution-operations/v1/` manifest/provenance/vector 原始字节，记录 repo、commit、path、
direct SHA、版本。build-time checker 不从可编辑 pin 自证：代码内固定 owner
repository/commit、exact 14 条 consumer path/owner path/SHA 及 provenance raw SHA，先验原始
provenance 再用其 13 条记录验 payload/aggregate。已用固定 Buf/Protobuf 生成 Python
message/Connect stub 与 typed binding projector，
运行时 Connect over HTTP（固定 `KOKORO_PLATFORM_BASE_URL`），不手写 JSON/gRPC wire 或复制可编辑 Proto。
Python 候选先核验官方 `connectrpc==0.12.1`（Beta）与 `protoc-gen-connectrpc==0.11.1`
生成的 async client：固定 Proto SHA、生成 wheel、对当前 Platform Express 的 Connect/gRPC-Web
真实互操作、per-call header/deadline/error/cancellation 行为；当前 server 不支持原生 grpcio。
官方 `ConnectError(CANCELED)` 包装后须在 Agent client boundary 恢复 Python 取消语义。
隔离生成与 Express loopback spike 已通过，但正式 adapter 的取消恢复仍未实现；若后续正式互操作不通过，另以 ADR 比较限定 unary
Protobuf+HTTPX adapter 的成本/故障语义，不直接手写未验证的 framing。一个经完整 Platform vectors 验证的 Python
RFC8785 JCS encoder 服务 generated projector；现有 proof JCS 仅复用候选，不另建第二 canonicalizer。

每次 send：先验证 typed request/operation，固定本次 logical `request_id` 与 IAM 已验证的 tenant token
选择，按 owner `request-bindings.json` 投影 `{binding_version,fq_method,tenant_ref,request_id,request}`；
排除 proof 与 request 内重复 request_id，保留 Proto optional/message presence、数组顺序和 set UTF-8 排序，
拒绝非 safe integer/重复/空 set 值。lowercase SHA-256 交给现有 run-scoped supplier；supplier 每次
重新读 PostgreSQL clock/lease、生成新 JTI/sign，填 request `execution_proof=100` 后立即发送。相同
logical retry 保留 request_id 但重验 lease 并重新签 proof；改 request_id 必重算 binding。所有 24 个
tenant-execution RPC 都需 proof，但 Agent 只消费本段所列 subset；proof 不代替 transport bearer。

worker 专用 tenant-indexed `KOKORO_AGENT_PLATFORM_CLIENT_CREDENTIALS_FILE` 供 IAM
`tenant_machine` client-credentials token provider 使用，audience 精确
`https://kokoro.dev/resources/platform-internal`、scope `platform:execution.invoke`、caller
`kokoro-agent`；tenant 只来自持久 Run 的受信 ExecutionIdentity，绝不由 body/selector/env 默认租户挑选。
token cache key 包含 tenant、不可复用 credential generation/client/resource/scope；仅余期 >5 秒复用，
同 key single-flight，轮换即 evict，exchange 完成前复核 generation，错误不 stale-on-error。
worker 加载/核验 private key 后才消费；HTTP 进程只持 public ring。Transport 认证失败、权限拒绝、
IAM/Platform 503/限流/网络超时分别归类，不将依赖故障降格为“没有能力”；取消直接传播，设置
connect/read/overall deadline、1 MiB 响应边界、不跟随重定向，只对 owner 明确安全的请求有限重试。
拒绝/过期/撤权/当前 source 或 connector 禁用、proof/binding 漂移时，在 Storage/MCP/provider
副作用前终止；completed receipt replay 由 Platform 仍重新执行 IAM/current-state gate。

## 1. Owner 与依赖

```text
BFF / trusted service
        |
        v
interfaces/http -> application use case -> domain rules
        |                  |
        +-------------> infrastructure ports/adapters
worker bootstrap ---------+
```

`protocol` 是跨进程 wire 模型，不依赖数据库和传输实现。`domain` 不依赖 HTTP、Redis、PostgreSQL、
DeepAgents 或 provider SDK。`application` 决定用例、授权入口、事务和幂等；`infrastructure` 实现 SQL、
锁、外部 client、checkpoint 和 stream；`interfaces` 只做解析、映射和错误转换。

worker 长驻调度由 `supervisor.py` façade、`supervisor_control.py`、`supervisor_execution.py`、
`supervisor_recovery.py` 和显式 `supervisor_context.py` 协作契约组成；PostgreSQL RunRepository 也按
admission、dispatch、events、leases、effects、sandbox capability 拆分。旧入口不保留同义 alias。

## 2. Run 生命周期

```text
HTTP/Redis request
  -> validate trusted identity and feature key
  -> persist dispatch intent + immutable request fence
  -> publish Redis notification
  -> worker reads canonical request from PostgreSQL
  -> atomic claim creates lease generation
  -> build Feature/Agent and invoke native DeepAgents
  -> persist fenced evidence/chat/outbox
  -> claim one terminal transition
  -> publish/replay durable event and cleanup sandbox
```

同一 `run_id` 的请求 body 变化返回冲突；旧 worker 的 generation 不得写入新 owner 的 run。Run ingress scoped
读取同时校验 trusted tenant 与派生 namespace，SQL JOIN 也按 tenant 连接，避免只依赖
hash namespace；chat scope 的 namespace 仍只能由同一 trusted identity 派生。Redis 丢帧时由 PostgreSQL
pending intent/outbox 扫描恢复。控制命令使用 durable command ledger，重复
identity 重放已有 receipt，digest 不同则拒绝。

`POST /v1/runs` 的 202 和 `GET /v1/sessions/{session_id}/events` 的 200 分别投影现有
`LaunchReceipt`、`ReplayPage`。HTTP envelope 仅封装对应强类型 `data` 与 request-id `meta`；
机器 OpenAPI 使用 `LaunchReceiptEnvelope`、`ReplayPageEnvelope` 固定这两条响应，不能退回
`data: {}`。Run admission/Chat replay 的 owner、事务、幂等和数据写路径均不因此变化。

执行事件中 `message.completed` 是单个模型 segment 的权威全文快照，不是“存在非空文本”信号。
原生 `output_message` 存在且 `text=""` 时仍发一次空完成帧；`message.delta` 不发送空片段，
而没有 `output_message` 且没有 text delta 的投影不虚构完成帧。空完成沿现有 RunEmitter
index、Chat projection、SQL 事务与 replay 序列持久化，必须早于同 run 的 `run.completed`；
这样工具之后的空最终段不会让先前非空草稿冒充最终回复。

## 3. Agent 装配

`Agent` 是静态能力声明，`Feature` 是产品入口和 peer handoff 声明，`AgentFactory` 直接调用
`deepagents.create_deep_agent`；多个 peer 只使用官方 `langgraph-swarm`。请求不携带 graph、tool、Skill、
MCP 或 namespace 配方。native state、checkpoint 和 loop 归上游框架所有。

## 4. 事务与一致性

- PostgreSQL 是 Agent durable facts 的 owner；Redis 只作通知和短期传输。
- 关系写入先校验 trusted namespace/状态，再以固定顺序加锁，在一个事务内写事实、receipt/outbox 和
  sequence。
- 没有数据库外键；跨 owner 关系由应用校验、事务、锁、状态检查和 reconciliation 维护。
- 事件 projection 使用 `(run_id, durable_seq)`/业务 idempotent id，重复投递不产生重复事实。

## 5. 故障恢复

worker 启动依次 republish pending dispatch、outbox、未应用 control 和 cleanup intent；心跳续租失败时旧
任务停止副作用。SIGTERM 停止新消费，drain 超时后交给 lease TTL 恢复。异常单 run 收口为 `run.failed`，
不杀死长驻调度循环。候选按本文 AGENT-FAILURE-CONTRACT 统一 typed 归码并保留安全 retryable；
普通装配异常仍为 assembly_failed，取消/lease/CAS 不改。

## 6. System 模型路由接线（2026-09-08，已接线、live smoke待验）

本节记录 ADR-031 已落地的窄消费者边界；原实施基线为 `70a38138f42f29e8a482fde7890fe0e2d0c27e34`。
当前已有 System client 与 Factory 接线；新增失败语义按本文顶部设计，不沿用旧阶段的 Run wire 不变假设。

| 放置项   | 决定                                                                                                                   |
| -------- | ---------------------------------------------------------------------------------------------------------------------- |
| Owner    | System 拥有 label/policy/availability→route；Agent 拥有执行、模型实例及进程凭据                                        |
| 当前事实 | `agent_factory.py` 已经经 ModelResolver 调用 System；候选精确失败语义已贯穿本仓 Run/Chat，跨 owner 消费待验        |
| 目标职责 | 创建实际模型前以 trusted tenant、feature、可选 label 调用 System；失败关闭，不绕过回本地名称                           |
| 目录比较 | 采用已有 `clients/system.py`，与现有 owner clients 邻接；拒绝新 `infrastructure/system` 或第二 runtime 层              |
| 粒度     | 一个单一 HTTP 边界模块含窄 Protocol/内部路由结果；`model/factory.py` 负责路由到模型实例映射                            |
| 依赖     | worker 入口创建进程级 HTTPX client 并关闭，Factory 依赖窄 ModelResolver；不跨仓 import/SQL，不导出 HTTPX Response      |
| 数据/API | System 路由接线不改 schema；新失败 Run wire 目标见本文顶部，System pin 见 API_CONTRACT；解析不处于事务内                                     |
| 删除     | 删除 `select_model_label` 与硬编码模型 fallback；显式 Agent model 若与路由冲突则拒绝，不静默覆盖                       |
| 验证     | HTTP client、真实Factory调用、缺配置/404/403/503/坏响应/超时/取消/限额；Ruff/Pyright/pytest/contract/build和live smoke |

System 当前只返回 `litellm` 路由，`gateway_model_name` 映射为 Agent `ModelConfig.name`；
provider endpoint/secret 永不来自解析响应。Agent 声明的 effort/thinking 仍属于执行设置。
CLI 启动必须配置 System URL/服务凭据及 LiteLLM 网关；嵌入部署可显式注入同一窄 resolver，fake 只在 tests。
HTTP 客户端设置总体 deadline、各阶段 timeout、响应上限、不跟随重定向、不自动重试；取消直接传播。
模型解析在创建 sandbox/附属能力前执行，失败不先制造外部资源。每次 build（含恢复构造）重新受信解析并记录
revision/digest/generation 的结构化日志；本切片不承诺持久化 run-level 模型快照，也不复制模型表。

## 7. Execution proof 与 JWKS（2026-09-12，A1 machine contract 已实现）

本节承接 IAM `bf160be173ef473bebe8e4a93b74ec52c230f180` 的 ADR-005，但不复制 IAM 的 operation/permission
catalog。Agent 是 execution proof schema、canonical claims、Ed25519 signer、active signing key 和 public JWKS 的唯一
owner；IAM 只验证 proof 并重验当前 IAM 事实，Platform 只拥有 Skills/MCP operation、request binding 与业务 receipt。
固定交付顺序见 §7.5；任何阶段都不创建临时 wire、手写兼容 DTO、fallback 或双读。

A1 当前已发布 strict decoded-profile schema、canonical/negative/one-bit-tampered vectors、provenance pins 和薄 OpenAPI+
proof-checker 编排；proof 专项校验集中在 `src/kokoro_agent/execution_proof_contract.py`。A2a 已实现 pure runtime profile/signer；A2b 已实现 private loader、anchored public-ring snapshot、JWKS HTTP projection 与独立 HTTP root。A2c 已实现 owner-internal database-clock lease reader 与 run-scoped supplier。IAM verifier 与 inactive Platform owner contract 已另仓发布；本仓 worker 尚未装配 private loader 或 Platform adapter，不能作为端到端授权能力。

该 IAM 固定提交目前只把 `lease_generation` 描述为正整数，尚未固定本节的 JSON safe-integer 上界、原始 integer-token/
`1.0` 拒绝规则；其 ADR-005 的交付段也尚未拆出 Agent 真实 Platform client 接线与 Platform fresh/completed-replay receipt 两个门。
因此它是 owner/方向基线，不是已经与本 R2 候选逐字一致的机器契约。§7.5 第 2 步必须让 IAM ADR、verifier、OpenAPI/SDK 和测试按
Agent 已固定 artifact 消费同一数字矩阵与六段依赖，再称跨仓对齐；IAM 不复制或另行发明 proof schema。

### 7.1 放置与依赖

| 项       | 决定                                                                                                                                                                                                                                                                                   |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Owner    | `kokoro-agent/execution` 唯一写 proof profile 与签发语义；`interfaces/http` 只投影 public JWKS                                                                                                                                                                                         |
| 当前事实 | A1 schema/vectors/checker/provenance 已发布；`protocol/control.py` 已有 typed `ExecutionIdentity`；`RunRequest.session_id` 是目标 claim 的 `execution_session_id`；`LeaseFence` 已有 owner/generation，但现有 `is_lease_current` 使用连接前取得的应用时钟，不能作为签发 freshness 证据 |
| 目标职责 | 每次真实 Platform 出站调用前，使用 canonical Run、当前 execution identity、同一 run/owner/generation 的未过期 lease、Platform 边界已校验的 operation/binding 即时签发；proof 不缓存                                                                                                    |
| 采用位置 | `execution/` 保存纯 profile/canonicalization、signer、worker-only private-key provider 与 run-scoped proof supplier；现有 Platform client adapter 接收 supplier；`interfaces/http/` 保存 HTTP-only public-ring reader/JWKS handler                                                     |
| 淘汰位置 | 不新建顶层 `auth`/`attestation`/`ports`，不把签名塞进 `protocol/`，不在 supervisor 或 `build_deep_agent` 中拼 compact JWT，不让 IAM 反向读取 Run，也不让 Platform 自签或验签                                                                                                           |
| 进程隔离 | A2b 提供 worker-only signer private-key settings/provider，但尚不接入 worker；HTTP 已只装配 public-ring settings/handler。两者使用不同配置类型、启动检查、readiness 状态和脱敏日志，HTTP 对象图不得包含 private path/bytes/provider                                                    |
| 数据     | 复用 canonical Run/lease；本设计不加表、列、Redis key、nonce receipt 或 key store                                                                                                                                                                                                      |

实现时按真实变化原因拆文件，不把 schema model、key I/O、签发编排和 HTTP handler 混成单文件。Signer 依赖注入 UTC clock、
CSPRNG nonce source、private-key provider，以及由 signer 消费方定义的最小 current-lease reader；不依赖 HTTP、Redis、IAM SDK、
Platform policy catalog 或 supervisor。Platform generated client/type 在 Agent client adapter 终止，不穿透 execution model。

### 7.2 每次调用的签发路径

```text
canonical LeasedRun(request + LeaseFence)
  -> run-scoped proof supplier
  -> Platform adapter validates typed operation and computes canonical request binding
  -> supplier queries current Run lease with PostgreSQL clock
  -> compare signing clock with returned database instant and lease expiry
  -> build exact v1 header/claims and sign once
  -> immediately attach opaque compact proof to this one Platform request
```

当前 `PostgresRunLeases.is_lease_current` 与 `PostgresRunRepositoryContext.is_lease_current` 都在连接前读取应用时钟；连接池或
查询排队跨过 `lease_expires_at` 时仍可能返回 true，因此 A2c signer 不复用该语义。已落地 reader 在获取连接后以一个 SQL statement
使用同一个 PostgreSQL `clock_timestamp()` instant，验证 `run_id + owner + 1..9007199254740991 generation + lease_expires_at > db_now +
terminal=false`，并返回该 `db_now` 与 `lease_expires_at`。签发前再次读取注入 clock；与 `db_now` 相差超过 5 秒、lease 已过期、
paused、terminal、owner/generation 不同或读取失败均 fail closed。真实 PostgreSQL RED 必须让连接/查询排队跨过 expiry，证明旧
应用时钟实现会误放行而目标 query 拒绝。

`build_deep_agent` 当前虽然接收 `RunRequest + LeaseFence`，但 `resolve_declared_skills`、`SkillReader.load_package`、
`build_toolset` 与 `McpClient.resolve` 没有 fence 参数。目标实现给这些真实 Platform client call boundary 传递同一个 run-scoped
proof supplier；每次出站前重新验 lease、生成新 `jti` 并签发，不在 build、Skill package cache 或 MCP snapshot 中预签/缓存 proof。
本地缓存的 Skill package 内容不伪装成新 Platform authorization；发生新的 owner API 调用时仍须重新取 proof。

数据库判定与签名不是同一原子操作：generation 可能在 query 返回后、签名期间或请求在途时失效。`exp` 不晚于
`min(iat + 60s, floor(lease_expires_at))`；没有至少 1 秒正有效期时不签发。这缩短已知临近 expiry 的窗口，但 generation 被提前
接管时旧 proof 仍可能在其剩余 claim TTL 内到达 IAM。IAM 的 5 秒 skew 只用于验证端时钟容差：要求
`iat <= verifier_now + 5s`，并仅在 `verifier_now < exp + 5s` 时接受，因此 `exp-iat` 始终不超过 60 秒，但最坏接受时间可到
`iat+65s`。V1 明确不声称实时撤销；更强保证需要新的 Agent assertion/introspection owner contract。

### 7.3 Profile、机器事实与依赖选择

当前机器事实为 `contract/execution-proof/v1/schema.json`，version=`1.0.0`。它定义 decoded protected header 与 claims 的
strict JSON Schema，`additionalProperties=false`，并以扩展元数据固定 compact JWS、RFC 8785/JCS、UTF-8 和 unpadded base64url。
A1 已把该文件和 vectors 加入 `contract/provenance.json.source_files` 与 `kokoro-agent-contract-check`，并提供固定 header、payload、
signing-input、signature/JWK、one-bit tampered signature 与语义负向 vectors；A2b OpenAPI `1.1.0` 已新增 JWKS route，并只引用
`ExecutionProofJwkSet` response component，不复制 claims schema。

protected header 只有 `typ=kokoro-agent-execution+jwt`、`alg=EdDSA`、非空 `kid`。claims 只有
`contract_version="1.0.0"`、`iss`、固定 `aud=https://kokoro.dev/resources/iam-execution-authorization`、`tenant_ref`、typed
`actor`/`subject`、`run_id`、`execution_session_id`、JSON safe integer `lease_generation`、exact `operation`、lowercase 64-hex
`request_binding_sha256`、integer NumericDate `iat`/`exp` 与唯一 `jti`。`jti` 是每次签发新生成的 128-bit CSPRNG value，以
匹配 `^[A-Za-z0-9_-]{21}[AQgw]$` 的 canonical 22-character unpadded base64url 表达，解码恰好 16 bytes并要求重新编码相等。
`kid`、`iss`、`tenant_ref`、actor/subject `opaque_ref`、`run_id` 与 `execution_session_id` 只要求非空字符串，不附加 pattern/maxLength。
V1 不携带 `identity_assertion_ref`、`nbf`、Skill/MCP ID、
owner scope、permission 或 provider secret。解析端拒绝重复 JSON member、额外 header/claim、URL-based key header 和非 canonical bytes。

`lease_generation` 精确范围为 `1..9007199254740991`；`iat`/`exp` 精确范围为 `0..9007199254740991`，并继续满足
`exp > iat`、TTL、lease expiry 与 clock policy。三者必须以 JSON integer token 进入 strict parser；Python `bool`、任何 float（包括
`1.0`）、负数、越界数都在签名/验签前拒绝，禁止截断、round、stringify 或先 coercion 再验证。JSON Schema 负责
integer/minimum/maximum，strict runtime type guard 负责拒绝 host-language bool/float，canonical-byte equality 另拒绝把 `1.0` 编码成
数字的非 canonical payload。当前 schema 对三字段同时标记 `x-kokoro-require-integer-token=true`，contract-check 从原始 JSON token
验证该扩展，不能只依赖 JSON Schema 的数学 integer 判定。

实现前重新核验精确依赖版本、维护状态、许可证与 Python 3.11 支持。ADR-004 选择直接声明成熟 PyJWT 与其实际使用的
`cryptography`，由成熟 JOSE algorithm/key parsing 路径处理 EdDSA，Agent 自己只负责受限 profile 的 canonical bytes 与业务 gate；不依赖
其他包的 transitive 安装，也不手写 Ed25519/JWS primitive。若已固定版本不能对预先 canonicalized bytes 使用公开稳定 API，则实现片先更新
ADR/依赖比较，不静默回退到私有 API 或自研 crypto。

### 7.4 Key 配置、多副本与 JWKS

worker private key 通过受控 file/secret mount 注入，使用 PKCS#8 Ed25519 PEM；以 `O_NOFOLLOW` 打开后在同一 fd 上 `fstat`，必须是
effective UID 持有的 regular file，mode 仅允许 owner-read-only `0400` 或 owner-read/write `0600`，内容非空、有界且只含一把可解析私钥。
私钥、compact proof、signature、完整 binding 都不得进入配置摘要、异常或日志，
也不得放入环境变量明文。worker active descriptor 是非 secret `kid + RFC 7638 public JWK thumbprint`，worker 从 private key 派生并
精确核对；不一致时在消费前 fail closed。HTTP 有独立的 active descriptor，必须精确指向其 public ring 中的同 kid/thumbprint，否则
`/readyz` 与 JWKS route 返回 503。两个进程不读取对方 key material；跨进程/跨 replica 一致性由下面的部署阶段门证明，不由单个
readiness 冒充原子保证。

HTTP 只加载 public ring。每个 key 必须严格为 Ed25519 public JWK：`kty=OKP`、`crv=Ed25519`、`use=sig`、`alg=EdDSA`、
非空唯一 `kid` 和 32-byte `x`，禁止 `d`、证书链、URL 或未知 member。目标 route 是
`GET|HEAD /v1/execution-proof/jwks`，visibility=`internal-owner`，但作为具名 public-key 例外不要求 bearer/tenant/body/query，仅由内部网络
policy 暴露。GET 返回标准 `{ "keys": [...] }`，HEAD 返回同 status 与 representation headers（包括 GET 表示长度）但不写 body；
已知路径其他 method 返回 405 与
`Allow: GET, HEAD`，未知路径返回 404。所有响应 `Cache-Control: no-store`，不生成 ETag/304，使 IAM 的 30 秒内存 snapshot 成为唯一
freshness cache，避免代理 cache 叠加撤销延迟。route 不重定向，JWKS representation body 必须小于等于 64 KiB（不含 HTTP headers）；这与 IAM client 固定的
2 秒总 timeout、64 KiB response-body limit、禁止 redirect、per-issuer single-flight、known-key 30 秒 freshness和 unknown-kid 最多一次
强制 refresh策略兼容。OpenAPI operation 明确 `x-kokoro-permission=none`，不能让匿名公钥读取被误解为 IAM authorization grant。

正常 rotation 固定状态机为：

1. 基线：HTTP ring=`{old}`、HTTP descriptor=`old`、worker descriptor/private=`old`。
2. 发布：全部 HTTP replica 先变为 ring=`{old,new}`，HTTP descriptor 仍可=`old`；全部 worker 仍=`old`。逐 replica 验证后才推进。
3. 签发切换：全部 worker 逐一变为 descriptor/private=`new`；HTTP ring 保持 `{old,new}`、HTTP descriptor 仍=`old`。记录最后一次 old
   签名时间；任一 worker 落后或失败都停止推进。
4. 公布 active：全部 HTTP replica 把 descriptor 切为 `new`，ring 仍=`{old,new}` 并验证；任一 HTTP replica 落后或失败都停止推进。
5. 收尾：从最后一次 old 签名起至少 70 秒后，全部 HTTP replica 移除 old，最终 ring=`{new}`、HTTP descriptor=`new`、worker
   descriptor/private=`new`。

每阶段必须观察全部 replica 的实际 descriptor/ring/thumbprint，不能只依赖 deploy 期望值。紧急轮换先停用/替换 signer，再移除 public
key；IAM 仍可能在既有 fresh snapshot 内接受旧 key，上界由 IAM 固定的 30 秒 freshness + 2 秒 refresh timeout 承担，Agent 不宣称瞬时撤销。

### 7.5 串行交付与验收边界

下列编号保留原设计的交付顺序；截至 2026-09-27，第 1–3 项的 owner artifact 已分别发布，
第 4 项 Agent consumer 是当前未完成门，第 5 项 Platform server 的代码已发布但仍 inactive，
第 6 项 Root 真实组合仍待验，不能把历史未来时态读成当前 owner 缺口。

1. Agent 独立验收 machine artifact、canonical vector、signer/key/JWKS 与 run-scoped supplier；supplier fake-client/unit test 只证明
   supplier 本身，不称为真实 Platform call 接线。
2. IAM 在固定 Agent artifact 后先对齐 ADR/API/安全设计，再实现 verifier、OpenAPI 与 generated SDK；验签端必须消费同一 strict
   profile、safe-integer/token 矩阵与 canonical vectors。
3. Platform owner 发布最终 compact-proof wire、request-binding 规则与 generated helper；只发布最终 contract，不部署临时字段、fallback
   或双协议。
4. Agent 固定 Platform artifact，在真实 Skills/MCP client adapter 的每个 owner call 边界接入 supplier，并验证每次 call 前 DB-clock
   gate、每次新 proof 及实际 transmitted bytes；这是第一阶段可称“真实 Platform client 逐 call 接线”的证据。
5. Platform server 消费 IAM generated SDK，删除 Capability 旧 attestation/手写 wire，在 fresh 与 completed receipt replay 前完成 IAM
   验证与 receipt 闭环。
6. Root 用固定三仓 commit 运行真实 Agent -> IAM -> Platform -> PostgreSQL receipt sandbox。

machine schema 与 strict parser 向量必须逐字段覆盖：`2^53-1=9007199254740991` 边界，
`2^53=9007199254740992`、`2^53+1=9007199254740993`、0、负数、JSON boolean 和 float。`lease_generation` 只接受
1..2^53-1；`iat/exp` 的 0 与 2^53-1 先通过 type/range 层，再由 `exp>iat` 与 current-time policy 裁决；其余越界/type 样本都在
crypto 前拒绝，且不截断或转成 string。

### A2b current implementation boundary (2026-09-12)

A2b separates three lifetimes: `execution_proof_keys.py` owns worker-only private PKCS#8 loading and signer construction; `interfaces/http/execution_proof_jwks.py` owns an anchored immutable public-ring snapshot; `interfaces/http/main.py` owns only HTTP business configuration and the public descriptor. The HTTP process never imports worker, private loader, signer, provider, model, sandbox, or MCP configuration. The OpenAPI `1.1.0` JWKS route is dispatched before body/auth/identity/dependency creation. Private parent directories remain a deployment-trusted secret-mount boundary; the public ring performs the stricter anchored parent walk.

A2c adds exactly one production `issue_execution_proof` caller inside the standalone supplier. IAM verification and the inactive Platform owner contract have since shipped in their owner repositories; worker signer gating, Agent Platform transport/call sites, and the real cross-owner transport remain later work.
