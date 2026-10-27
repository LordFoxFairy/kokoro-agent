## HTTP8.1 原生写文件与交付文件展示后继

本片父机器版本为 HTTP8.0.0，完整 OpenAPI SHA256 `93103b5f1df90ad4524e7fc08274211ed2f0805760d70923931e9449a59e37ff`；本片后继为 8.1.0，仅在 `ChatActivity.oneOf` 的 tool 分支 `display_code.enum` 原七值末尾追加：

| 新值 | 来源及语义 |
| --- | --- |
| `tool.file.write` | 实际官方 ToolNode 中通过保留名边界及 SDK WriteFileSchema 身份证明的原生 write_file 定义；仅声明工具类别，成功与否仍由原 status 表达。 |
| `tool.file.deliver` | 现 delivery 来源分支实际创建并经 ToolNode 对象 identity 核验的 deliver 定义；不以此类别代替 Storage 成功或 Artifact 当前资格。 |

没有新增字段或 endpoint；原 required、opaque activity／segment ID、三状态及 additionalProperties:false 保持。没有 filename、args、路径、content、result 或任意 title 的公开展示字段，产物继续由现 Delivery／Artifact 投影展示。原 Redis 工具事件、HTTP 请求、failure／proof／usage、权限、receipt 和 Schema 不变。原 tool.execution 历史保持，不根据工具顺序或产物标题重新分类。

此 minor 是需协调消费者的输出闭集扩展，不是旧严格 reader 的兼容承诺。Agent canonical → 原 `scripts/generate_failure_models.py` → OpenAPI／provenance 固定发行 → BFF 正规消费／public 后继 → App 固定消费后，Root 才准入新 producer；现有正常资源不在本片操作范围。`8.1.0 → 8.0.0` 只移除两个新枚举并还原版本后必须得到完整父 SHA，继而保留原 8.0.0 → 7.4.0 及全部更早父证明；其它 delta 拒绝。生成 Failure 的业务声明不变，仅正规生成的源摘要及 provenance 更新，不手填 hash。

## 2026-10-26 Agent8 安全工具类别（历史设计）

**基线/变更性质。** 从正常 `83e9e36efeb8086074648ea9d5bd9a3bea43d5cd` 的 HTTP7.4.0（OpenAPI SHA256 `7637757f2f8b1fc1556e62c6566db5de5eb2ae883cf6e24e918b53081cc664ad`）演进到 HTTP8.0.0。`ChatActivity` 的 tool 分支当前 `display_code` 仅 const `tool.execution`；后继把它改为下列严格枚举，保原 required 字段、opaque ID、三状态与 `additionalProperties:false`。这是输出闭集扩展，旧严格消费者会拒绝新值，不能伪称兼容的小版本或先启 producer。

| `display_code` | 可声明的公开语义 |
| --- | --- |
| `tool.execution` | 通用工具调用，包括旧历史与未获本片受信语义绑定的工具。 |
| `tool.file.read` | 官方文件读取；包括经真实后端路由读取的 Skill 文件，但不额外声明“读取技能”。 |
| `tool.mcp.discover` | 自有 MCP 固定 list wrapper 的工具发现操作。 |
| `tool.mcp.describe` | 自有 MCP 固定 describe wrapper 的工具说明操作。 |
| `tool.mcp.call` | 自有 MCP 固定 call wrapper 的工具调用操作。 |
| `tool.web.search` | 自有联网检索操作。 |
| `tool.web.fetch` | 自有网页读取操作。 |

类别只由已验证实际挂载定义/来源绑定，经 Agent 请求内可信上下文进入安全 Chat 投影；调用名仅定位受信绑定，不按原始名称/参数/结果/description 推断。native read_file 必须证明实际官方挂载来源，同名对象不足以授类别；不读取 SDK 私有字段或修改第三方 runtime。未知公开 code 整条拒绝，不默认为 generic。现 Subagent、Skill preflight、Todo、HITL、Delivery、Run failure/usage/proof 及状态字段保持原定义；不新增 `tool.skill.read`、任意标题、参数、URL/路径、远端工具名、phase、时长或隐藏推理字段。MCP discovery/describe/call 标签不证明远端执行资格、调用成功或成果已发布。

**生命周期/历史。** 同一冻结 Run 定义下的同 call 在 running/completed/failed 及合法恢复时类别一致，opaque 身份与源序号沿原算法；权限、tenant/actor、lease、取消、迟到、HITL/unknown 和 journal 不因展示改变。原 Redis `ToolInvokedPayload`/`ToolReturnedPayload` 及 PostgreSQL schema 不增字段，安全 code 仅进入既有 Chat activity payload。旧已发布 `tool.execution` 仍合法并保原 bytes/digest，重播原值；不回填旧活动、不推测 R6 工具名称、不重做旧 Run。缺新鲜来源证明不授权更具体的类别，也不以显示元数据授予执行许可。

**唯一发行图。** 本仓 canonical OpenAPI8/`contract/provenance.json` → BFF 固定 owner commit、version、完整 OpenAPI/provenance SHA256，沿 `scripts/generate-agent-http-client.mjs` 正规更新 vendor、`contract/dependencies/agent-http.json` 与生成 decoder → BFF canonical Public28 `RunToolActivity.display_code` 同闭集 → App 固定 BFF 完整 artifact/生成物并同步唯一 activity schema 与本地化。BFF 仍用 `CUSTOM kokoro.activity.updated`，compact process、snapshot、ordered parts 与 live 必须同值，不加第二网络事件；App 不直读 Agent 或复制 Python 类型。跨仓 consumer 写入与正常装配由各 owner/Root 后继承担，不由本仓设计稿冒充已发行。

**本仓机器/生成门。** 精确候选写区见本仓技术设计本节；只改 `contract/openapi/v1/openapi.json`、`contract/provenance.json`、`src/kokoro_agent/contract_check.py`、`src/kokoro_agent/chat_contract_check.py` 的版本/闭集/来源核验，沿 `uv run python scripts/generate_failure_models.py` 正规刷新 generated Failure 源摘要与 provenance，再用同命令 `--check` 和 `uv run kokoro-agent-contract-check` 核验。生成的 Failure 业务 tuple、proof2、usage2、其它 owner pins 和原独立生成链不得变化；不手填生成字节或摘要。HTTP8 → 完整7.4父原件的逆证明只用于测试，保旧机器约束与历史 hash，不在运行时双读/降级。行为、contract、安装分发及持久 replay 风险按技术设计既有测试路径核验，无新 endpoint、SQL、Redis 字段、依赖或配置。

**配对/回滚界限。** 发布 artifact 与正常激活分开：BFF/App 先能严格读取上述完整闭集，再由 Root 成套准入 Agent8 新 Run；不修改正常源或已有 frozen recipe。新 code 已持久化后，停止新 producer/派发不等于旧 reader 已可回退，恢复期间须保能读取这些事件的固定消费者；不删除/改写历史事件来降版。Root 已复核设计门并授权本仓候选业务/机器实施；实际验证与冻结收据另交 Root，正常资源与跨仓配对仍须 Root 独立授权。

## 2026-10-25 Personal MCP SSE runtime 消费（候选已实施，尚未正常激活）

正式 producer 固定 Platform `8cfead647856ef54ec851a3bebe169a3ca14d212`／Runtime1.5。现 `ResolveMcpRuntimeConnection`／`AcquireMcpRuntimeCredential` 两 RPC、operation `mcp.resolve_runtime_connection`／`mcp.acquire_runtime_credential`、当前 E2 与 binding3.0.0 不变。新 resolution `sse=10` 与 credential `sse=5` 必须同 case；exact connector/connection/server/selector/declaration、resolution reference SHA、discovery/invocation purpose、grant/audit/原参数 bytes／approval presence／key、trusted Run/tenant/lease 与最小 expiry 均沿原检查。HTTP／managed STDIO不降级、不兼容错 case，SSE不伪装 HTTP；BFF Public26只运输ID，不授执行权。本仓只消费该已发行 wire，不新增 Agent API/Schema/依赖。

通过现官方 SDK 的 typed `SSEConnection` 与具名 SSE-only factory执行，不新增浏览器事件流或第二 RPC/session 协议。广告 endpoint 原文及解析后的含 query URL均≤8,192 UTF-8 bytes；同 scheme/host/有效 port，无 userinfo/fragment/控制字符/反斜线/redirect/降级，当前 DNS 全集合资格／IP pin／TLS不由已完成 Prepare替代。临时query只在会话内存，GET流累计及单POST ACK各≤4,194,304 bytes；原30秒绝对调用与材料min-expiry覆盖读/EOF/退出，不分步重置。202 Accepted只ACK，有效结果仍取官方流上exact JSON-RPC id。

压缩策略只作用新SSE：未配置accept-encoding时默认identity，已配置 Header原值保持、无覆盖或额外请求值限制；transport原始响应仅接受无Content-Encoding或单一identity，非identity明确失败并关闭，不自动解压或重试。identity表述体字节接受后才按硬限计数，不以压缩wire界代替解码体积界。POST可能已产生效果，压缩／超界／断流失败不证明未执行，原journal unknown/failed与原key恢复、不重派保持。

局部SSE ContextVar＋实际输出handler/caplog端净化按锁定MCP模块`record.pathname`覆盖`mcp.*`及其root/client命名的shared/session/TaskGroup路径；不能只按logger名称。HTTPX/httpcore仅本factory request/read/close来源进入净化；保闭合安全phase与原level/source，剥query/header/原异常及traceback参数，不吞业务错误／日志。其它root/client来源、非SSE及其它并发HTTP记录不变；不调进程全局日志级别、不改RecordFactory或SDK源码、不另建通用日志框架。若有QueueHandler，须在生产task的enqueue handler前净化，不在listener线程依赖ContextVar；锁定SDK离线拓扑为root StreamHandler／相关logger传播且无自身handler，非正常进程探测。所有SDK child teardown完成才释放scope/filter引用，失败／取消／HITL均清材料与连接引用；原HTTP/STDIO生命周期不变。

唯一固定来源仍本仓provenance/vendor Proto/runtime profile及原正规generator。起始Agent Runtime1.1 descriptor不同于producer的1.4父图，不能以1.5→1.4逆证冒完整旧1.1；新current-pin与旧历史父证明分别绑定、原两op/v3向量和独立operation24/service-auth/package pins保持。Root已通过设计门，候选已按正式对象正规生成；本段不声明正常Agent/SSE/同Run已可用，旧Run不重派。

候选另承接锁定SDK会吞POST异常的边界：仅已观测本会话transport失败置latch，managed正常退出前核验；GET exact-id结果先到或后到均不把已拒绝的压缩／超界ACK冒成功。未观察完成的合法ACK、普通close、取消／HITL不置失败、不重派；官方SDK实际loopback与先后序负控证据由最终候选收据绑定。

SSE SDK入场要求root已配置实际handler；缺失时静态MCP_SESSION_FAILED且零SDK/网络调用，不自行basicConfig或加handler。原worker入口先basicConfig再装配执行；HTTP入口不执行MCP。现handler与lastResort的自身filter引用按并发会话释放，运行期间其它组件整体重配logging不在本片支持范围；不以被level抑制的info/debug来发现懒创建sink。

## 2026-10-25 原生摘要的对话流隔离

框架标记 `metadata.lc_source=summarization` 的内部摘要仅在现 summary adapter 的调用 config 追加锁定 SDK 原生 `TAG_NOSTREAM`，不作为公开 assistant delta/completed；不按标题或正文过滤，普通主回答即使含相同标题仍正常流式发布。实际 provider stream、真实用量、摘要 capsule、原生 `_summarization_event`、seal/金融 ACK、失败与取消语义保持；没有新增 wire/Schema，也不改写既存 Chat 投影。

## 2026-10-25 SDK send 安全cause诊断

仅在既有model-call handler捕获真实SDK `APIConnectionError`／`APITimeoutError` 时写私有诊断：固定SDK类别、最多4层显式 `__cause__` 的白名单类别、complete/cycle/truncated与现call/attempt UUID；截断或循环的root为unknown，不按名称/文字归因。真实 RuntimeError、OS拒绝/重置/中止连接及SSL错误仅映射固定类别，不按文字或errno推断loop关闭/供应商故障。无异常text/repr、URL/header/body、traceback或凭据，无新事件/Schema/wire。日志失败不替换primary；原denial还原、secondary settlement、pending ACK／hold、single-dispatch、零自动重试和摘要语义保持。此诊断不证明供应商故障或当前可用性。

## 2026-10-25 SDK 连接失败的安全分类

仅真实 OpenAI SDK `APIConnectionError`／`APITimeoutError`（`isinstance`）归为既有 `model_unavailable`、`retryable=false`，优先于装配调用点提供的缺省码；同名普通异常或错误文字不参与归类。此为安全失败分类，不证明供应商 TCP/DNS/TLS 子因或可用性，不增加 wire／重试。原 API 主异常、secondary settlement 处理、pending financial ACK／hold、single-dispatch、历史终态和 preflight/start 顺序保持，不释放资金或重派模型。

## 2026-10-24 普通会话 MCP 精确选择（Agent 候选实施，尚未正常配对）

本节目标已在 Agent 候选唯一机器源 contract/openapi/v1/openapi.json 7.4.0 实施；BFF/App 尚未配对，正常服务仍运行7.3，不冒整链已发行。

现POST /v1/runs的LaunchRequest新增selected_mcp_connection_ids：optional array<string>，省略/[]均表示没有额外的用户选中MCP；最多16项，唯一且保持调用者顺序，每项^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$，JSON ASCII编码至多4096 bytes。类型、重复、超长、URL、空白、嵌套对象或secret字段错误均为原400 invalid_launch_request。不从trace或mcp_servers读取该字段，不引入新RPC或错误码。

LaunchBody→RunRequest保持同一精确列表；空值由唯一Run序列化规则省略新增字段，使既存request_json/fence原字节可保持，非空必含且同Run变更拒绝。恢复/重试继承原选择，不重新读取账号开关；请求不接受credential、endpoint、tool list或policy。固定artifact及parent完整证明先交付给BFF，消费者不得先手写wire。

## 2026-10-24 主链用量待核对的具名失败（实施中）

沿既有失败/unknown资金策略，仅扩展安全failure码model_settlement_pending且retryable=false。只有匹配原admission/run/attempt/event/revision/digest的已记录pending_reconciliation ACK可产生此码；缺回执、错绑定、网络未知仍原dependency_unavailable，不能凭错误文字推断资金状态。旧通用失败历史不改写。原sealed证据、ACK、预占/结算、single-dispatch与后续调用停止保持；不发新模型或改费率来刷成功。HTTP7.3只追加该枚举，完整7.2父字节证明保全部旧API；BFF正式固定后再接App，不先正常激活半条链。

## AGENT-1：供应商响应编号的内部观察（2026-10-23）

HTTP7.2、usage2、BFF settle wire 不变。内部 PhysicalUsageObservation 与 SealModelCallObservation 增加可空 provider_response_id，限1–256个可见 ASCII 字符，来自同次已消费响应的 id；不接受 SDK 后合成编号。流式所有出现的 id 必须有效且一致，缺失保持未知。该编号不进入日志/公开事件/usage wire，不改变原 raw_usage_digest；只作 Agent 后续核查索引，不能用于签发权限或自报新用量。原 first-seal replay 同时比较编号，禁止替换或丢弃。

## 2026-10-23 Platform 执行认证 consumer 配对（实施中）

固定 Platform `4c3853b1062a9e22c9be6c059720a092805cb63f` 的 `contract/auth/agent-execution-service-auth-v1.json` 原件，
仅替换机器认证为固定 caller/tenant/surface 服务凭据。既有业务 Proto、request digest、fresh execution proof 和 IAM 当前用户权限判断保持。
私有文件 version=1，entries 每项严格 credential_id/caller/surfaces/tenant_id/secret，Agent 独占单用途且每 tenant 唯一；
不接受旧 OAuth 格式、generation、client_id 或隐式默认 tenant。秘密只经受信连接的 Bearer 头，不进 Run、日志或文档。
现 Platform provenance 追加独立 service_auth pin，不重写未变化的 Proto/operation 来源；owner 原件只读，checksum 由固定 Git blob 产生。
没有新增公开 API、数据模型、服务或用户登录流程。此次只是 consumer 实施门，不是正常激活或完整验收收据。

## AGENT-1：Worker恢复接线（2026-10-23，本地已验）

原HTTP7.2／BFF1.3／proof2／usage2均不变；仅在现Worker装配原BFF pool和逐事件恢复。内部机器secret按可信tenant选择，部署snapshot不进入Run，不复用用户／System／模型密钥。恢复只调用recover／settle，原六事实及event/key保持，无首次reserve或proof；未配置时启动拒绝，无免费／旧协议fallback。可观察产品功能和新派发尚未因此开放。

## AGENT-1：本地金融投递回执（2026-10-23，本地已验；worker装配待接）

不新增HTTP／RPC／wire版本。既有BFF1.3 consumer原请求／原key及ACK使用不变；Agent存独立预算关联与完整本地canonical ACK／domain digest供崩溃恢复，不对外投影金额或更改sealed证据。历史reader同cursor复用保持HTTP7.2语义与原canonical bytes。requested_unknown恢复不改granted授权、不授旧fence执行权；miss只留待核对。

## AGENT-1：固定 BFF internal1.3（2026-10-23，本地已验）

固定5a303f2003fabeb50376c7f7e1f66f0d163adebb原始YAML及SHA4de1a6c7564f3a994d879cfc93588021c5ef4aeebbcc89959fa574d5b6f16d39；现唯一receipt扩为create／recover／settle三操作，无新可编辑DTO。recover仅200原held创建ACK或精确404miss，settle仅200原sealed命令ACK／原错误，均禁proof／用户token；首次create仍201＋fresh proof。客户端严格核可信调用参数与ACK身份，不重算费用、不补0／夹预算／重发provider。三态及完整字符串限制由owner生成模型承接，不复制手写wire schema。

## AGENT-1：精确历史证据读取（HTTP7.2，2026-10-22，本地已验）

仍为GET /v1/model-call-attempts/{attempt_id}/usage-evidence/{event_id}，原专用Billing机器Bearer／受信tenant、无query/body／现大小与取消界限保持。HTTP7.2明确任一immutable sealed历史修订按原event返回，不限当前head；响应原bytes／artifact版本／revision／digest／observed_at不改写。请求row及独立实际head均须完整，坏head不因读历史绕过；其他tenant／attempt／missing／unsealed不暴露，不增加人类／服务caller身份。

原7.1整件由仅撤销本语义description及version精确逆证明承接，既有7.1→7.0门继续；usage1只读历史／usage2当前生产与proof2原件不变，Billing须后继固定新HTTP发行。历史读取不代表新执行许可、当前成员资格或资金ACK，不新增恢复写旁路。

## AGENT-1：本轮预算结算的实际归属 profile（2026-10-22）

通用usage2继续接受原合法Opaque归属，不改变schema／vectors／artifact版本。本轮预算业务只允许
原精确System模型，不开放actual fallback集合：用于该业务结算的verified.provider_ref／model_ref
分别表示经具名认证上游证据确认映射的System provider／model目录ID，不是上游raw model名、
显示别名或Billing商业引用。映射证据必须绑定同attempt、最终请求及冻结revision的执行目标；
不以目录存在、复制planned或单独response.model自证。不能确认映射时保持actual unknown。

Billing的局部收费profile只在这两个实际目录ID逐项等于原frozen planned.provider／model时继续，
再沿原revision ID/digest→已冻结商业五元组／费率计算实际用量；其他命名空间、缺依据或超出
此集合的真实归属保原证据和hold，进入pending，不套planned价、不另选现价。这一相等是必要
条件，不替代原身份、授权、seal、usage/profile或预算校验。后继真正producer装配必须实现
上述认证映射及错归属负控；当前尚未装配／启用，不能以本说明声称真实费用链已完成。

## AGENT-1：唯一 BFF internal1.1 预算消费（2026-10-22）

固定 consumer：BFF 0082b5cee4437367a608b5bd37732674a1622664，原 internal1.1 YAML SHA256
695901b23f1f0322fd18ec2a11be27c5b6efcce07b04f57839498eab402d17c6，build-only 生成 strict Python 边界。
只 POST /internal/v1/model-admissions：独立机器 Bearer、Idempotency-Key、x-billing-execution-proof；
body 仅 run_id／attempt_id／request_ref／attempt_binding_digest／planned_digest／final_request_digest。
用户身份由 BFF 原提交事实派生；不发送 tenant/payer body、浏览器 token 或旧 consumption header。
201 必须完整闭集、原六事实、held、created_at=updated_at、合法 UUID／整预算／摘要／毫秒UTC；
映射 admission_id 为包内 admission_ref，Agent 不读取或计算价格。错误码与 status/retryable 精确配对。
unknown／invalid ACK 不证明未预占，不自动重试；恢复只沿原 request_ref/key/body。

不变更 Agent 自有 HTTP、RunRequest、proof2 或 usage2；normal worker／finalhook 未接入。

## AGENT-1：billing.credit.consume 的现 proof2 绑定（2026-10-22）

不新增 HTTP、Redis wire 或 proof schema 版本。原 supplier 对具名 `billing.credit.consume`
明确填入既有 execution_binding；三个原 Skill operation 和其余 operation 的现行为保持。
输入仍是 operation＋request_binding_sha256，身份来自已绑定的 Run／lease，不接浏览器 token、
payer 或模型自报身份。后继预算消费者须按 BFF 内部正式 Admission 契约计算摘要并调用本 supplier；
本片不发布或假造 Agent 预算客户端、模型外发许可、capture ACK 或结算结果。

## AGENT-1：当前调用准备事实读取（HTTP7.1，2026-10-22）

`GET /v1/model-call-attempts/{attempt_id}/prepared`，专用 Billing bearer，tenant header；
无查询、正文、Transfer-Encoding 或人类 identity/service 覆盖头，canonical 小写 UUID。
成功沿现 owner envelope、no-store 和 request-id header，返回 original canonical RFC8785 UTF-8
bytes 的无 padding base64url 与普通 SHA256：attempt binding、planned、当前 authorization。
attempt binding 是 usage2 EvidenceIdentity 去掉 event_id 的精确字段集；planned 为 usage2
PlannedBinding；authorization 随当前 status 为 not_requested/requested_unknown/granted。
所有字节分别1..65536，数字序号／fence generation 沿 usage2 十进制字符串；无 Key、endpoint、
完整消息或供应商商业归属。另返 binding_version=2.0.0、attempt_id、status、final request
binding version/digest、prepared_at、observed_at、lease_expires_at，时间 UTC 毫秒。

404 不存在／不可见；409 model_call_prepared_unavailable 表示当前 lease／状态不适合新准入，
不是未派发证明；500 model_call_prepared_invariant_failed 不降为缺失；依赖503保持。
只新增此 operation 和两个 schema，旧 HTTP7 完整表示不改；当前版本7.1由正式生成器刷新摘要。
用途、首笔资金准入与历史恢复边界见 TECHNICAL_DESIGN 首节；不沿旧 R252 创建 bound artifact。

## AGENT-1：预算授权与结算的最新裁决（2026-10-22）

用户已确认预算预占→真实用量一次整数结算，取代旧额外供应商成本上界保证前置。本片不修改Agent HTTP7/proof2/usage2、schema、client或鉴权；R252旧capacity-bound新artifact/reader不再是资金链前置。后继在现可信attempt／final digest／exact model route上接正式Billing授权及严格回执；预算和price由Billing管理，不允许前端/Agent自报actual金额。unknown或超预算保留原资金与证据、停止同Run新派发；只有明确未派发可释放，取消/超时不是零用量证明。运行仍缺正式Billing consumer，未由本文放行。

## MC-02：受控领取契约（2026-10-20，实施门）

唯一 System 消费在既有 resolve 外加入 `POST /v1/system/model-catalog/credentials/read`，固定 System6 正式 source/artifact/provenance，拒旧/新混用。领取是无幂等写入的只读 POST，服务身份仍 kokoro-agent，可信 tenant 与原始选择保持；精确版本只作一致性比较，不授予历史路由读取权。仅接受200、no-store及严格字段/Key字符域，错误/超大/错误版本/错绑定/跳转均脱敏失败，不重试或返回给 Web/BFF。无 Agent public/Redis/usage 契约修改。

固定消费为 System `550d65498822d8d2e629199482de4f6990b62708` /6.0.0；artifact `10ff7ee82ca7405dcab473f3fd80e620aa6c4e0bc92e64111797144ce0fa4061`，provenance `ed2e312bc5bc3022111797771190e57133818af83c0bb7ad2024a615a4e033c5`，22来源与两操作receipt经Git原件及安装发行物检查。组件验证与正常未激活边界见CURRENT。

## MC-02A：System5 固定依赖（2026-10-20）

唯一消费操作仍为 `POST /v1/system/model-catalog/resolve`，固定 owner 提交 `b7fe78e25e3c569ae0e1cb9b42cddab02074ca07`、版本 5.0.0 与原始 artifact/provenance/20 来源摘要；不混用旧新 pin。System4→5 的该操作及递归 schema 已逐值核等，Agent HTTP7、proof2、usage2 和其它 owner 固定依赖保持原样。

业务 feature_key 与稳定 label_key 仍经现 resolve 校验；展示别名不替换原 request_model，不充当 provider/凭据身份。receipt checker 只核本地精确收据，Git 原始来源核验属于组合交付证据，不将前者称后者。

## Raw通知不要求外部ACK（2026-10-19，实施门）

当前HTTP7/Redis机器artifact与所有外部字段不变；BFF仍只读Agent durable Chat并写自身ledger，没有raw ACK endpoint。内部published通知不等待并不存在的外部receipt producer，移除宽限重发参数/配置及内部republish候选；Run/control admission receipt不是该通知回执。queued失败重试与精确Chat恢复不变。既有真实rejected和损坏历史保护继续，不把可丢通知称BFF已消费；不新增公开协议、自动模型/工具调用或扣费。

## HTTP7 ControlReceipt 输出收敛（2026-10-19）

`POST /v1/runs/{run_id}/control` 继续按原正式 `ControlReceipt` 输出 command_id、request_digest、status、replayed 和可选 error_code；内部入账模型的 run_id 不属于该 HTTP 数据体。首次、命令重放和 Redis 入队失败均通过同一个字段白名单映射，禁止直接序列化内部模型。命令摘要与可信身份/session/Run 核验不变；HTTP7 artifact、版本与 BFF 固定消费者均不变，无旧/新响应回退。

## D6-B MCP 当前运行消费者（2026-10-18，本地候选）

不新增外部HTTP/OpenAPI、Run请求或owner RPC。现RunPlatformClient exact allowlist由8扩至9，仅增加固定v4已有的ListMcpServerDeclarations；唯一Proto/Runtime1.1 pin及原proof算法不变。Agent代码声明为McpConnectionRef(connection_id)，不是名称、URL或执行命令。稳定三工具的公开name/description/schema保持，server参数只匹配本Run已声明Connection ID。

同一调用固定原typed_arguments_json与SHA、Run/call派生idempotency，Authorize回包须匹配connector/connection/server/selector/原SHA及当前grant/audit/expiry，再用同一原bytes取得invocation材料。approval缺省与空字符串不互换，不接受模型注入的grant/approval/tenant/run或runtime调用ID。Discovery只用于list/describe。跨owner失败和SDK错误不返回URL/header/env/原异常；标准MCP isError映射原journal failed，GraphInterrupt或取消的不确定副作用不重发。

schema采用owner当前JSON Number语义；typed arguments只需是严格JSON对象（至多1MiB），大整数不误套proof安全整数域。静态profile的mcp是闭集[{connection_id}]，旧名称数组不作兼容恢复；旧历史绑定原样保存，新构建仍经原fingerprint一致性检查。生产当前准入/实际IAM组合尚未据此宣称验收。

## HTTP7与execution-proof2目标（2026-10-17）

HTTP7仍/v1，仅LaunchRequest新增required nullable project_ref，所有producer显式提供，Redis发布/恢复保留null。proof唯一current2.0.0增required execution_binding:null或closed {identity_assertion_ref,project_ref(nullable),selection_digest(64hex)}；refs不重复塞JWT，由IAM读BFF并核摘要。旧v1仅冻结历史artifact，无runtime fallback。JWKS/签名算法和现Platform请求digest不变。

## MC-01：模型连接消费者契约（2026-10-15；目标，未发布）

System 当前固定 artifact 3.0.0 不变。用户已确定默认 direct、可选 LiteLLM；后继只从 System 正规发布的 breaking artifact 接收明确协议、连接与唯一请求模型绑定，version/commit/artifact/provenance 一起 pin。旧 payload 缺少新连接字段时拒绝，不从本机环境补成成功 route。

Agent 不给普通 Web/BFF 返回上游地址、secret reference 或密钥。请求中任意自报 endpoint/provider/account 不覆盖 System 路由与 worker secret 配置。资金授权、tenant/subject/Run/lease fencing 和现有 AG-UI 网络边界不因拓扑变化而改变。

现 usage `PlannedBinding` 的两份旧模型名称属于已发布机器契约。最小 usage 2.0.0 保留 provider_model_name 的声明元数据与其余语义，仅以 request_model 替换 gateway 专属执行名称，增加显式 planned binding_version=2.0.0；不重复嵌入 System 的整个连接快照。独立 v2 schema/generator/strict decoder/manifest 已在候选源码实现，公共发布检查和安装资产闭包同时验证冻结 v1 与 v2；v1 六份来源和 manifest 原字节不变。v2 evidence 使用独立 v2 digest domain，跨版本输入不回退，3 正向、29 反向及10 successor pair 由公共检查执行。不在 System 或 Root 复制一份可编辑 usage schema。

本前置切片不改变 HTTP5、System3 pin、RunRequest、AG-UI、SQL 或实际 producer。HTTP6、durable 历史读取分派及 Billing 消费固定仍待后继原子切换；现调用不会因为安装了 v2 模型而自动写 v2。

同一 evidence GET 在 HTTP6 中按已验证 evidence 的 artifact_version 返回 envelope 与原 bytes：明确接受冻结历史1.0.0和当前2.0.0，未知版本拒绝；不按当前部署常量返回1.0.0。新 planned 仅接受显式2.0.0，旧未sealed attempt须由旧producer收束后才切换。历史 v1 decoder 不生产新请求，解析失败不重试另一版本；不得把旧记录重新执行。Billing 当前只有目标artifact/pin，实际 reader/资金consumer尚未接通，需后继正式pin并验证，不能称已可消费。

worker 私有凭据文件与loopback开关的精确表示见技术方案MC01；不新增HTTP字段或浏览器凭据入口。System4的connection/protocol/type/base_url/secret_handle_ref仅在内部严格消费，不投影到usage planned。

验证包括直连/实际网关的协议 wire、同 path 多连接、alias/request model 绑定、缺/错凭据、错误 artifact 与取消零发送、最终限额、usage 绑定和历史读取不变。自有 peer 不称真实 provider/gateway；远端收费与归属仍需来源及正式资金准入证据。

---

## R353-A-S06：Storage v3 Artifact consumer contract（候选已实施；2026-10-08）

### Wire authority 与可见性

本片不新增 Agent public HTTP/OpenAPI、Redis event 或浏览器字段。唯一 wire authority 是 Storage commit `d13f33acfc7a482d61ebffc58e7b2667b58ae197` 的 `kokoro/common/v1/common.proto` 与 `kokoro/storage/v3/storage.proto`，组合 SHA-256 `47e2659f577cf5c0a5e44a9ee9165d0fee17471bb3d7abb80095a956f655c1f9`。Agent vendor pin与原generator已单轨切到 `kokoro.storage.v3.StorageService`；完整17 RPC生成可见，但生产只允许 `CreateUpload/CompleteUpload/AbortUpload/GetUploadStatus/GetScanStatus/CreateArtifact/FinalizeArtifact` 七操作，其余继续 `STORAGE_OPERATION_FORBIDDEN`。v2 vendor/generated已删除，无alias/fallback/双pin。

### PUT ACK、Complete 与status核验

signed PUT继续只允许批准 object origin、PUT、未过期且不超过901秒、exact `content-type` required header、无redirect/cookie/bearer/internal secret。2xx必须返回合法 `x-amz-version-id`：1..512、非空/纯空白/字面`null`、无C0/DEL。consumer不trim已接受值，原样返回、持久、摘要和发送。A不是授权或CLEAN证明，只能与同upload/descriptor/trusted conversation scope一起发v3 Complete。

全部owner command使用既有确定性JSON编码与新 `delivery-v2` namespace：

```text
seed = SHA256(JSON([run_id, tool_call_id]))
command_id = SHA256(JSON(["delivery-v2", seed, stage]))
request_digest = SHA256(JSON(["delivery-v2", stage, fields]))
complete.fields = [upload_id, content_sha256, size_bytes, source_version_id]
```

此JSON为Python `sort_keys=True`、compact separators、默认`ensure_ascii=True`，不是RFC8785/JCS。A与Complete command/digest在首次Complete前同一次selected journal transition持久化；outbound Complete使用该exact identity与A。

GetUploadStatus逐项匹配 upload_id、expected_sha256、expected_size_bytes、mime_type。`PENDING + local A + owner同A或缺省A` 只重发Complete(A)，零PUT；`COMPLETED`还要求同A与asset_id。两边无A时recover不产生副作用，publish持有原bytes才可重新PUT。本地/owner A冲突、owner有A而本地无durable selected identity、completed缺A/asset都失败关闭。

### 内部 typed errors 与稳定成功形状

| code | retryable | 语义 |
|---|---:|---|
| `STORAGE_UPLOAD_VERSION_ID_INVALID` | true | PUT可能已成功，但2xx ACK没有可持久的version identity；不Complete。 |
| `DELIVERY_INTENT_VERSION_UNSUPPORTED` | false | started-v1专用unknown诊断；不调用handler/recover/finish，row保持started。 |
| `DELIVERY_SOURCE_VERSION_CONFLICT` | false | local/server locator不一致，或server locator没有对应本地durable Complete identity。 |

既有 lease、身份、origin、scan/final、Connect错误分类保持。成功 `DeliveryReceipt`/`DeliverResult` 仍只含artifact/asset、Storage权威kind、digest、size、MIME与replay信息，不向Chat/BFF暴露A。旧 succeeded结果直接重放最终摘要，不调用v3。纯contract/codegen已通过；真实owner配对及S06产品链由Root另验。

## T-A01.R257-A1 当前实现与验收边界（2026-10-06）

正式源码已实现 final usage evidence 专用读取，Root有效目标404 RED后完成；现十四文件冻结与独立复审无剩余已证P0–P2。
唯一机器事实源contract/openapi/v1/openapi.json已声明internal-owner GET；200为闭合八字段，专用认证先行，错误credential含Latin-1输入均401，身份/body override403，不可见404、corrupt500，private no-store与本次request ID。
Root最终真实HTTP1与完整PG90通过（PG会话America/New_York），自有资源回收，正常owner/provider/资金未写或重载。
此证据只接受读取切片；Billing admission/actual结算、bound guarantee、消费者装配、真实模型用户链仍OPEN，不把该读operation当付款许可。

### 原A1实施前设计记录（下节为历史基线，当前事实以上文及机器契约为准）

## T-A01.R257-A1：final model-call usage evidence internal-owner GET（D0，未进入机器契约）

**当前态。** `contract/openapi/v1/openapi.json` 没有本operation；现 `/v1/runs/{run_id}/events` 是BFF/用户范围执行事件，
不返回strict model usage artifact。下列目标只在Root安全裁决与真实RED后写入机器事实源，不是当前可调用接口。

```text
GET /v1/model-call-attempts/{attempt_id}/usage-evidence/{event_id}
owner: kokoro-agent
visibility: internal-owner
stability: candidate（首次发布前）
permission: kokoro-billing / model-usage-evidence.read
idempotency: read-only
```

- path均为canonical UUID；tenant只来自认证成功后的受信上下文，不从body/query、actor/subject或未验证header取得。
- 不提供list、`latest` alias、run/session lookup或cursor；Billing必须持有已绑定的exact attempt+event identity。
- event必须匹配tenant/attempt且等于attempt current `latest_event_id/revision/digest`，attempt已sealed、evidence `sealed=true`；旧revision返回404。

目标200表示如下；最终字段只由OpenAPI维护：

```json
{"data":{"artifact_version":"1.0.0","attempt_id":"UUID","event_id":"UUID","revision":"1","evidence_bytes_base64url":"BASE64URL_NO_PADDING","evidence_digest":"LOWERCASE_SHA256","sealed":true,"observed_at":"RFC3339_UTC"}}
```

`revision`为canonical十进制字符串；exact bytes用无填充base64url，digest对象是解码后原bytes。响应带`x-request-id`与
`cache-control: no-store`且有界；不含prompt/request、authorization/planned raw bytes、credential、金额/价格/hold/ledger、Chat或用户资料。

### 认证与错误

Root已固定HTTP-only `billing_model_call_reader_secret` / `KOKORO_AGENT_BILLING_MODEL_CALL_READER_SECRET`；旧bound-only名无alias/fallback。
常量时间Bearer匹配只映射caller=`kokoro-billing`，具名helper只授`model-call-bound.read`与`model-usage-evidence.read`，本operation只要求`model-usage-evidence.read`；不产生全HTTP权限或通用权限引擎。
共享`KOKORO_INTERNAL_SECRET_AGENT`及身份headers不得获得200。当前配置/helper/route未实施，不冒充受信caller或IAM token。

| HTTP | 稳定语义 |
|---|---|
| 400 | 认证与tenant断言通过后，path/request形状非法；不回显输入。 |
| 401 | reader credential缺失、Bearer scheme错误或值不匹配；静态secret没有token expiry语义。 |
| 403 | credential已匹配，但tenant断言缺失/空白/畸形，或携带试图覆盖身份的service/actor/subject/assertion/body字段。 |
| 404 | tenant不可见、attempt/event不匹配、非final、未sealed或不存在，统一不泄露。 |
| 500 | persisted final evidence违反Agent不变量；非retryable，不暴露SQL/bytes。 |
| 503 | reader auth未配置、数据库不可用或有界deadline；retryable由机器码固定。 |

鉴权先于tenant断言、path UUID解析与任何SQL；未配置secret直接503，错误credential直接401。GET无body，不接受普通ExecutionIdentity headers替代service authorization。成功读取没有ACK含义且零写。只有transport失败和契约标记
retryable的503可有限retry；404/500不自动retry。

实现顺序：Root安全裁决 → OpenAPI与contract tests RED → 真实HTTP+PG 404业务RED → repository/server实现 → digest/provenance与
consumer固定。验证error envelope、request-id/no-store、method/path负例、response size、base64 roundtrip与artifact digest。当前不改
OpenAPI，Billing不得复制本文JSON提前造client。

## R252-AGENT-BOUND-D0：目标 artifact 与受认证 read（2026-10-05；未发布）

本节只冻结后继机器语义；当前公开机器事实仍是既有HTTP5、execution-proof与usage/v1，下面的`model-call-bound/v1`、route、version和digest尚不存在，不能被Billing pin或用于授权。

目标 `contract/model-call-bound/v1` artifact version拟固定`1.0.0`；下列是已冻结语义骨架，最终closed schema还必须纳入本节后述正式guarantee的exact机器身份，在该owner/source获批前不得生成schema/positive vector/manifest：

- `artifact_version="1.0.0"`、`profile_id="openai_chat_completions_capacity_v1"`、`unit="token"`、`revision="1"`、`sealed=true`；
- identity：`tenant_ref/run_id/call_id/attempt_id/attempt_ordinal`及原`lease.owner/generation`，producer固定`kokoro-agent`；
- planned：完整现 `PlannedBinding`、`planned_binding_ref="mpb_<attempt UUID>"`与其现canonical SHA-256；
- final request：binding version=`1`、现final digest、`request_profile="chat-completions-materialized-json-v1"`及final wire正整数`max_completion_tokens`；不含body/headers/secret；
- System provenance固定为contract `3.0.0`、source commit `955bd587f46a771e43a4330d202d90a7d2d67262`、artifact SHA-256 `94ed91655741cbba66ce80a4c41717b9179cc509cb6858534e0b78aa3e6dc059`、provenance SHA-256 `5fda620ca1526f57d8759b2c6282ed6d674455a78e8ff9ee7fc0522e8f5a3832`、19-source receipt `aaf1428aa898592fb370021790d5e55cb3dd43b44080696dd2ec99f91904408b`，并携同planned revision的`context_window/max_output_tokens`；这些字段只证明目录来源，不是计费cap guarantee；
- constraints是closed joint relation：input/output两个root有限上界、`total=input+output`、cache-read→input与reasoning→output的inclusive subset；cache-write/未知category、模态、协议进入显式unsupported闭集而非填0；
- `bound_ref="mcb_<attempt UUID>"`、UTC毫秒`sealed_at/valid_until`。两瞬时点分别由同一事务DB clock和锁内lease expiry向下量化为不晚于原值的整数毫秒；payload不含自身digest，`execution_bound_digest=SHA256(RFC8785(payload))`由持久列/read envelope给出。

整数都拒绝bool/float/string混型；count为非负int64，context/output/final cap为positive int32且`final cap <= max_output_tokens <= context_window`；量化后`valid_until > sealed_at`。数据库不得依赖TIMESTAMPTZ(3)隐式round，也不得由应用时钟或两次now分别生成二值。同attempt exact replay必须返回原bytes/digest/time；任一planned/final/System/profile/limit/lease漂移是identity conflict，不生成第二bound、不静默升revision。首版revision恒1且sealed，未来变更必须新attempt或新breaking artifact，不能原地续租/重算。

**未满足的正向发布前置。** `openai_chat_completions_capacity_v1`只有在具名trusted owner发布versioned capacity-and-usage-semantics guarantee，且Agent固定其owner/version/digest并核其精确覆盖本planned provider/model/gateway后才可进入schema合法正向vector。该保证必须明确billable input/output cap、cache/reasoning inclusive关系及不存在未声明收费category；System 3.0 contract/provenance当前不含这些断言。因此当前没有合法positive bound fixture，producer/read/paid Admission必须fail closed。后继owner/source由Root另行批准；本合同不预造`guarantee_id`占位字段、不把provider网页/YAML/header/credential/tokenizer猜测塞进artifact。保证正式发布后才把其精确机器身份加入本schema并固定新freeze SHA，不得以本D0文字当机器证据。

目标internal-owner operation为 `GET /v1/model-call-bounds/{bound_ref}`。成功仍用现request-id envelope，`data` closed字段为：`planned_binding_ref`、`planned_binding_digest`、`planned_binding_bytes_base64url`、`execution_bound_ref`、`execution_bound_digest`、`execution_bound_artifact_version`、`execution_bound_revision`、`execution_bound_bytes_base64url`；base64url无padding，consumer解码原bytes、strict parse、要求RFC8785逐字相等并重算两个digest。一次read同时关闭planned+bound tuple，不新增generic artifact endpoint；响应不含prompt/final body、actor/subject明文内容、authorization bytes、Billing request/admission、金额或secret。

read只接受 `Authorization: Bearer <KOKORO_AGENT_BILLING_MODEL_CALL_READER_SECRET>`；专用`SecretStr`常量时间匹配成功即唯一映射caller=`kokoro-billing`，现共享`KOKORO_INTERNAL_SECRET_AGENT`即使合法也不得fallback到本route。成功后才接受必填`x-kokoro-tenant-ref`作为该受信Billing caller的tenant断言并与artifact逐值相等；`x-kokoro-service`、actor/subject/assertion headers和body不参与认证或授权。URL ref也不构成权限。repository以tenant+ref定位后，在一个只读事务/同一DB clock核Run非terminal、原lease owner/generation仍current、当前lease未过期且未缩短到bound期限之前；错误tenant与unknown统一404 `model_call_bound_not_found`，stale fence/terminal为409 `model_call_bound_stale`，墙钟到期为410 `model_call_bound_expired`。专用secret缺/空或own dependency不可用为503 `agent_reader_unavailable`；credential缺失、scheme不为Bearer或值不匹配为401 `agent_reader_auth_failed`；credential已匹配但tenant断言缺失/空白/畸形，或携带试图覆盖身份的service/actor/subject/assertion/body字段，为403 `agent_reader_forbidden`。全部使用现安全error envelope且零业务写；deadline/取消有界、无自动重试、credential不进入error/log/metric label。当前route/config/schema仍未实现，不能先以shared bearer或header字符串通过contract测试冒充认证。

后继生成与消费顺序固定：可信capacity guarantee owner先正式发布并获Root准入 → Agent把exact guarantee identity纳入schema/vectors后生成manifest/parser/package provenance → Root固定commit/direct/source/archive digest并验producer/reader → Billing独立pin和strict consumer。usage/v1保持post-call actual evidence，不升版、不接受bound字段；System/Billing合同不由本仓复制。当前route/config/schema/guarantee均未实现，未运行contract/HTTP/PG/provider测试，缺任一前置仍0 bound/0 send。

**R255 owner 边界澄清（仅 D0 对齐，未实现 reader）。** 本节目标 bound 只证明技术 planned binding 与本次执行界，不声明 actual provider attribution，也不发布 Billing 独占的商业 pricing account；实际 provider 归属由后继 strict sealed evidence 证明。后继 `request_ref` 由 Agent authorization 状态机绑定同一 attempt 与已冻结的 refs/digests，并供 Billing Admission 幂等身份核对，不作为 bound 生成或 reader 的前置。当前 `PlannedBinding` 闭集没有 `provider_account_ref`；若需要该技术账户身份，必须先由具名技术 owner 以固定版本正式发布，不从名称、credential 或商业账户推导。本次只澄清既有 owner 边界，不改变 guarantee 缺失时 fail closed、reader 专用认证或 DB 时钟/lease 规则，不新增机器字段或实现。

## R252 包内合同实施状态（2026-10-05）

下方R245 required final_request_binding_version/digest与final hook已在当前R246 frozen21实现，本次从29a7a18a精确交付；算法/字段/版本没有改动。只有包内普通对象/数据库事实，没有发布execution-bound或authenticated immutable-read机器artifact，也没有Billing grant consumer。技术摘要不能替代可信token/exposure bound、实际provider attribution或费用授权；缺正式许可零send。验证和未验范围以CURRENT顶部R252为准，历史“未实现”仅保留时点。

## R245 最终 HTTP 请求绑定内部合同（2026-10-05；仅 D0，未实现/未发布）

正式基线 `cb64537048d34ce6db5bea5f2e12e182ba88092c`；当前只有 handler 前 prepare/无 Billing fail-closed，尚无 final-binding 字段或 mandatory final hook。本节与 TECH/DATA 同批设计前缀；不把下方历史验收或本次文字当新源码/系统证据。

### 唯一版本与摘要域

新增**包内** PrepareModelCall/PreparedModelCall required `final_request_binding_version=1`、`final_request_digest`（64 lowercase hex）；只由本次 final HTTPX hook 产生并由原 repository 首次冻结。唯一算法 `SHA256(b"kokoro-agent:final-http-request:v1\n" + RFC8785(E))`，E 精确字段为 `version=1`、`request_profile="chat-completions-materialized-json-v1"`、`method="POST"`、已验 exact `endpoint`、既有 canonical `planned_digest`、原 materialized body 的 `body_bytes` 与 `body_sha256`。body 不重新 canonicalize；不同 bytes 即漂移。不可改用 tuple-only/临时 gate hash；profile 不按请求切档。

RunRequest.input 对应的旧 `input_digest`、ExecutionInfo 派生 call/attempt UUID、planned/authorization/usage-v1 bytes含义不变。新摘要不进入 usage1/HTTP5/Redis/BFF或公开 endpoint，不自报 body/header 身份，不产生 Billing 可读取 ref、成功 input bound 或许可。仅存摘要时 owner 不宣称能从DB独立重建原body。

### 最终请求接纳 profile

只接收已 materialized `Request.content` 的 JSON bytes；未知stream/multipart/request压缩拒绝。Root固定 B 档：body8,388,608；容器深度64（根1）；所有JSON值节点+object key节点131,072；单array元素/object成员16,384；单string原始token4,194,304 bytes；key token4,096；number token128。定义/预检→strict UTF-8/stdlib parse→复核顺序见 TECH 唯一细节，不复制另一parser规则。重复 decoded key/BOM/非finite/孤立surrogate/额外文档/非object根拒绝，原model/cap/n检查仍mandatory。是工程接纳限制，不是token数量、上游完整接纳或SDK materialization前内存保证。

Header固定名单、值与method/target规则以 TECH R245 为准。必须包含锁定SDK的合法 `X-Stainless-Raw-Response` 缺省或 **true**、`X-Stainless-Helper-Method` 缺省或 **chat.completions.parse / chat.completions.stream**；不是任意 raw/stream或x-*。实际 with_raw_response 使用legacy wrapper true，parse/stream是精确helper标记，均不放松body/profile/mandatory gate。拒Cookie、组织/项目/路由账户override，Authorization不复制/摘要/记录；无新token读取接口。

### 内部生命周期与失败

middleware仅保存可信tuple/生命周期上下文并进入原SDK；首个同route final request claim可来自SDK gather child，不要求同owner Task。实际claimant在await前登记现ModelCallScope；唯一claim不因ContextVar继承而复制。最终hook先profile/摘要，再唯一prepare，然后无Billing typed unavailable，仍0transport；sync无正式授权一律拒绝。

同attempt新version/digest不一致是现 ModelCallIdentityConflict，相同逐值replay返旧receipt不更新，不授第二外发。格式/资源错误复用ModelCallInputInvalid；没有新增wire error。SDK包装仅由owner按本scope精确错误对象/直接cause恢复；取消优先，无关callback错误保持，记录denial不得被正常SDK返回升级成功。materialization前失败没有final prepared row，不补Run digest/空串。

middleware关闭只同步closed/reset/cancel独立claimant并传播；不在finally调用全scope.settle，不新增child等待API。现run_agent外层唯一settle在invoke_once返回/终态前收束；关闭后迟到child拒prepare，已commit/ACK未知不回删。并发claim/取消窗口待真实native与PG验证。

SDK-only正控迁测试侧真实SDK/peer机制helper，调用同纯 `model/request_profile.py` 与cap/observer逻辑；生产无缺scope fallback/allow_unbound/fake grant。原actual body/次数/usage/stream/取消/关闭断言保留，并独立维护正式constructor/native mandatory门，含普通ainvoke/astream和structured parse/stream的header真实正控。

本次仅文档。最终wire事实不代表可信token bound/expiry、authenticated bound read、Billing quoteTTL/grant/结算或真实provider成功；这些依赖保持OPEN，不修改其owner机器合同。

---

## R233 当前内部观察 API 已验边界（2026-10-04；无机器发布）

候选已实现 `capture_model_usage(route)`、`capture.mark_sdk_consumption_complete()`与冻结 `capture.snapshot()`。Root真实peer310验证合法完成、missing/null/explicit0、SDK合成total不冒原报告、四编码、marker/early-close/并发/unknown与资源关闭边界；完整工程、build、installed与独审收据集中于 CURRENT R233，尚待Root Git交付。下方“marker尚未实现/待设计准入”属于设计期历史。

本 API 仍仅包内单物理响应观察：projection_digest不是原HTTP bytes摘要或published raw_usage_digest，marker不是认证、grant或可结算声明。HTTP5/usage1/System3固定收据、公开schema与错误协议均未修改；没有新网络入口或财务完整性承诺。正式native caller、Billing许可、provider归属及sealed evidence producer仍 OPEN，本片不以接口存在代替资金授权。

---

## R228 usage 观察内部 API 候选（2026-10-04；R229回填待复审，无机器发布）

当前基线 `main@f3050fccc458dae1491cc5b9b98af79d9ff838ce` 已有System3.0严格消费与n1/output guard；下方待提交是历史。R228仅四docs前缀，HTTP5、usage/v1、execution-proof、System955固定收据、RunRequest和Billing contract原bytes冻结。下一切片只产生内部观察，不定义新HTTP/API/事件/schema，也不认为真实provider或财务链已完成。

### 包内 seam（Root已裁方向，待设计独审与实现验证）

新普通`model/usage_observation.py`提供 `capture_model_usage(route)` context manager及冻结的`PhysicalUsageObservation`；`with ... as capture`可包sync或async SDK调用，正常退出后`capture.snapshot()`取单物理响应结果。活动scope只给pending；未响应/多响应/route冲突/并发共享均unresolved，不选择last_usage。ContextVar作用域退出恢复原token；每request独立claim，后续response wrapper不查询可能已经切换的当前scope。新scope可并发独立，子task继承scope不意味着可以混用两个物理响应；SDK内部child Task的唯一同route首次claim合法。

结果是内部原始观察层：completion状态/闭集原因、source protocol、白名单计数字段presence与strict integer值、语义projection digest；不含tenant/payer/admission/金额/verified provider，不生成完整ModelUsageEvidence。成功响应model或gateway alias也不证明actual provider；无需修改现`PlannedBinding`或给它偷加metadata。内部presence区别missing/null/reported/invalid；只有显式整数0才reported0，bool不算integer，counts只接受非负且符合现usage1 Count上界。input/output/total齐全时关系必须相符；缺total保持未报告，不合成。cache_write没有本profile可信来源，继续unreported；已知subset必须≤其inclusive total。未验证service_tier或额外usage类别标unsupported，不默认为ordinary/free。

### R231显式完成seam（当前窄修待Root独审）

新增唯一内部方法 `capture.mark_sdk_consumption_complete()`，只能由scope owner在SDK invoke正常返回或stream正常耗尽后调用。正常scope退出仍必须有此标记；未标记（含提前break/close）为unresolved/sdk_consumption_incomplete，即使raw buffer已含final/DONE。异常/取消退出覆盖此前标记，不形成完成。

scope进入时记录marker owner线程、当前asyncio Task身份与ACTIVE token。marker要求活动scope、ACTIVE为本capture、调用者同scope-enter owner且已有唯一绑定response；wrong-owner、scope错配、尚无response或多claim/route/绑定冲突等结构误用以固定safe ValueError拒绝，活动capture永久misuse/ambiguous unresolved，原owner后续marker不得挽回。scope外迟到marker也报safe ValueError，但已冻结snapshot不变；同owner合法重复marker幂等。

物理request claim不要求Task等于scope-enter Task：锁定SDK内部继承ContextVar的child Task或to_thread中的唯一、同route首次claim合法，经request extension绑定唯一response；第二物理claim、route漂移或response绑定冲突永久ambiguous/unresolved，不窥探SDK私有Task身份。

输入/输出预算耗尽、unsupported content-type/encoding、坏压缩、JSON/SSE解析失败等观察unknown不是marker误用：SDK正常完成后合法owner标记不新增调用异常，保留原unknown reason且不升级complete。transport/SDK原异常继续传播，scope异常优先unresolved。

complete是正常scope退出、合法owner marker、唯一同route request/response与既定JSON完整body/validated或SSE final+DONE/压缩完整性规则的合取；raw DONE、HTTPX close、marker任一单独均不充分。无marker的正常scope与early break/close固定sdk_consumption_incomplete。parent仅在invoke/ainvoke正常返回或stream/astream自然耗尽后标记；SDK child Task及to_thread单调用由parent await正常返回后标记，继承child自身无marker权限。break、close/aclose、GeneratorExit、取消或异常均不标记。

该marker是可信包内调用责任，不是认证、grant或可结算声明；无自动完成fallback，不改HTTP/schema或call_runtime。Root终审后另授marker source/tests；当前marker尚未实现。

### JSON/SSE终态与观察透明性

JSON必须完整且合法，SSE候选必须为choices=[]的final usage事件并后续收到DONE；usage:null普通chunk不计数。完全相同的final语义投影重复只去重一次，冲突永久invalid；不相加final_cumulative。缺final/DONE、提前close、取消/断流、scope异常退出为unresolved，既有SDK异常原样传出。正常DONE是协议终点，不为socket EOF强行预读；scope正常结束才取冻结完成结果。详细边界与预算见TECHNICAL_DESIGN同节。

response hook的公开HTTPX raw tap仅将SDK已拉取bytes原样转发并复制到observer≤4MiB buffer；不在线decode，不用SDK stream作reader输入。scope正常末冻结同immutable compressed buffer再观察；输入超限停止复制/释放，SDK继续。无scope维持原行为；scope异常、取消、transport/close异常原样传播且观察unresolved，正常读取/backpressure/ownedclose不变，不预取SDK。

Root唯一zstd路线为既有锁定zstandard0.25.0公开`stream_reader`读取自有BytesIO第一遍（read_across_frames=True，每read≤min(64KiB, remaining+1)，全decoded≤4MiB）→全owned EOF及parse成功生成模块私有`FIRST_PASS_BOUNDED_COMPLETE`→释放reader/window/parser scratch→同immutable buffer、同window/backend的公开`decompressobj(read_across_frames=False)`逐frame第二遍。第二遍仅核eof/checksum/unused_data、最多4096frame、无进展/垃圾拒绝；不重新解析，不留各frame输出。禁止外部布尔伪proof、变更buffer、失败/提前终止第一遍后二次decode；decompressobj自身无输出参数界，只有此前同buffer全量有界证明才允许调用。

Root实测当前C backend `max_window_size`必须传4194304，非4096；官方KiB措辞矛盾保留，不跨minor/backend推广。无compression.zstd/在线writer/fallback/新依赖；Python>=3.11不变，3.11–3.13 installed/peer及backend单位复验仍未执行。gzip/deflate同scope-end使用公开zlib有界解码与完整性检查，identity直接有界解析；四编码首片覆盖，brotli/未知/坏编码unresolved且SDK原行为不变，不改Accept-Encoding/private decoder。正式native无Billing仍0provider。

内部观察明确区分`compression_integrity=validated`与`unverified_unconsumed_tail`，其余未完成/错误仍unresolved/pending，不进入完整报告。JSON需完整document且validated。SSE正常scope+final+DONE只完成逻辑usage；自有前缀只缺压缩trailer、无已知decode/checksum错误且SDK正常因DONE停读时记unverified_unconsumed_tail，不补读SDK或冒整流认证；已消费prefix坏编码仍unresolved。全部自有frames校验完成且无garbage才validated。该字段是包内completion/reason，不发布新机器合同，不将projection_digest升级为raw_usage_digest。

### digest材料与隐私（内部，不是raw HTTP摘要）

拟固定domain为 `kokoro-agent:usage-observation-projection:v1\n`，SHA256输入为该domain字节加RFC8785规范化**白名单观察投影**。投影只含protocol、usage对象presence、prompt/completion/total及cache-read/reasoning等已选计数的missing/null/reported值标记、service-tier的闭集适用判定、未知类别存在标志；不含模型输出、prompt/tool参数、headers、token、cookie、任意上游字符串或request ID。合法integer规范化为十进制字符串，保missing与null区别；对象键序/空白不影响digest。invalid语法/超限未取得完整合法投影时digest=null，不把partial材料冒完整证明。

投影拟采用一个闭集内部值（不是新wire schema）：`{version:1, protocol:"openai_chat_completions", usage_presence:"missing"|"null"|"object", counts:{prompt_tokens,completion_tokens,total_tokens,cached_tokens,reasoning_tokens}, service_tier:"absent"|"null"|"default"|"unsupported", unsupported_usage_category:boolean}`。每个count固定为`{presence:"missing"|"null"}`或`{presence:"reported",value:"<canonical decimal>"}`；details容器missing/null作为对应child missing，不宣称容器原文保真。错误count形状/类型不进入canonical投影，digest=null；未知成员仅投影为存在标志，不保存其名称/值。只对白名单观察语义承诺稳定摘要，不承诺还原整个usage对象。protocol是解析方言，不是provider身份或收费profile认证。

这是明确的 **projection_digest**，不是原HTTPbytes/完整provider receipt/原usage对象byte digest；首片不写入published `raw_usage_digest`，也不从SDK归一metadata逆向造原响应。后继真正证据组装若要映射该字段，必须单独核现owner合同语义，不在本片暗改。原始content只存在消费者收到的网络chunk、observer有界compressed buffer及阶段内解析scratch，处理后丢弃，观察结果及日志只保上述投影或安全reason。

Root预算：输入buffer4MiB、decoded累计4MiB、zstd window4MiB独立；JSON document4MiB/SSE事件64KiB、4096事件/4096frame、depth16/4KiB投影/单响应scope。parser遇DONE也要有界drain全部自有buffer完成第一遍证明，但不再采纳后续usage、不读SDK新bytes。预算耗尽unknown；decoder/scratch按阶段释放，snapshot只留白名单投影/安全状态。

允许短时buffer/document/frame含当前内容，不日志/持久化/结果保留，scope正常/异常/取消/超限均释放。受控payload估算JSON首遍12MiB+68KiB、SSE首遍8MiB+132KiB、次遍12MiB；未证零拷贝冻结另加4MiB，BytesIO/unused_data复制与对象/native开销另计。非RSS或token/财务界，具体峰值/阶段释放由系统门验证。

### 验证与OPEN

现`tests/integration/test_model_provider_retries.py`先真实SDK/HTTP characterization，再capture oracle；无未来import假RED，不以Mock/构造参数代wire。原147 peer、n1/output guard、ownedclose与native缺Billing拒绝必须保持。原usage1机器/Schema门用于证明未漂移，不证明新观察已实现；本轮未运行任何门。Root审完本D0后另授test/source，输入可信界、technical-bound/read、真实Admission/expiry/dispatch、actual attribution和财务封存/ACK仍OPEN。

---

## R226 当前：单 completion SDK执行限制，无公开契约变化（2026-10-04）

Agent基线已为 `main@d50bde5f5b19dc89f6a56640c9e464174dc52ab4`；本两文件候选已通过Root147真实peer、451邻接与2253默认门、静态/contract/隔离build及独审0P0/P1/P2，待Root提交，完整证据与未执行资源范围见CURRENT。四文档回填后Root另跑build，本段不预报。

正式LiteLLM SDK请求显式发 `n=1`，最终wire hook仅接受strict整数1；缺失或非法值在外发前以原safe bound error拒绝。原endpoint/model/output限制不变，不扩cap、不重写调用方override。这里变的是内部SDK最终请求约束，Agent公开HTTP5、usage/v1、System3.0固定收据、execution-proof和资金契约均未修改，也未发布technical-bound/read或Billing许可。输入有限上界、真实外部provider与UI整链保持OPEN；本peer证据不冒正式收费调用。

---

## R216-AGENT-EXECUTION-D0：System 3.0 固定消费者边界（2026-10-04；候选已验待Root提交）

本节对应 Root `docs/task.md::R216-AGENT-EXECUTION-D0` 与本仓 TECHNICAL_DESIGN 顶部当前方案。Agent 基线 `92b802e223547288c4a8dcaba56dfd2f0c327f93`；候选 consumer pin 已原子更新为下列3.0.0固定收据；限定消费者与安装门已验，但尚未提交/部署3.0。本片不修改 Agent HTTP5、usage/v1、RunRequest、AG-UI、execution-proof、Billing或canonical SQL契约。

### 唯一 owner artifact 与来源

| 项 | 固定目标 |
| --- | --- |
| Owner / visibility / operation | `kokoro-system` / `internal-owner` / `POST /v1/system/model-catalog/resolve`，HTTP路径仍v1 |
| Owner本地已提交commit | `955bd587f46a771e43a4330d202d90a7d2d67262`；未push，不宣称远端可取得 |
| Artifact版本/路径 | `3.0.0` / System `contract/openapi/system.openapi.json`（runtime-Zod只读生成物） |
| Artifact SHA256 | `94ed91655741cbba66ce80a4c41717b9179cc509cb6858534e0b78aa3e6dc059` |
| Provenance路径/SHA256 | System `contract/provenance.json` / `5fda620ca1526f57d8759b2c6282ed6d674455a78e8ff9ee7fc0522e8f5a3832` |
| source digest集合 | 上述固定provenance的 `openapi.sourceSha256` 全19项，必须逐项对该commit原bytes校验，不只挑本片文件；例如resolve schema `31b6d275c9187b3592ff402b3b3bef3da3b83629ffe53896050a8129310b1b74`、revision schema `5d8559c0d4bac7e88a3e54d753c30e87c0f070be05dae813a401afe2b1a5274b` |
| 发布前置 | Root已验owner HTTP/PG/Redis/fresh并本地提交；Agent限定消费工程/真实peer/PG/安装门已验；Root提交、固定组合分发与远端发布仍待执行，不用provenance中的历史consumer disposition冒充当前Agent3.0证据 |

D0基线 Agent receipt为2.0；R219候选已换下述唯一完整3.0收据；`clients/system.py`是手写窄client，不存在System Python consumer generator，也没有vendored System可编辑schema。现 `kokoro-agent-contract-check` 验Agent-owned/Platform/usage等事实，R219已接具名System receipt validator。候选采用**现provenance内精确收据＋包内checker＋Root Git-object来源门**，不新增vendored资产/第二schema。包内checker验证固定收据，Root独立验证owner原件，两门均通过才可放行；runtime不读取邻仓checkout或网络原件。精确写集见TECHNICAL_DESIGN；当前候选已通过Root机器/资源/installed门；本R221仅回填文档，Root提交与3310启用未完成。

### R217窄修：精确收据与两道独立机器门

唯一持久位置是现 `contract/provenance.json::consumer_dependencies` 中唯一 `owner="kokoro-system"` 元素。该元素精确如下；顶层恰8 keys、provenance恰path/sha256、source_sha256恰19个路径、operations恰一个操作，禁止额外/缺失key、重复owner/operation/source路径/JSON member及类型coercion。候选已整体替换旧2.0元素，不并列两套收据：

```json
{
  "owner": "kokoro-system",
  "contract": "contract/openapi/system.openapi.json",
  "version": "3.0.0",
  "source_commit": "955bd587f46a771e43a4330d202d90a7d2d67262",
  "sha256": "94ed91655741cbba66ce80a4c41717b9179cc509cb6858534e0b78aa3e6dc059",
  "provenance": {
    "path": "contract/provenance.json",
    "sha256": "5fda620ca1526f57d8759b2c6282ed6d674455a78e8ff9ee7fc0522e8f5a3832"
  },
  "source_sha256": {
    "scripts/generate-system-openapi.ts": "4830a6c594255984261efe2907c17cd4e63df4b28d10c2ef1cf65c8e61287890",
    "scripts/system-openapi-operations.ts": "fb46bea2ec53a09c0f397d2f38aafc77a1c6e7eaef904b11667d56fda919d9f3",
    "src/http/protocol.schema.ts": "72dbe6ee57d842b855b337d1f1603a7ddbb8c3a4bb6c4f7bf3fe62821c8160c0",
    "src/modules/model-catalog/schemas/definition.schema.ts": "e2063b9ae62b8e5c4ca7cccce09dd4874f818831fc8966de2cf6d015a99bef64",
    "src/modules/model-catalog/schemas/label.schema.ts": "96116b4d390e203be056f73e32a30ea93e40dc0c60966ce60579cdb949f31a6a",
    "src/modules/model-catalog/schemas/provider.schema.ts": "f33d4aaa5bd34ff7008712ecda8f27c0c8769830652ada7df60f184e3961c57c",
    "src/modules/model-catalog/schemas/resolve.schema.ts": "31b6d275c9187b3592ff402b3b3bef3da3b83629ffe53896050a8129310b1b74",
    "src/modules/model-catalog/schemas/revision.schema.ts": "5d8559c0d4bac7e88a3e54d753c30e87c0f070be05dae813a401afe2b1a5274b",
    "src/modules/products/products.public.ts": "ccea3298f5ca5549d7882ce056e8bbb3445d23a9e1703958bd4b0a220882983e",
    "src/modules/products/schemas/application.schema.ts": "087d997360add9b9bbb2e244c1b1f9864fc6cf4ba93a7c9e8c14767288ca6a57",
    "src/modules/products/schemas/config.schema.ts": "92324f19fd4e39fdf2cef0a4a28f9e43f8b70c09fd5290e8153035a5a2aa2060",
    "src/modules/products/schemas/feature.schema.ts": "5222976ca8ccba5669e868386132dca6489d44c26f9a23526aaaeb0d20147014",
    "src/modules/products/schemas/presentation.schema.ts": "5cd491fe9788de6fb8e7d575fd303b795251e7f682e89c10506e6ee7601174b5",
    "src/modules/products/schemas/product.schema.ts": "90c57e7ecfa71346698ed7f399c6e31a5258451d4eea16023d2c011b4986beb6",
    "src/modules/products/schemas/release.schema.ts": "2d21d41ebb4b0a0c8b33dde26783052f1142eaf8f44ab0027e350cb1b6e2a669",
    "src/modules/runtime-manifests/schemas/manifest.schema.ts": "8b73a78e98c64174494398bc68c69061a408f53a9e81bf19d5e970d1ef828553",
    "src/modules/sites/schemas/policy.schema.ts": "b9aa0e7d70d9212a668b7c88133fb337acc8248f956240bbf0afe60d12eff7e9",
    "src/modules/sites/schemas/site.schema.ts": "98031e74e4badb28d519f9f42e4c29894aa2a7c4b9388b0756764abc9e7b3636",
    "src/modules/workspaces/schemas/workspace.schema.ts": "a52e63e9fc798eb25b2003830ad4fcebb7ce9edf0a06b90a5f3b72ad752bc16d"
  },
  "operations": [
    "POST /v1/system/model-catalog/resolve"
  ]
}
```

**Agent包内门。** 对原始provenance JSON使用拒绝重复member的读取；从consumer_dependencies提取唯一System元素，以RFC8785 canonical bytes核固定收据SHA256 **`aaf1428aa898592fb370021790d5e55cb3dd43b44080696dd2ec99f91904408b`**，同时校验上述key/type/count闭集。它固定全部19项而不在另一个资产复制可编辑schema；更改任一field/hash/path/operation、重排array、删加source、重复元素均失败。对象member顺序不影响canonical值，不强制文档pretty-print。这个hash只证明包内固定receipt，**不证明owner Git原件**。source-tree与installed均走现 `kokoro-agent-contract-check -> contract_check.main -> distribution_assets.contract_audit_root -> validate`；现 `share/kokoro-agent/audit/contract/provenance.json` 已是data-files，正常wheel RECORD绑定与audit closure验证机制保持；闭集仅追加下述validator Python原件/audit副本，不新增System安装资产。

现 `contract_check.py` 保持199行且硬门<200，保持编排职责，不放宽阈值或机械压行。**采用同package新增普通 `src/kokoro_agent/system_consumer_contract.py`**，唯一公开函数 `validate_system_consumer_receipt(root: Path) -> None` 读取现provenance原bytes、拒绝duplicate并验证上述闭集与固定canonical hash；仅标准库（此全ASCII字符串/对象/数组收据的sorted compact JSON等于RFC8785），无网络/Git/SDK/SQL/import副作用。**淘汰复用 `execution_proof_contract.py`**：它已809行，Agent-owned proof与第三方System receipt是不同变化原因。不是新目录、一级业务模块、资产或schema。`contract_check.py::validate`显式调用具名validator，现reader仅局部导入别名read_document使HTTP provenance调用自然单行保持<200，不移动业务或放宽架构门。新普通module同时加入现 `execution_proof_contract.py::OWNER_SOURCE_FILES`（仅inventory项，不放新校验逻辑）、provenance.source_files及现pyproject的 `share/kokoro-agent/audit/src/kokoro_agent` data-files；现distribution_assets自动核精确closure、audit副本与installed Python原件及wheel RECORD。现source aggregate经 `uv run python scripts/generate_failure_models.py` 正规刷新，生成的Failure字节实际不变；该命令不是System生成器。

**Root来源门。** 以下是可复现只读命令，最终owner原件已由Root独立固定对象实核；以下Agent候选来源门也已由Root执行通过；Agent收据从候选文件读，System所有原件只用固定commit的 `git show`，绝不哈希浮动工作树。R219候选pin已3.0，Root已独立执行固定原件门exit0；全包与installed门亦已独立通过；来源门自身仍不替代消费行为证据。Root记录stdout/exit、Agent候选SHA与固定commit后，再放行组合分发：

```bash
python3 - <<'PY_GATE'
import hashlib, json, subprocess
from pathlib import Path
root = Path('/Users/nako/WebstormProjects/github/thefoxfairy/Kokoro')
commit = '955bd587f46a771e43a4330d202d90a7d2d67262'
def strict(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON member')
        result[key] = value
    return result
def decode(raw):
    return json.loads(raw, object_pairs_hook=strict)
def original(path):
    return subprocess.check_output(['git', '-C', str(root / 'apps/kokoro-system'), 'show', commit + ':' + path])
def digest(raw):
    return hashlib.sha256(raw).hexdigest()
manifest = decode((root / 'apps/kokoro-agent/contract/provenance.json').read_bytes())
receipts = [r for r in manifest['consumer_dependencies'] if r['owner'] == 'kokoro-system']
assert len(receipts) == 1
r = receipts[0]
# This receipt contains only ASCII string keys/values, objects and arrays;
# sorted compact JSON therefore equals its RFC8785 bytes.
canonical = json.dumps(r, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
assert digest(canonical) == 'aaf1428aa898592fb370021790d5e55cb3dd43b44080696dd2ec99f91904408b'
assert r['source_commit'] == commit
artifact = original(r['contract'])
provenance = original(r['provenance']['path'])
assert digest(artifact) == r['sha256']
assert digest(provenance) == r['provenance']['sha256']
a, p = decode(artifact), decode(provenance)
assert a['info']['version'] == r['version'] == '3.0.0'
assert p['openapi']['sha256'] == r['sha256']
assert 'contract/' + p['openapi']['file'] == r['contract']
assert p['openapi']['sourceSha256'] == r['source_sha256']
assert len(r['source_sha256']) == 19
for path, expected in r['source_sha256'].items():
    assert digest(original(path)) == expected, path
assert r['operations'] == ['POST /v1/system/model-catalog/resolve']
assert 'post' in a['paths']['/v1/system/model-catalog/resolve']
print('System fixed Git-object source gate: artifact + provenance + 19 sources PASS', commit)
PY_GATE
```

**installed门已验（Root独立执行）。** `/tmp/kokoro-r219-agent-installed-root-v2.json` 记录18步骤通过：从固定锁导出116个hash-locked运行依赖，独立venv安装wheel、检查依赖及隔离import origin，仓外 `kokoro-agent-contract-check`、`kokoro-agent inspect --json` 均exit0。两层负控分别把已安装audit/provenance内System version改为2.0：第一次保持原RECORD，checker因完整性不符exit1；第二次同步更新对应RECORD的hash/size，使安装记录自洽，checker仍因fixed receipt语义不符exit1。两次均为预期拒绝，不是门失败；恢复原bytes及RECORD后checker exit0。受保护392文件不变、自有venv已删除，未读取浮动邻仓作为运行依赖。

source public checker矩阵另覆盖缺/重复/extra key、artifact/provenance/source hash/path/type、18/20 sources与操作闭集等；这些source级测试不冒充每一种已安装包篡改均逐一运行。当前安装实证是上述两层version降级及恢复，未来新增资产仍须完整closure/RECORD门。新validator原件和audit副本沿现data-files/RECORD登记，无vendored System资产或第二schema。

**上游元数据债已关闭（历史处置）。** 旧commit的Agent disposition/description失实措辞已由上述最终metadata-only commit修正，Root已完整205/16contract/fresh23/22及独审归零。本前缀receipt、source集合与Root命令已整体重pin，不沿用旧hash。Agent候选已pin3.0并通过限定门、待Root提交，未push与3310运行实例均不冒消费3.0；未来再修owner bytes仍须整体重新固定并重跑两门。

### 消费语义，不复写 owner schema

上表OpenAPI artifact定义两字段、required及各自正int32范围；跨字段 `max_output_tokens <= context_window` 由同commit的runtime-Zod `src/modules/model-catalog/schemas/resolve.schema.ts` refine定义、经provenance固定，当前OpenAPI未表达该关系。Agent在自身不可信HTTP边界重复fail-closed校验，不能仅凭OpenAPI生成推断关系；Agent `_Route -> ResolvedModel` 必须strict解码 `context_window`、`max_output_tokens`，JSON整数范围 `1..2147483647` 且 `max_output_tokens <= context_window`，拒绝bool、float token、字符串、null、缺失和额外字段；旧2.0合法响应在新消费者中必须拒绝，禁止optional/default/fallback。保留同response的model/provider/revision/digest/generation/tenant_generation/feature/label绑定和现transport=`litellm`。未通过解析映射现 `MODEL_RESPONSE_INVALID`；不新增公开错误码、自动重试或第二resolve入口。

界沿AgentFactory现正式caller显式下传到SDK；context window不是输出token字段、账户余额或Run预算，output上限不是计费单价/授权金额。System published revision的immutable/digest/generation/checksum语义由System负责，Agent只固定消费，不复制修改规则。价格、费率、Admission与结算只归Billing。

执行bound目前未发布为HTTP/usage字段：仅后继包内逻辑在逐调用gate由当前有效Run lease与真实Billing授权构成，最早expiry控制本次外发。System health/缓存TTL不授执行许可；当前未有Billing consumer，正式native路径仍prepare/not_requested后现safe failure并0 provider。isolated SDK→loopback的wire正控不调用正式Run，也不代表收费准入完成；不fake grant、known-zero、免费降级或绕过middleware。

### 可执行契约核验与证据边界

- Owner工作目录的既有命令：`pnpm contract:generate` 仅后继需重新生成时由owner运行；只读漂移门 `pnpm verify:contract-provenance`、`pnpm contract:check`。本D0未运行任何owner生成/测试，也未改其文件。
- Agent现命令：`uv run kokoro-agent-contract-check`、`uv run pytest -q tests/contract/test_machine_contract.py tests/unit/model/test_system_client.py`；System dependency pin新增断言须先RED再GREEN。Root现已运行相关source门通过；targeted394含90真实peer，默认2253及contract/build均通过，证据不只来自文档或unit。
- R218最终来源由Root从固定commit的Git原bytes核artifact/provenance及19sources；本writer核对Root固定receipt并同步本前缀，不读取浮动邻仓。Root已对候选pin固定原件门得到exit0；后续任何相关bytes变化仍重跑，不以该快照代完整GREEN。SDK固定版本及Root真实HTTP/PG终态见TECHNICAL_DESIGN；正式Billing reader/Admission、exact input token、真实provider、GP/catalog/Swarm恢复、UI与3310启用仍OPEN。

---

## R203-A4-SCOPE：包内有限settle与取消审计边界（2026-10-04；无机器契约变化）

本节取代下方R198取消段的旧“实现门仍阻断”状态；旧段仅为历史快照。

本片不新增HTTP/RPC/event/generated、RunErrorCode或usage/v1字段。每次`invoke_once`包内绑定唯一`ModelCallScope`，并把`astream_events`获取、公开run context、pump与interrupt读取放入一个同步登记的显式native driver task；middleware在repository/provider之前登记真实model-node task。caller以shield等待driver，首次取消立即启动同一固定deadline来cancel/join全部exact owned tasks。正常、普通异常、`asyncio.CancelledError`均须先有限join，再允许native-settled、usage seal、pause/reconcile或terminal；caller取消仍不映射`RunFailed`或用户cancel。

唯一配置为正有限`model_call_settle_timeout_s`，默认5.0秒；production显式从AppConfig传入。deadline耗尽抛包内`ModelCallSettlementTimeout(BaseException)`，只含安全计数/phase。它绕过普通`Exception`映射；Supervisor先stop-accept，再调用NoReturn process-fatal执行`os._exit(70)`，callback违规返回也必须立即exit70兜底。该出口不走正常handle/sandbox cleanup、不释放lease、不生成terminal/evidence/费用结论；普通SIGTERM仍使用原graceful drain。

显式driver、固定deadline及完整`serve→stop-intake→exit70`链已实现。Root43772在最终22路径上验证真实fresh PostgreSQL 306项、fatal/非法callback/production SIGTERM系统子进程3项、默认2192项（另421项资源deselected）以及全部静态、contract与build门；独审0 P0/P1/P2。唯一严格`__error__="CancelledError()"` pending audit不构成新wire、usage或费用事实。artifact发布与既有3310进程消费、Billing admission/dispatch/provider成功/evidence ACK及收费链仍OPEN。

---

## R198-A4：包内identity与现有安全失败映射（2026-10-04；最低main caller已验，机器契约不变）

A4不新增HTTP/RPC/event/generated或usage/v1字段。logical call仅为包内执行身份：RFC8785 locator绑定locked tenant/run、显式peer path及公开`ExecutionInfo(checkpoint_id, checkpoint_ns, task_id)`；根`checkpoint_ns=""`合法。固定namespace `a06d00e2-defa-5fe3-8f2c-10ecb19c1161`派生call UUID，固定namespace `64675e75-8af6-50b4-ade3-d413e5144dc9`以`{call_id}:1`派生首attempt UUID。node_attempt/metadata/time不入identity。

显式peer path来自同一frozen recipe而非wire或runtime metadata：Swarm peer使用entry出发的排序BFS最短路径（同长完整path字典序）；合法cycle/多入边仍有稳定结果。每peer的内生`general-purpose`以`source="built-in"`作为recipe显式child且保持工具继承，catalog child沿现spec；child path追加其name。该规则不新增公开字段，也不把静态path称为实际handoff轨迹。

usage/v1现`input_digest`仍精确表示locked `RunRequest.input` canonical SHA-256，不表示实际model invocation摘要；本片没有发布后者。execution info缺失/非法使用现`contract_incompatible`。Billing consumer缺席时，包内typed `ModelCallAuthorizationUnavailable`只映射已发布Run failure `dependency_unavailable`且`retryable=false`；不新增`model_authorization_unavailable` wire code。

有合法identity时先留下`prepared/not_requested`，随后失败封闭且零handler/provider。现`invoke_once`把typed native异常转成单一RunFailed outcome，现terminal closure把该attempt封存为`not_dispatched + unresolved/missing_usage + actual unknown/not_observed`；这不等于known-zero、Billing denial或费用结清。正式Billing begin/grant/dispatch、evidence read/ACK仍OPEN。

Root54105已以真fresh PostgreSQL/native loopback验证这一main负路径：0 provider、1 prepared/not_requested、safe failed及终态ND封存。Root50248又完成database 304、default 2182与contract/build全门；独审0 P0/P1。这些证据未发布新wire，也未覆盖GP/catalog/Swarm的真实caller/recovery、实际model invocation digest、Billing准入/dispatch、provider成功或收费。16个显式隔离的旧非费用unit case不作为model gate证据。

取消收束仍无机器契约变化。`asyncio.CancelledError`保持包内执行取消并原样向worker传播，不映射`RunFailed`、`dependency_unavailable`或用户cancel，不调用terminal finalizer。scope不进入Runnable metadata/config/checkpoint、HTTP/event或usage/v1；done callback必须立即且仅一次取回task结果，非取消异常留在scope私有队列并由同一个owned cleanup task消费后给首次取消加note。当前SDK没有公开有限`wait_closed`终止保证，因此实现门保持阻断；超时返回后遗留task不构成合法取消合同。

---

## R196-A4-D0b：原生model-call hook与retry语义（2026-10-04；机器契约不变）

A4唯一package-internal拦截形态为LangChain `AgentMiddleware.awrap_model_call`：handler内部才执行`bind_tools/bind→ainvoke`，所以middleware可在真实模型调用前完成持久gate，并在结果/异常/取消后记录strict evidence。它不是HTTP/RPC/event，不改变usage/v1或Billing机器契约。main/swarm peer的Run/lease/System route/peer key来自构造闭包；declarative subagent path按受信spec显式装配，不从metadata或`runtime.context`推断。Compiled/remote subagent当前未启用，不在覆盖声明内。

锁定OpenAI/Anthropic SDK默认隐藏retry为2；R196-A4-Retry现已将五个实际provider构造显式设`max_retries=0`：一次dispatch permit至多一次SDK HTTP attempt。Root本地HTTP sandbox 14项与完整工程门通过；500、429、断开或取消不得在同attempt内自动重发，上层未来只有创建新attempt、重新取得Billing admission并赢得新dispatch permit后才能显式retry。本片不新增provider/Billing wire、错误码或client；本地HTTP sandbox不是真实模型/收费闭环，stream错误矩阵仍非穷举。

失败/取消仍仅结算已核实actual并释放其余预占；unknown不补零。callback error中的partial usage只有经后继attempt-scoped可信collector验证后才能入strict evidence；全Run usage callback仍只属预算。

---

## R192-AGENT-A2：仅内部 operation，无机器契约变化（2026-10-04）

当前内部片已由Root完整database 281项、完整工程2178项及独立审查0 P0/P1限定验证。已实现的`prepare/begin_authorization/grant_authorization/start_dispatch`是Agent进程内不可变 command/receipt 与 repository Protocol，不是HTTP、RPC、event或Billing wire。Root隔离真实PostgreSQL 70项通过证明内部authority、canonical observation、一次性dispatch permit与只读重放；它不发布新endpoint、错误码、schema版本、generated client或跨仓ACK。

唯一机器事实仍是`contract/usage/v1` 1.0.0。Agent只冻结其authorization observation中的opaque request/admission refs、canonical bytes及其SHA-256摘要，不复制Billing request/amount/quote/settlement DTO。`newly_started=true`仅授权当前caller执行一次后继provider外发；false表示不得再次外发，不证明先前外发成功或失败。Billing experimental machine contract尚无Agent runtime consumer/read path/ACK接线，本片不得臆造第二协议。

待实现边界：runtime/provider、finalizer、cancel/failed终态封存、Billing evidence读取和ACK、actual-only结算及unknown reconciliation。失败或取消只结算已核实实际消耗、释放其余预占属于Billing owner；Agent unknown必须持久保留，不能补零。本节不改变现OpenAPI、RunRequest、AG-UI或机器artifact bytes；纯artifact职责搬移已按限定内部片验证，且无机器契约版本变化。

### R193-A3a当前态：内部append已实现，无wire变化

A3a的append command/receipt与cursor primitive现已实现且仍全部是Agent包内接口，不是HTTP/RPC/event/Billing API，不复制usage/v1机器字段，也未升级任何contract version。调用方提交完整owner usage/v1 canonical artifact；repository严格验证后只返回内部持久receipt。Root真实PostgreSQL定点81项与完整database 292项通过；完整工程门自然exit 0（2178 passed、390资源deselected，lock/frozen-sync/Ruff 280 files/Pyright/contract/wheel/sdist build均0）。

普通worker只在Run active且lease fence有效时追加首revision或contiguous monotonic successor；active `unknown/granted`允许合法unknown→known/verified usage增补。Run terminal后新revision一律拒绝，exact readback只恢复已提交同event/revision/bytes/digest，绝不授予provider重发或terminal写权限。Billing evidence读取/ACK及收费仍没有本片契约。

包内目标签名沿现immutable command惯例：`append_evidence(AppendModelCallEvidence) -> ModelCallEvidenceReceipt`。command显式携带受信`tenant_id/run_id/attempt_id/lease`、`attempt_binding_digest/planned_digest/authorization_digest`与完整`evidence_bytes`；event/revision/digest/observed time只从strict artifact解析，不接受raw之外第二套caller自报授权字段。receipt仅绑定tenant/run/attempt/event/revision/digest、原`observed_at_ms`与`replayed`，不包含`newly_started`或任何provider外发许可。此处仅定义包内目标，不复写generated usage机器字段。

append只接受`seal=true`、revision在正int64范围的artifact。repository在Run锁后以受信DB时刻验证`prepared_at <= observed_at <= db_now`，有dispatch时还要求`dispatch_started_at <= observed_at`；输入观测时间成功后原样持久，exact replay返回原`observed_at_ms`，绝不按重放时钟刷新。

## R195-A3b当前实现：终态closure仍为包内事务，无wire变化

A3b不新增或修改HTTP、RPC、event、generated、usage/v1或Billing契约。它只在现`finalize_terminal`事务内从冻结Attempt事实生成合法strict closure：未dispatch的prepared/pending/authorized保存`not_dispatched`与unresolved missing usage；已dispatch但无观察保存`unknown`与unresolved incomplete response。exact terminal retry验证全部heads并读取原event/time/bytes且零写，不提供provider重发、post-terminal新证据或收费许可。既有terminal `outcome.usage`/预算累计不成为strict Evidence或费用事实。当前包内实现已由Root全真实PG303及完整默认工程2178/contract/build限定验收；不发布或开放Billing/provider caller能力。

## 历史基线：R188-AGENT-A1-SCHEMA内部数据合同（2026-10-04）

> 以下“当前/尚无”均为R188/R189当时快照；R192现状以上方首节为准。

> R189内部prepare候选不新增HTTP/跨仓contract。`PreparedModelCall`只是immutable creation receipt：`initial_status=prepared`/`initial_authorization_state=not_requested`是创建事实，不是current-state声明；合法推进后exact replay仅核不变binding并零写，不比较或回滚当前authorization/status/latest。

当前canonical PostgreSQL数据合同已通过Root真实PG定点58项；prepare repository行为56项及完整database 267项通过，但没有runtime producer。完整工程门当前2174通过、1个architecture失败、365 deselected，失败为`typing.cast`逃逸且pytest失败后build未执行；当前候选已改为递归`JsonValue`校验，待Root完整复验。A1不修改HTTP5/OpenAPI、RunRequest、BFF/AG-UI或Billing operation，不新增跨仓RPC。Billing v2 2.0.2仍是未部署experimental Run/invocation target，Agent/BFF无consumer pin；A1不复制其request/response或generic service receipt。

唯一机器语义来自已发布`contract/usage/v1` 1.0.0：小写UUID event/call/attempt；正ordinal/revision/lease generation；revision1无predecessor、后继精确引用前event/digest；同identity+revision+bytes+digest只读幂等，漂移冲突。authorization闭集`not_requested|requested_unknown|denied|granted`；只有granted的authorized attempt可dispatch。execution闭集`not_dispatched|completed|failed|cancelled|unknown`；usage为normalized或unresolved，totals和classification各自保留unknown。

A1状态映射以TECH真值表为唯一内部合同。特别是：prepared/pending不得dispatch；denied在同事务写sealed not_dispatched；unknown是sealed事实；unsealed扫描只含prepared/pending/authorized/dispatch_started。evidence同表投影遵循已发布codec：not_dispatched不接受normalized，known totals只配partial，totals unknown只配classification unknown；后一合法组合仍使Run unknown。全sealed not_dispatched表示“已证明零外发”，Run completeness可complete，但不生成provider reported zero。

finalizer closure严格复用冻结attempt字段并经现codec产生合法artifact。首次事务内随机event UUID+DB毫秒observed_at；无派生UUID算法。unknown commit后按`(attempt_id,revision)`读取已提交原bytes/event/time/digest，只有不存在才新建。not_dispatched使用unresolved/missing_usage，started unknown使用unresolved/incomplete_response；actual unknown/not_observed；raw digest只保留已观察值。缺冻结事实或codec失败使Run终态全回滚。

repository内部结果闭集`created|replayed|conflict|authority_lost|terminal`，不直接成为HTTP error。Run completeness内部投影不进入公开payload：none/complete/partial/unknown按TECH逐attempt真值计算。终态后普通lease callerauthority_lost；reconciler另设计。A1没有delivery ACK/outbox、Billing receipt、结算状态或金额。

---


## R122 契约当前态（2026-10-02）

当前为Agent main `2653bcc723da5366fd877db73d701410e7bdc3a8` 之上的完整HTTP5/Todo、安全过程、安装资源与生命周期候选，待Root提交/发布；下方R104提案与R117 RED均为历史阶段，不覆盖本节。事实owner/模块、唯一canonical SQL与机器契约不变；distribution `2.0.0`、HTTP `5.0.0`、execution proof `1.0.0` 是不同身份。当前OpenAPI SHA256 `bca8e4f4fd613e4325f594266893d5b089168cf14f2ad7a7df03f3f116af85f2`；跨仓消费者必须随后固定正式owner commit/digest，不靠未发布源码或兼容协议。

Root E95实际1960纯节点全部通过/零skip、七静态门通过；E96当前源码新wheel/sdist与完整runtime安装、四布局64负向/219步骤通过；E97同包installed CLI真实PG首次安装/重复拒绝、canonical catalog与六漂移回滚通过；E98同包仓外安装态现HTTP acceptance实际36通过/0失败/0跳过（36 setup/call/teardown），151 loaded模块在collection/finish对应本次site-packages/RECORD。当前wheel SHA256 `4d0e7c8d1455fa395faaebc5e0de123f7131a31d02c413f04266ec67bad78fc3`；Root运行/manifest详见Root `docs/progress.md` E95–98。自有库精确回收/Redis测试区空/owned进程自然终态、371冻结路径保持；七固定测试工具按当前lock URL/hash单独供应，资源测试不出localhost。

这些是限定源码/安装/资源门，不是完整Agent或用户链闭环：真实console启动/SIGTERM、S3/Docker/E2B/custom/provider、完整retention、全部数据owner同库组合，以及正式BFF/Web过程消费与浏览器仍未验；T-Q03/T-R02继续开放。此四文档前缀只修正当前态，不改源码、SQL、协议、生成物、依赖或锁；发布前Root须重建当前最终文档集合的wheel/sdist并核与已测包的全部entry/RECORD一致性，差异则重新验证。

---

## R118：构造期资源收束仍不改变 wire contract（2026-10-02）

**当前裁决：R119 九源 GREEN 候选仅改变 Agent 内部 ownership 与 shutdown 时序；HTTP/RPC/event/schema/version 字节面继续冻结，待 Root 最终复验。** R118 的真实 RED 已转为本地 GREEN；这不是机器 contract 或消费者发布完成声明。

- initial、resume、recovery 统一的 assembly ownership、未转交 handle close 与 Docker 半构造 cleanup 都不是公开 endpoint、payload、header、event 或 error code。
- caller cancellation 继续以原 `CancelledError` 传播；Supervisor 强持有仍在真实 connector/thread 中的 assembly。`drain_timeout_s` 覆盖 run、assembly、cleanup 的同一绝对停机预算，超时只形成进程级非零失败，不新增第二个 Run terminal 或网络重试协议。
- resume 只有成功 `_spawn_agent` 才转交 handle；转交前 replay、observed、非 Started、异常和取消均内部关闭。recovery probe handle 永不转交。close/cleanup secondary 不替换 primary，也不得把 secret、endpoint 或 payload写入日志/wire。
- Docker wrapper 后半失败只清本次自建 client 与本次新建 container；复用 sandbox 继续受原 durable cleanup identity 管理。该区别不新增请求字段或 cleanup event。

机器 OpenAPI、proof、Platform/Storage pins、generated Failure、contract provenance、SQL 与依赖锁保持未编辑；本地 contract/failure check 仍须与最终 evidence 一并记录。任何后继机械差异必须停止并交 Root 重新授权，不手填生成物。

---

## R117：资源关闭不改变 wire contract（2026-10-02）

**当前裁决：无 HTTP/RPC/event/schema/version 变化；生产实现尚未授权。** 基线 Agent `2653bcc723da5366fd877db73d701410e7bdc3a8`。本节与同轮 TECHNICAL_DESIGN、DATA_MODEL 及 Root R117 生命周期实施卡共同构成文档门，并遵循 [Python 后端工程规范](../../../docs/kokoro-handbook/standards/09-python-backend-engineering.md)。

- 现有 `RunRequest`、Feature/Agent 声明、AG-UI/Chat 投影、terminal payload、error code、sandbox durable cleanup record 与公开 HTTP 版本全部保持；不新增 client ownership、close 状态、timeout 或 endpoint 字段。
- 新增能力仅为 Agent 内部 Python 生命周期：Factory 创建的归档资源在成功时随 `AgentHandle` 转交给 Supervisor；plain/state 无资源时 close 为具名 no-op。不存在可供调用方注入任意 S3 client 的公开面，因此不以虚构 caller-owned 注入断言定义契约。
- native stream 排空、归档 operation 排空和 client close 是内部先后条件；终态 wire 仍由原 durable terminal transaction 决定。close/cleanup 错误必须可观察，但不得把 secret、endpoint、payload 或原始工具结果写入公开错误/日志。
- 顶层取消仍传播 `CancelledError`；普通 primary 不被 close 错误替换。shutdown 超过现 `drain_timeout_s` 是进程级非零失败，不生成第二种 Run terminal、不增加网络重试协议，也不声称资源已关闭。
- S3 workspace archiver 的本地 boto client 与 Docker/E2B/custom 的 durable sandbox teardown 是不同 ownership；后者继续使用现 repository cleanup intent/teardown contract，前者不发布为 Storage Artifact API。

机器 OpenAPI、proof、Platform/Storage pins、generated Failure、contract provenance、SQL 与依赖锁当前预期字节变更为零；runtime descriptor 摘要由现 codec 对实际源码自然计算，新 wheel RECORD 仅由 build 派生。任何实际机械生成文件差异须先由 Root 另行授权；若后继实现出现 wire 差异，须停止并重新通过 owner/版本设计门，不能以本节预先授权或手填生成物。

---

## R104：安装契约资源闭包 D0（2026-10-02）

**仅方案候选，待 Root 裁决；本轮机器文件全部冻结。** Agent main `17c73541ae5d9f123d85cf531a79503df5c463bd` 加冻结候选，Root `264de6b8`。实际 installed checker RED为 `/tmp/kokoro-r104b-installed-checker-red.json/.log`：安装exit0、checker exit1 / missing-installed-openapi、network_attempts0、临时安装已删除。HTTP36真实通过不代替安装门；下方R90的“当前/目标”属于当时设计，本节不重开HTTP5字段决定。

### Owner、版本与单一编辑方向

Agent canonical OpenAPI仍是 `contract/openapi/v1/openapi.json`，proof仍是 `contract/execution-proof/v1/`；`contract/provenance.json` 保留其现角色，派生digest由现 generator产出。Platform/Storage consumer pins只是各自owner发布的只读输入，不由Agent重写。安装目录不是第二可编辑contract中心，也不加入Root/BFF副本。
当前冻结候选的HTTP机器版本为5.0.0；Python distribution版本2.0.0、proof自身1.0.0是独立身份。本片不改HTTP方法/路径、decoded payload映射、Todo预算/空表语义、failure tuple、JWKS、完整HITL、签名或租户规则，不用安装布局变更制造协议升级或宣称正式发布。

### 建议方案B的安装资源清单

所有相对路径以Agent仓为源；目标是该wheel只读 `share/kokoro-agent/audit/` 下相同逻辑路径。完整方案比较与定位规则见本轮TECHNICAL_DESIGN；最终采用仍待Root批准。

| 输入闭包 | 安装与校验要求 |
|---|---|
| `contract/openapi/v1/openapi.json`、`contract/provenance.json` | 字节与canonical构建输入相同；仍核HTTP版本、exact route/governance/JWKS、decoded mapping、直接digest与generated provenance。 |
| `contract/execution-proof/v1/schema.json`、`vectors.json` | 保留schema、raw/canonical JSON、整数token、重复键、签名/JWK、正负向向量和owner aggregate全部既有门。 |
| `contract/platform/v1/provenance.json` 及 `execution-operations/v4/` 完整21文件树 | 输入集合以现 `EXPECTED_EXECUTION_SOURCES` 为准，含原owner provenance和20个payload文件；精确目录/无symlink/owner commit/digest/aggregate/inactive状态及原schema/ZIP/正负向/command向量均不削弱，不按旧README的v3历史清单打包。 |
| `scripts/generate_failure_models.py` | 作为现owner provenance的原始bytes资源；安装checker不执行该脚本、不联网装工具，不移除该来源记录。 |
| `OWNER_SOURCE_FILES` 中所有 `src/kokoro_agent/…` 文件 | 原10项inventory不删减；建议末尾追加新定位模块 `src/kokoro_agent/distribution_assets.py` 后，由现generator机械更新provenance。审计副本先逐字节对应实际安装module，再参加原有顺序aggregate。 |
| 已安装 `kokoro_agent/protocol/run_failure_generated.py` | 必须等于同wheel OpenAPI 经现 `failure_model_bytes` 派生的完整bytes，并等于审计副本；OpenAPI未变时本片不应改生成物。 |

这里21是当前读取到的固定v4集合，不是未来浮动latest；后继测试需核清单而非只核数量。Storage/Platform其余Proto/codegen原生成一致性门继续按owner pin运行，不把installed checker未承诺的生成工具执行伪装成已覆盖；不删既有5模块projector smoke，只将其归回安装门的一部分。

### 读取、生成与失败语义

- 既有显式 `validate(root: Path)` 仍验证调用者明确给定的源码/fixture事实；已注册 `kokoro-agent-contract-check` 必须在正常安装下默认绑定自身distribution，不搜索cwd/父级checkout、不接受缺包后从源树补齐。
- `importlib.metadata` 定位必须核该distribution的实际module origin、文件清单和唯一资源根，支持受验venv及`--target`；资源缺失/路径越界/混装/审计源与运行源不一致立即非零退出。只读资产不得自动重生成、下载或写回安装目录。
- 原完整 `validate_openapi_document`、`generate_failure_models(check=True)`、`validate_execution_proof_contract`、`validate_platform_binding_artifact` 顺序与语义保留；“安装模式”不得跳过任一分支、降低向量集合或删除console entrypoint。
- schema operator不是新HTTP/API：后继精确范围补入 `src/kokoro_agent/application/schema.py`，现入口先纯定位/读取/校验DDL再 `connect_pg`；现 `src/kokoro_agent/infrastructure/schema.py` installer在 `ensure_schema` 前取得已验SQL，事务内执行该已捕获值，不改现事务、require_blank或catalog语义、不新installer。初始缺失/漂移在连接前失败；直接installer面对既有连接时在任何schema创建前失败。
- 写模式只属于显式源码维护入口；source inventory机械变化须由Root另准后调用现generator，审查diff仅限清单/摘要。HTTP/proof/schema/vector/pin字节保持，不手填provenance、也不把RECORD当owner签名。

### 后继验收与消费者边界

先在现 `test_machine_contract.py`、`test_execution_proof_artifact.py`、`test_platform_generated_consumer.py` 和 `scripts/check_platform_wheel.py` 固化r104b真实缺资源RED与缺包/篡改/源回退负例；路径均位于本仓既有tests/contract或scripts，精确授权见TECH。缺生成文件、改实际module但不改审计副本、反向篡改副本、完整Platform子树缺文件、错误provenance、无/多RECORD定位均应非零；保留raw duplicate/canonical/边界负例，不只测文件存在。
DDL顺序负例扩现 `tests/unit/test_cli.py`（现仅CLI委派测试，须补application入口）与 `tests/contract/test_canonical_database_schema.py`：缺失/漂移时入口connect调用0、schema创建0，直接installer ensure_schema/DDL调用0；合法control维持原事务顺序。源码与测试路径均仅后继候选，本轮不实现。
Root再用新wheel SHA在无checkout导入的隔离环境验证CLI完整checker、原生成门与安装HTTP Todo精确bytes/identity/毫秒/cursor/watermark/跨tenant和subject隔离；全部是待验条件，本轮没有执行。Agent安装通过仍不等于owner已发布；BFF/Web只在Root正式commit/digest发布后固定消费，不把目录里出现HTTP5当已上线。retention与正式数据处置门继续保留。

---

## R90-W03：安全过程 Chat 契约 D0 闭集（2026-10-02）

Agent main基线17c73541ae5d9f123d85cf531a79503df5c463bd；本节替换未提交R87前缀，原HEAD正文保留。当前正式HTTP **4.0.0** 与机器artifact原字节不变；目标breaking **5.0.0** 待实施/验证/发布，不原位覆盖4的digest。
调用方向仍Agent internal-owner Chat→BFF durable AG-UI/snapshot→Web同源adapter；原/v1与payload_json载体不变，不新endpoint/浏览器Redis协议/跨仓可编辑schema。本文是已决定的实现约束；实施后唯一机器事实源仍contract/openapi/v1/openapi.json，不另建schema副本。

### decoded payload闭集与presence

| event_type / 分支 | 唯一字段与约束；以下列出的字段均必填，注明条件者除外 |
|---|---|
| todo.updated | 仅todos有序数组；每项仅content/status；status仅pending/in_progress/completed。主Run完整替换，显式[]清空。 |
| activity / tool | activity="tool"、activity_id、segment_id、status∈{running,completed,failed}、display_code="tool.execution"。 |
| activity / subagent | activity="subagent"、activity_id、segment_id、status∈{running,completed,failed}、display_code="subagent.execution"。 |
| activity / skill | activity="skill"、activity_id、preflight_id、source_refs（本Run canonical selected_skill_source_refs原有精确refs，不改其内部结构）、phase∈{resolving,loading,ready,failed}。 |
| skill失败presence | phase=failed **必须**有error_code∈{skill_resolve_failed,skill_load_failed}；其余phase **禁止**error_code（含null），无默认失败码。 |
| interaction.state / delivery | 原完整durable pause/revision/全部未决项、成功owner receipt与作品字段保持；不是上述activity分支，不施加其删字段规则。 |

对象均严格extra=forbid、不做类型强转；未知event/phase/status/code与错分支字段拒绝。安全activity没有truncated、name、args、result、error、description、source、subagent_type、tool_id、subagent_id或task_input；禁止可选raw字段/fallback。
未知内部工具/子agent名字仍映射上述唯一通用display_code；不把原名拼进code，不从模型文本猜调用身份。原私有执行payload的诊断/raw字段可保留，禁止借此进入公开Chat/AG-UI/snapshot；HITL与Delivery有各自既有授权语义。

### Todo边界与唯一字节预算

完整表0..100项；每项content为1..1024 **Unicode码点**的纯文本（不是UTF-16 code unit或grapheme计数），status仅上表三值。拒绝孤立surrogate U+D800..U+DFFF；有效补充平面字符解码成一个码点再计数。禁止trim、Unicode normalization或替换非法码点后偷偷通过。
容量计量对象明确为**整个decoded payload对象** {"todos":[{"content":…,"status":…},…]}，不是仅数组/单项，也不计外层ChatEvent、payload_json二次转义或HTTP envelope。确定性序列化后的UTF-8字节数≤65536（等号合法）。
定义C(x)：Python参考 json.dumps(x, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("utf-8", errors="strict")。这里对象键均固定ASCII，字典键按升序，数组原顺序；无BOM、空白缩进或末尾换行。Todo键顺序因此固定content再status，顶层仅todos。
跨语言C必须同字节：引号/反斜杠分别为\"/\\；U+0008/0009/000A/000C/000D用\b/\t/\n/\f/\r，其余U+0000..001F用小写\u00xx；斜杠不转义，其余Unicode含U+2028/U+2029直接UTF-8。输入解析仍严格拒绝重复对象键/未知字段，不用最后值覆盖伪装合法表。
先校验类型、字段、码点/项数，再按C计量；缺todos、null、任一非法项/超限均拒**整表**，不默补[]、不截断/删项、不发布旧值冒充新成功。显式[]是唯一空表更新；不存在计划与空表事实分开。
纯文本只按text渲染，不执行HTML；Todo应为面向用户任务摘要，不复制工具参数dump/包路径/隐藏推理。结构/容量校验不宣称通用自然语言脱敏；sentinel负例必须验证投影来源约束。

### opaque身份的确定编码（不改原Chat身份）

所有身份输入仅来自已批准执行域：受信request的tenant_id、namespace、session_id、run_id，以及现ToolInvoked/Returned的tool_id/segment_id、SubagentStarted/Finished的subagent_id/segment_id或RunEmitter预留的source_index。禁止从name、result、错误、模型文本、当前时间/随机UI key补身份；缺真实必需身份即拒投影。
设R=[tenant_id,namespace,session_id,run_id]，每项保持原字符串、无trim/归一化；H(tag,parts)=SHA256(C(["kokoro-agent.safe-progress.v1",tag,*R,*parts]))的**完整256位、64个小写hex**，不截短、不用base64。C即上节编码，parts只含字符串；不同tag做域分隔。digest只用于opaque归组，不是授权凭据/加密或任意文本匿名化保证。
tool activity_id="act_"+H("tool-activity",[segment_id,tool_id])；subagent activity_id="act_"+H("subagent-activity",[segment_id,subagent_id])；两者segment_id="seg_"+H("segment",[原segment_id])。这里右侧为真实原身份，左侧为公开opaque值；same source identity的start/finish保持同ID。
skill activity_id="act_"+H("skill-activity",[])（同Run稳定归组）；preflight_id="spf_"+H("skill-preflight",[str(anchor_source_index)])。anchor为首条**durable resolving**的非负Run source_index；十进制字符串，无前导零，0编码"0"。不得把未持久reserve当已确认轮次。
三种公开ID均固定68个ASCII字符；分别匹配 ^act_[0-9a-f]{64}$、^seg_[0-9a-f]{64}$、^spf_[0-9a-f]{64}$。不转发上述原tool/subagent/segment身份；原ChatEvent外层scope与chat_event_id/seq算法保持，不借本轮重写消息/HITL/Delivery身份。

### 真实阶段、发射与恢复

1. 先冻结static recipe；非空selected refs时首resolving reserve→构造ID→同原lease append_fenced成功确认，才可执行Skill I/O。Factory typed回调传阶段观察，emitter保管anchor/派生；同轮loading/ready/failed重用preflight_id。空选择不发假ready。
2. resolving在真实resolve前；loading在首真实包load前；ready仅全部所需peer的Skill resolve/load成功。多peer聚合一轮；MCP或model失败不制造skill.failed；failed的两个error_code严格对应resolve或load/包校验失败。
3. 回调未持久/失fence必须停止后续I/O并作为持久化/authority错误传播，不转换Skill/assembly失败、取消失败或第二terminal。initial/control/recovery与invoke catch一起承接；Chat成功而live失败由durable replay恢复，不重做外部操作。
4. 已提交事件replay保原identity/created_at/payload；崩溃后重新授权或包读取是**新真实轮次**，以新首resolving source_index为anchor，绝不复用旧ready。单纯再emit分配新index不是旧事件重放；未提交阶段不补造。
5. build成功后initial invoke调用Run锁内started幂等入口；resume不新started，恢复probe不自行产生started；next_index==0不再是资格。started原event/index/timestamp重取规则见DATA；Skill ready不是run.started或永久授权。

### replay、发布与消费者门

原tenant/namespace/session授权分页、Chat seq与namespace/run/source_index派生chat_event_id不变。source_index可有private/预留崩溃间隙；同源不同payload/时间冲突，禁止覆盖或伪造连续+1 Chat cursor。
BFF独占durable AG-UI与compact snapshot：同event_watermark边界上的最新Todo/activities/消息/HITL/作品，一致快照后只replay更晚事件；无计划、显式空表、历史缺失/过期须分开。保留active/waiting与cursor-expired恢复由BFF后继owner切片明确，不以Redis TTL冒充durable retention。
Agent5只在明确fresh批准测试或正式已批准数据处置cutover启用；不自动销毁用户数据，不双读旧raw，不把历史raw重标safe。Run purge未清Chat/Chat-BFF retention不足仍是发布/整链门，**不阻挡本轮闭集的纯RED**；不杜撰保留天数。
先Agent5发布commit/digest→BFF在direct/public6闭环后另定过程/snapshot版本并固定artifact→Web正规pin/复用既有组件；审批仍全集幂等、作品仍成功receipt，终态折叠不等于删除事实。R80 usage/v1独立后继，同writer串行，不占HTTP5、不以Billing/付款展示为前置。
机器/生成精确写集及全部caller见TECH本节；尤其contract_check.py、chat_contract_check.py的HTTP4断言/decoded mapping、现scripts/generate_failure_models.py及provenance必须一起闭环；生成物只收实际生成变化。proof自身1.0.0/签名tuple/JWKS及ChatFailure/interaction.state语义不改。
后继RED须覆盖合法/空/缺失/extra/raw、100/101项、1024/1025码点、补充平面/孤立surrogate、65536/65537 bytes与转义、全部phase/presence、无I/O失权、started/resume/replay漂移；Root另验真实PG/owner/刷新。闭集已锁，本轮业务测试、生成、HTTP调用均0。

---

## R80-W03：逐 attempt 用量与 Billing admission 契约目标（2026-10-02）

跨仓依据：[ADR-033：逐实际调用用量与 Billing 单一定价 owner](../../../docs/kokoro-handbook/decisions/ADR-033-actual-usage-and-pricing-ownership.md)。按 Root 已接受 owner 裁决同步；ADR独立审查不作为本仓实现验收。

原R80设计基线 main `444684d32473c96ddbb70247081b1d1cdb8558f1`；R143当前实现基线 main `79bf98c`；当前 HTTP 5.0.0 与所有机器字节未修改。本节沿已批准owner/发布顺序，R144有限artifact参数已由Root裁决、待contract落实，见§2.1；不是已发布字段/RPC。它与 TECHNICAL_DESIGN、DATA_MODEL 的 R80-W03 前缀共同覆盖历史“段汇总即可收费/固定Feature quantity=1结算”解释；不维护新旧双默认链或旧数据迁移。

### 1. Owner先行发布顺序

统一发布顺序：共同冻结语义 → Agent strict evidence producer artifact先发布 → Billing固定消费该artifact并发布逐attempt admission/证据接收contract → Agent固定消费Billing → 必要BFF消费者切换。纯artifact不依赖运行服务已经启动；语义协作不等于循环等待对方先发布。

下表为各owner的语义责任，不按表行顺序发布。launch只接收按IAM/Billing具名契约验证的付款/消费授权上下文，逐attempt预占在Agent准备actual call/attempt之后、provider之前申请；两者不得互代。当前不将逐attempt admission设为RunRequest必填，不新增Run级预占资源。

| owner / 机器事实源 | 必须先发布的语义 | Agent消费边界 |
|---|---|---|
| Billing Metering/Credit；本仓唯一批准OpenAPI版本 | immutable采购费率、销售倍率7/5可配置版本、换算/舍入；quote、reserve、增额/重授权、严格usage接收、settlement/unknown查询、release/reconciliation与幂等错误 | Agent不复制价格表、不自报actualMicros；仅传严格事实和引用、校验执行权限。现有feature/quantity报价及调用方actualMicros类型不作为本目标最终契约 |
| Agent；现 `contract/openapi/v1/openapi.json` 及 RunRequest边界 | 保持Agent durable launch与受信身份边界；若IAM/Billing具名正式契约要求新付款/消费授权引用，再决定RunRequest字段、绑定/缺失/冲突和breaking写集；逐attempt admission不在launch阶段必填 | 必要BFF普通/定时launch在Agent批准发布后串行消费；BFF不提前取得尚无attempt的预占引用。浏览器不自报付款资格/金额，request_id、execution_identity或Agent admission receipt都不是付款许可 |
| Agent；拟新增 `contract/usage/v1/schema.json`、`vectors.json`、生成的 `manifest.json` | internal-owner event-protocol的完整attempt evidence，ordering/revision/digest、幂等、知识状态和恢复；独立于浏览器AG-UI | Billing消费固定Agent artifact，不复制可编辑DTO；Billing接收operation仍由Billing发布，引用该producer artifact |
| System现model-catalog contract | model/provider/revision/digest/generation等planned技术binding及允许目标语义 | Agent原值冻结，不新增价格字段；planned不代替actual证明 |
| 实际gateway/provider证据发布方（Root须具名确认） | 请求与真实provider/model、内部retry/fallback/各attempt用量的可信关联，来源认证、可查询恢复和完整性 | 也可使用已验证无静默fallback的固定供应商绑定profile；两类证明均缺失时actual attribution unknown，单次HTTP不虚报gateway内部attempt已覆盖 |

usage artifact采用schema-first唯一可编辑schema与正负vectors；拟新增 `scripts/generate_model_usage_models.py` 单向生成运行时边界模型 `src/kokoro_agent/protocol/model_usage_generated.py`，manifest记录source/artifact digest；生成物只读，内部dataclass留model包。这一版本化契约目录有schema/vectors/manifest三项持续职责，不是新业务模块。具体发布版本、Billing operation名称与错误码由各owner批准，不在本D0预设已存在的RPC/字段tag。launch新增付款授权引用须先有IAM/Billing具名正式契约，再确定是否改变HTTP字段及所需breaking评审；本D0不先引入必填引用，也不默认补空或拿一次attempt admission代替整Run授权。

### 2. Agent evidence的必需语义组（逻辑字段，不代表已发布wire）

- 事件身份：schema版本、稳定source event identity、attempt identity、单调evidence revision、前版引用、canonical digest、UTC observed_at；精确同identity+digest重投幂等，漂移冲突不覆写。
- 归属：可信tenant、subject、actor、run/session、Agent launch admission/fence、受信付款/消费授权上下文绑定、logical call、peer/node、provider attempt序号、原lease owner/generation。请求trace仅关联，不授信；evidence按生命周期分闭集变体：已获授/已派发attempt必有该attempt的Billing admission/authorization opaque refs；预占拒绝或从未获授且未派发的证据明确“未获授”、无虚构引用；若已申请则携原预占请求关联，尚未申请则明确未申请，不编造请求引用。不得用缺引用的not_dispatched变体接收实际已派发用量。与Billing已持有付款/attempt绑定比对后接收。
- planned：System model/provider/revision_id/revision/digest/generation/tenant_generation、feature/label、gateway alias与planned provider model原值。不得把以后resolve的新revision改写旧attempt。
- actual：provider/model observation、gateway/provider request与response引用、来源类型/证据digest、attribution状态。缺actual时明确unknown及原因，不复制planned作为实测值；敏感URL、key、原prompt/response不进入payload。
- usage：inclusive token totals 的 known_zero / known_nonzero / unknown；分类知识独立；单位、input/output totals、provider profile版本、分类维度及其完整性。本Agent新evidence wire的数量采用canonical十进制字符串（0或非零首位的数字串，范围0..9223372036854775807），内部解析为严格int；禁止JSON number/bool/浮点/负号/前导零/溢出、缺失或null补零。attempt/evidence序号同样采用正十进制字符串。schema/golden vectors锁定一种格式，不影响当前未变的HTTP 5两项TokenUsage表示。
- outcome：not_dispatched、completed、failed、cancelled、unknown等执行事实与usage状态正交；若有已知部分，只作为partial evidence，不宣布最终总量。结束原因、请求是否可能发出、final/partial来源必须可区分。
- settlement相关：仅发送Billing引用及观测知识，不发送自算用户金额、费率/倍率修改或“是否收费”的Agent决定。Billing ACK/rejected/pending/settled不等于Run completed；同一source evidence可待决，而Run仍有唯一终态。

input/output总量与cache-read/write、reasoning、audio/image等维度的包含关系由每个provider profile声明。例如reasoning是output子集时不再加到output；cached input是input子集时保留子集用于Billing拆价，不增加总量。不同计量单位独立字段；未知分类不当普通token。完整profile要求的任何计价维度缺失则相关向量unknown，即使input/output总量已知。

### 2.1 R143-A 有限 token profile 与 artifact 边界

本片沿 R81 已裁：canonical decimal string 是唯一 Agent wire count（0..9223372036854775807）；正 ordinal/revision 同格式正数域。provider raw JSON integer 与 wire string 是两个边界，raw adapter 后继才接；不能从已补0的 SDK callback 反推 presence。类型/闭集由 schema-first 生成模型承接，跨字段/严格原始 JSON/修订由 model 纯 codec 承接。

`token_usage_v1` 在同一 evidence schema 的 `$defs` 中，单位 token、聚合 final_cumulative；source 仅 openai_chat_completions / ollama_openai_chat_completions。六项 input_total/output_total/total/input_cache_read/input_cache_write/output_reasoning 均显式 observation：reported 必有 count；unreported 无 count；not_applicable 词汇不授予使用权，当前两 source 均拒该分支。缺失/null 不转换为 observation 或0。

三 totals 全 reported 且 T=I+O 才 known_zero/known_nonzero；缺任一项为 unknown，不派生缺T；非法算术拒 normalized payload。cache-read 是 input 子集、reasoning 是 output 子集，双方 reported 时分别 C≤I、R≤O，不重复加总。当前两 source cache-write 固定 unreported；Ollama 当前保守 source 的 C/R 亦 unreported，意外原始明细不能擦除后伪称支持。分类与 totals 分轴：supported 且 totals known 为 partial，totals unknown 或 unsupported 为 classification unknown；当前 profile 不可达 complete，不造假正例。known_zero 只表示 inclusive totals，不等于未调用/供应商成本0/用户免费。

完整 artifact 仍包含 §2 的主体、Run/call/attempt、原lease、planned/actual、授权观察、outcome/seal、revision/前序/digest，不缩成孤立 totals JSON。actual unknown 与有效 token totals 可以并存，sealed unknown 不等于结清；付款语义未经 IAM/Billing 发布不自创 claims，prepared/not_requested 不编造 refs。Billing 可固定 totals-only 价目投影，但是否足以计价由其已冻结投影所需输入决定，不由 Agent classification 字段授予收费资格。

canonical 方向沿 R81：严格 UTF-8/重复 member 拒绝→typed与跨字段校验→排除顶层digest→RFC8785→固定 domain separator `kokoro-agent:model-usage-evidence:v1\n`（末尾 LF）→SHA256；保留 Unicode、不 trim/NFC，数组顺序保留。raw receipt digest 与 normalized evidence digest 分栏；同 attempt/revision 同 digest 重投，漂移冲突，原lease/planned不被新revision改写。以下技术参数已由Root锁定，正式机器/golden已在R145实现并通过Root限定验证，提交发布仍由Root执行，不把文本当发布凭据。

**R144 Root 已裁参数（2026-10-03，R145 contract 已落实并验证，待提交发布）：**下表是当前离线 artifact 唯一技术参数表，不是已发布 wire。原 R143 一页建议经 Root 接受，唯一修订为 UTC 固定毫秒，与 TIMESTAMPTZ(3) 精度一致；本片不改 SQL 或 HTTP5/public7 入域。规则实现与 golden/schema 一致后才发布，既有付款 owner 不变。

| 已批准参数 | R144 精确约束（沿 R81 / ADR033） | 已发布消费者影响 |
|---|---|---|
| 版本/闭集 | R81单schema：artifact_version=`1.0.0`，profile_id=`token_usage_v1`；所有object extra forbidden、所列分支必填，不加平行schema_version。顶层固定 identity、planned、actual、authorization_observation、execution、usage、revision、predecessor、observed_at、digest；具体nested按以下组展开，无任意dimensions | 新artifact，Billing发布后才pin；不改HTTP |
| 身份字段与容量 | 沿现ExecutionIdentity原结构+run_id/session_id/call_id/attempt_id/原lease owner；owner opaque串1..512 UTF-8 bytes、保原Unicode不trim/NFC，拒非法Unicode与控制字符；不得复制token/assertion正文。现HTTP可接更长值，artifact拒绝并报边界错误而非截断/改身份；此上限已由Root批准 | 不改旧入站接受域；后继运行接线须处理超界，不假称所有既有Run可直接编码 |
| 唯一tuple | R80/R81：attempt键=(tenant_ref,run_id,call_id,attempt_id)，版本键再加revision；session、actor、subject、identity_assertion_ref、原lease、planned、attempt_ordinal均不可变绑定，比较不匹配拒绝。call_id/attempt_id/event_id固定新生成标准小写UUID串（36字符），event_id每revision一个，重投不换；ordinal是同call实际尝试序号，不复用HITL ID | 无旧字段迁移；Billing按完整tuple而非raw digest去重 |
| 数量/序号 | R81已裁：count为0..2^63−1 canonical decimal string，ordinal/revision/lease_generation为1..2^63−1；内部任意精度算术再验界。manifest文件size是构建数值，不与业务token混淆 | 新wire；不改旧TokenUsage |
| revision/predecessor | R81单调/前序：首revision=`1`、predecessor=null；后续恰+1，predecessor={event_id,digest}指紧邻已接受版本。先识别同tuple/revision完整digest精确重投；新revision必须核前序，跳号/分叉拒绝，不以时间排序；无本地前序时不假称验证通过 | 无游标服务/新表；codec提供成对比较，持久并发后继 |
| UTC/时序 | Root依SQL手册批准：RFC3339固定UTC `YYYY-MM-DDTHH:mm:ss.SSSZ`，有效日历、年0001..9999、不闰秒/offset变体。prepared_at必有，dispatch_started_at/observed_at按执行变体presence；不靠跨主机时钟先后推断revision或“未发送” | 新artifact；不强改Chat毫秒时间戳 |
| planned/actual | System既有ResolvedModel字段原值固定；沿model/provider/revision_id/revision/digest/generation/tenant_generation/provider_model_name/gateway_model_name，数值采用本artifact十进制串。actual={state:unknown,reason}或{state:verified,provider_ref,model_ref,evidence_ref,evidence_digest}；verified仅表示上游具名认证凭据已核，不靠schema/response.model自证。unknown reason固定 `not_observed / unverified_binding / incomplete_attempt_coverage` | 不增System价格字段；真正verified来源认证仍runtime门 |
| 授权/执行观察 | D0已有生命周期：not_requested无refs；requested_unknown/denied必request_ref；granted必request_ref+admission_ref（opaque，绑定当前attempt）。未granted不得标dispatched；outcome闭集not_dispatched/completed/failed/cancelled/unknown，seal布尔仅不可变观察封存。schema合法不代表授权有效，不加入wallet/payer claims；payer上下文待IAM/Billing正式契约 | 不强制launch新增attempt admission；不发明付款API |
| usage与reason闭集 | R81：normalized token_usage_v1或unresolved；unsupported reason=`unsupported_usage_category / unsupported_service_tier / unverified_source_semantics`；首版未支持类别出现即隔离，显式0不例外。unresolved固定=`missing_usage / invalid_usage / incomplete_response / unsupported_source`，可携raw_usage_digest（未取得raw则明确null），无raw正文/非法计数。有效totals可保留在unsupported normalized分支，非法算术不可伪装unreported | Billing不可把unresolved/unknown补0；classification partial不自动阻/授收费 |
| 内容/byte预算 | R81有限profile：整个输入JSON≤65536 UTF-8 bytes（含空白），最大嵌套16；peer/node路径最多32个非空组件，每个≤128 UTF-8 bytes；其他不引入可扩展数组或任意map。无单Run调用总数上限。先预算/严格decode/duplicate-key拒绝再typed；超界不裁剪 | 仅单evidence安全边界，不限制HTTP过程分页或整个Run |
| digest/manifest | R81：evidence去顶层digest后RFC8785＋`kokoro-agent:model-usage-evidence:v1\n`（LF）SHA256小写64hex；不混raw摘要。manifest固定{artifact_version,owner:`kokoro-agent`,files:[{path,sha256}],aggregate_sha256}，path仓相对POSIX、按path排序且唯一，无..或绝对路径；inventory固定schema/vectors/CLI/compiler/codec/generated，不列自身/时间/commit；aggregate=对排序files RFC8785字节加独立domain `kokoro-agent:model-usage-artifact:v1\n` 后SHA256。消费者另外pinGit commit+manifest原bytes摘要，无自引用 | 同既有failure/proof manifest隔离；14路径安装assets闭包仍必须过实际installed门 |

seal 仅封存该 revision 的不可变观察；unknown 可由下一 revision 增补可信知识，旧 bytes 永不覆盖；已知计数或已核事实矛盾拒绝，不以 latest-wins 覆盖。纯 codec 的成对前序验证不假称已实现数据库原子 CAS 或消息认证。付款观察仅事实类型，schema/granted 字样不证明真实授权，不新增 payer/价格/launch 付款字段。

合法边界：revision="1"/predecessor=null，时间 `2026-10-03T12:34:56.001Z`；下一revision="2"引用前版event/digest且不可变身份不变。I/O/T="100"/"50"/"150"、C="20"、R="15"、W=unreported 为known_nonzero/partial，可同时actual unknown及outcome failed。非法：跳revision3引用1、同revision换event/digest、改原actor/planned、数字JSON count、缺T后补派生总数、微秒或非Z时区时间、未支持类别显式0当supported。unknown不是零，取消不授权整笔释放。

用户已批准 ADR033 的失败/取消规则：仅结算已核实实际消耗，释放可确认未用预占；unknown 持久待核实，不自动免费或整笔release。此业务规则由 Billing 实施，Agent本片只严格表达知识，不执行计价。上述离线参数未决项为0；实际认证/付款上下文契约、runtime SQL/恢复、费率及资源证明仍后继，不构成本片技术参数等待。


R145限定验证结果见CURRENT顶部；已知非null raw_usage_digest不可改写/抹除，已有normalized profile_assessment不可替换，null→known允许。独立 usage manifest/checker 与 installed 资产闭包已沿 TECH §5.1 的14路径实现；HTTP5、公有public7、RunRequest、AG-UI与原failure inventory保护。Billing 未运行不阻 artifact 发布；实际 provider/outbox/接收操作和按 attempt 的预占另行串接，不能以离线校验声称真费用链。

### 3. 预占、retry和重投协议边界

- launch付款/消费授权上下文由IAM/Billing具名正式契约定义和验证，不等于预占。Agent在call/attempt准备后申请逐attempt Billing admission；其执行许可必须绑定同tenant/subject/run、付款上下文及确切call/attempt、计划或允许actual路由集合、不可变报价版本、数量上限/并发额度及有效期。每个actual attempt外发前独立校验，前次许可不覆盖后续调用/重试/动态子代理。
- 预占请求/状态查询网络结果未知，保持同幂等identity查询/重投；未确认许可时不执行provider。预算变更是带前序authorization引用的新明确操作，不篡改旧请求重试body。
- 新actual retry有新attempt identity并重新获授额度；usage事件/outbox重投保持旧attempt/source identity和字节，不触发新推理。未知外部效果先reconcile，不自动new key/retry。
- 预占到期不证明已经dispatch的attempt零消耗；Billing须定义保留在途/unknown责任、查询及最终释放条件。Agent不自动release全部hold，不凭Run失败/超时/取消清账。
- actual归属超出预授集合时保留真实证据，停止后续调用并进入Billing reconciliation；不追认planned价格、不伪造事后预授权。未来gateway应在实际fallback前执行同等预算控制，否则该收费profile不放行。
- 各服务局部事务+outbox/inbox幂等组成恢复协议；不宣称跨服务/远端exactly-once。稳定拒绝不隐藏成成功或默认零。

### 4. Run终态与公开投影

现RunCompleted.token_usage只有input/output且全零时可为空，现RunFailed不携完整usage。目标Billing严格证据不依赖此简化浏览器投影；新Agent证据契约独立发布，BFF只转发其被授予的必要状态，不复制原始provider细节。

需要向公开Run投影表达usage completeness时，由Agent OpenAPI先发布，再由BFF public/AG-UI和Web依次消费；不把旧null解释为known_zero。终态事件保持唯一，不为迟到usage再发第二个run.completed。终态后证据修订使用独立usage revision通道，拒绝旧worker直接改写终态；Billing按正式更正/reconciliation契约处理，不覆写已结账历史。

### 5. 机器门与拒绝矩阵

后继须覆盖launch不要求未来attempt admission、付款上下文验证失败零provider、准备attempt后才获授、前次attempt许可拒用于后续，以及锁定字段presence、身份容量、UTC/整数界、分类互斥/子集关系、unknown组合、同key相同/不同digest、late revision前序、planned/actual错绑、SDK/gateway多attempt、cancel/失败仅核实消耗与unknown待核实规则、ACK丢失、跨tenant/subject拒绝及脱敏vectors。Agent正式checker/生成器和Billing消费contract测试共同通过后才进入consumer实现。

拟新增 `tests/contract/test_model_usage_contract.py`，扩现 `tests/contract/test_machine_contract.py`、`test_public_contract.py`；后继生成命令 `python scripts/generate_model_usage_models.py --check` 尚不存在、未执行。现有 `kokoro-agent-contract-check` 的通过也不证明此未发布目标通过。本轮全部contract/source只读。

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
## 2026-10-25 模型网络有限期限（既定实施门）

正式 OpenAI-compatible 路由显式传 SDK `request_timeout=httpx.Timeout(connect=5, read=120, write=30, pool=5)`，最终 HTTPX request extensions 必须保留四值。SDK whole-request `max_retries=0`；owned 异步 HTTPX transport 仅设置 native connect retries=1，最多重建一次未完成 TCP/TLS 的连接，不重放已经开始写入的 POST，也不因 read/write/protocol/HTTP status 重新发送。保持最终请求准入、凭据、证书验证、禁环境代理/重定向和同流 usage hook。

每次现 model-call middleware 的 handler 区间共用一个绝对600秒期限，不含该scope之后的seal/drain。若 handler 包含原生摘要，外层期限包含这段工作及摘要自身seal，内层摘要期限不延长外层；外层尚无最终请求时不冒记主模型已派发，摘要独立 scope/capsule/usage 不合并。只有本期限自身到期才产生专属 typed `model_unavailable/retryable=false`；内层普通 TimeoutError、外部取消、失租和原 denial 保持原分类/优先。超时仍封存实际不完整 usage 为 unknown/pending，不补零、不释放为免费、不重新派发旧 failed attempt。无新 Schema、wire、配置项或依赖；正常激活与新Run验收由Root另控。
