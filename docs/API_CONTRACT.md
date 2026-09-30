# kokoro-agent API 契约

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
404 ROUTE_NOT_FOUND、403 POLICY_DENIED 不重试；503与传输超时标记暂时故障，客户端不自动重试；
worker 既有单 Run 失败路径收口，不泄漏上游 message/secret。

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
