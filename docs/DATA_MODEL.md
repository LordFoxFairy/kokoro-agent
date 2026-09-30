# kokoro-agent 数据模型

## W3 typed Skill source Run fence 当前态（2026-09-29）

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
当前 schema/contract 验证命令保持不变；目标失败路径与依赖 pin 见 TECHNICAL_DESIGN §6、API_CONTRACT。

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
