# kokoro-agent 数据模型

## W2-F2-S2 交付意图与恢复边界（2026-09-28；设计门，canonical SQL 未改）

Storage `d5cfc442c675e32363ae767f5ec662a9e0d9eaea` 是 Upload、Blob、Asset、Artifact、Scan
及交付阶段命令 receipt 的唯一 writer；Agent 只拥有 Run/lease、现有 tool journal、执行事件/outbox 和 chat 投影。
不在 Agent 存 Storage Asset/Artifact 镜像、预签 URL、对象 key/字节或扫描报告，不跨 schema JOIN、双写或
新建交付总表。`source_run_id` 是 Storage 的来源关联，不可替代授权、conversation scope 或作品 ID。

已存在的 `kokoro_agent_run.request_json` 保留受信 `tenant_ref`、`subject.kind/opaque_ref`、`actor`、
`run_id`、`session_id`；worker 从数据库 pending dispatch 回读后取得 owner/generation lease。BFF 正式
`chat` launch 的 `session_id` 等于 Conversation ID；Agent 的 `runtime_namespace(tenant,subject)` 是私有
workspace/checkpoint locator，不是 Storage scope ID。每次外部阶段发送前，使用本仓 canonical Run 行的
当前 owner、generation、数据库时间下未过期、非暂停/非终态条件核 lease；不得把 `is_fence_current`
（仅 owner/generation）当作有效性检查。Storage 只看到已认证代言 metadata，不查询 Agent 表。

同一 `(run_id,tool_call_id)` 的交付在现有 `kokoro_agent_tool_journal` 留一个不可变意图：
path、内容 SHA/size/MIME、kind/title、tenant/subject/conversation、各阶段 command ID 与 request digest；
不存内容 bytes 或 secret。首次外部写之前先冻结；后续重入必须逐项比较，变更则失败关闭。
`started` 不是“肯定未写”：网络结果未知时按原 command 和 Storage owner 状态/receipt 恢复，不能创建新
Upload/Artifact；已 `succeeded` 的完整结果可重放稳定 `artifact_id + asset_id + digest`，已失败只返回
明确失败。当前 middleware 对所有非豁免工具的 `started` 直接给 unknown-outcome error，且表内
`result` 仅用作终态文本，**尚无上述意图或恢复实现**；代码门应只为 deliver 增加具名重入/意图保存，
保留其他工具的 fail-closed 语义。若 `result` 复用会破坏已存结果语义，应先提交本仓唯一 canonical SQL
变化、事务/索引/测试；本次文档门不预写第二 Schema。

阶段状态由 Storage 的 Upload/Asset/Artifact/receipt 权威事实核对，不在 Agent 再造一套可漂移状态机。
CreateArtifact 请求的可选 `artifact_id` 始终留空，由 Storage 按稳定 create command ID 派生；
Agent 仅保存其回执 ID，Finalize 用同一 ID。AbortUpload 只在已确认 pending 且取消/确定失败时用
自身稳定 command；PUT/Complete 未知时先查状态或重放原命令，不盲中止。

Agent 只在 final+CLEAN 的 Storage 稳定三元组校验后，先把 `artifact_id + asset_id + digest` 保存进
现有 journal 成功结果/可恢复事件意图，再以 `(run_id,tool_call_id)` 为逻辑唯一键 stage 一条
`delivery.created` critical outbox 帧；queued 帧以同一 event ID/durable_seq/source index 重放，
确认 Agent Chat `event_type=delivery` 投影后，才允许同 Run 的 `run.completed`。当前 outbox 仅有
`(run_id,durable_seq)`/`event_id` 唯一约束，critical 集不含 `delivery.created`，不能声称已经按
tool_call_id 去重；代码门须核可否复用现有 event ID 唯一约束与 journal 事务，若不够则仅对本仓
canonical 增加必要的窄约束/列，不建第二总表。Chat append 的
`(tenant,namespace,run_id,source_index)` 去重只保证同一 outbox frame 的投影幂等，不能替代
工具级 frame 唯一性。

Storage final 后、Agent journal 前的崩溃靠原 owner command/receipt 恢复；journal 后、outbox 前靠
冻结的事件意图补 stage；outbox 后、Chat 投影前靠 queued frame 补投影。不能仅靠当前普通 live emit：
它可能在 `_drain` 异常路径被丢弃，且终态 fence 后再追加会被 supersede。Agent 与 Storage 之间不存在
跨 owner 原子事务；通过稳定身份、owner receipt、Agent 去重/outbox 与终态顺序收敛，测试覆盖
各 crash window。单独 tool 结果或短期 URL 不是最终用户可见交付的完成证明。

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
