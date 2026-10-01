# kokoro-agent 契约治理

## Owner

`kokoro-agent` 拥有本仓 Agent execution boundary：Run admission、Run control、Run evidence、
identity-scoped session history/replay，以及本仓 Redis control/event envelope。

- HTTP 机器事实：[`openapi/v1/openapi.json`](openapi/v1/openapi.json)。
- Execution proof decoded profile：[`execution-proof/v1/schema.json`](execution-proof/v1/schema.json)；
  跨语言 canonical/JWS/JWK 正负向事实：[`execution-proof/v1/vectors.json`](execution-proof/v1/vectors.json)。
- Redis 命令和事件的运行时模型：[`src/kokoro_agent/protocol/`](../src/kokoro_agent/protocol/)。
- PostgreSQL 事实：[`database/schema.sql`](../database/schema.sql)，不属于 API contract。

## Pinned Platform consumer input

`platform/v1/provenance.json` pins two read-only Proto inputs plus the complete 17-file
`platform-execution-operations/v3` artifact (including its provenance) copied from Platform commit
`5b6eb2c1532b23b9747bc4bf6ac99f69ad453de0`. Platform remains their only editable owner.
`scripts/generate_platform_consumer.py --check` regenerates the Python Protobuf and async Connect
client plus the explicit 24-message tenant request projector in a temporary Python 3.11 environment and byte-compares it with
`src/kokoro_agent/generated/`, rejecting stale Platform outputs while preserving the unrelated Storage generated tree. The isolated generator uses `buf-bin==1.73.0` (including its WKT),
`protoc-gen-py==0.1.1` and `protoc-gen-connectrpc==0.11.1`; those packages stay out of the application environment because
the Connect generator pins `protobuf-py==0.1.1`, while runtime `connectrpc==0.12.1` requires
`protobuf-py>=0.3.0`.

The owner execution-operation manifest remains `inactive` and `routable=false`. The build-time
artifact checker hard-codes the owner repository/commit, the exact 17-path v3 owner/path/digest
inventory (including the owner provenance raw digest), the v3 aggregate, 24 request bindings,
31 operations, 53 positive vectors, 142 negative vectors and 139 command-projection vectors. Separately, the
offline typed projector rejects protobuf-py's permissive scalar and message construction,
including cross-wrapper ID messages. The pinned artifact and generated files alone are not a
Platform business adapter, proof supplier call site, or activation claim.

BFF 是浏览器 Product API 和 AG-UI projection 的 owner；Capability、Storage、IAM、Model 等仓库的
业务模型不复制到本目录。Agent 只通过各 owner 的版本化 public contract 接入外部能力。

## Visibility

本契约所有 HTTP operation 都是 `internal-owner`，只供 BFF 或受信服务调用；除 `/healthz` 与 exact `GET|HEAD /v1/execution-proof/jwks` 外需要
Agent service bearer credential，`/v1/*` 还需要由受信调用方传入的 tenant、subject、actor 和 IAM
assertion headers。浏览器不得直接调用此服务。每个 operation 的 `x-kokoro-*` 扩展是机器可审查治理元数据。

## Version

当前候选 HTTP 机器 contract version 为 `4.0.0`，路径仍为 `/v1`；尚待 Root 协调发布。
本次 breaking 将 resume 一次切为 required expected_pause_revision/pause_ref、全集 item_id 决策，
Chat 源改为完整 interaction.state；旧 tool_id/request_id 寻址与 interaction 事件不保留 alias。
此前 Failure code/required retryable/合法 tuple 保持原义，不增加公开 reason。
`info.version` 与内部 URL major 分别计数。2.0 已要求的 selected_skill_source_refs/[] 保持不变，
不接受旧 trace fallback。Agent 固定 commit → BFF strict repin/正式 Product+AG-UI → Web 固定消费 →
Root 仅对已授权自有 fixture 有序停止/清理/fresh 切换；禁止旧 failure JSON 双读或默认补字段。
Protobuf/RPC 不在本仓发布；其他 Redis envelope 的 kind 与 payload 继续由现 Pydantic 定义。
正式对外发布后的 breaking 仍需评审版本、消费者与数据切换，不能沿用本地 fixture 清理作为生产迁移。

## Generation

OpenAPI 文件是本仓手工审查的 canonical source，不从 Root、BFF 或数据库生成。Pydantic 模型、HTTP
handler 和 contract tests 必须与它同步。失败形状例外为已落地的单向生成：基础 Failure 唯一定义
code/required retryable/合法 tuple，RunFailure 与 ChatFailure 在最终 profile 用 unevaluatedProperties=false
封闭；ChatEvent.x-kokoro-decoded-payloads 精确登记 run.failed→ChatFailure 与
interaction.state→ChatInteractionState；两者 payload_json 仍为字符串，消费方按声明解码。
`scripts/generate_failure_models.py` 调用 `chat_contract_check.py` 内唯一失败 profile 编译器，生成
`src/kokoro_agent/protocol/run_failure_generated.py` 的 RunErrorCode/RunFailedPayload/ChatFailure；
仅 stdlib/Pydantic，无 protocol 向内依赖。生成物带完整 OpenAPI source hash，不手改；
`--check` 验证完整 bytes/header/direct provenance，失败不写文件。contract_check 只编排，继续保持既有粒度门。

`createRun` 202 与 `replaySessionEvents` 200 分别以 `LaunchReceiptEnvelope`、
`ReplayPageEnvelope` 约束既有 `LaunchReceipt`、`ReplayPage`，不以开放的
`DataEnvelope.data={}` 代替。这一绑定由 owner contract checker 和实际 HTTP dispatch
测试共同防漂移。

检查命令：

```bash
uv run python scripts/generate_failure_models.py --check
uv run kokoro-agent-contract-check
uv run pytest -q tests/contract/test_machine_contract.py
uv run pytest -q tests/contract/test_chat_response_envelopes.py
uv run pytest -q tests/contract/test_execution_proof_artifact.py
uv run pytest -q tests/contract/test_platform_request_binding.py
uv run python scripts/generate_platform_consumer.py --check
```

Execution proof schema 是 decoded `{protected_header,claims}` 字段的唯一可编辑事实；OpenAPI、Pydantic
或消费者仓不得复制 claims schema。Vectors 固定 raw/canonical UTF-8、unpadded base64url、signing input、
独立 oracle signature、public JWK/thumbprint、one-bit tampered signature 和语义拒绝事实；A1 验证结构、重组、canonical bytes与
tamper 差异，数学验签与 tampered rejection 属于后续 signer/verifier 切片。

## Breaking policy

变更顺序固定为：先修改本文件同目录的 OpenAPI source 或 protocol model，运行 JSON/OpenAPI 结构校验和
contract tests，再更新 handler、client、示例和文档。失败字段只编辑 OpenAPI 后再生，不另编辑生成模型。
字段删除、必填化、消费者闭集枚举扩展或收窄、错误 code 重命名、
路由语义改变均视为 breaking change。发布前针对上一个固定 release artifact 做 schema diff，并由
BFF consumer contract test 验证。

## Provenance

contract source 与实现属于同一个 Git commit；`contract/provenance.json` 的 `source_files` 必须精确等于
checker 内置的完整有序 owner inventory，同时记录 HTTP `4.0.0` direct path/SHA、execution-proof schema/vector 各自 digest 和 aggregate
digest；generated_artifacts 记录 failure 生成物 direct hash/source hash/generator。消费者固定 `repository + commit + version + schema path/hash + vectors path/hash`，不把会随无关
OpenAPI/protocol 变化的 aggregate 当作 proof digest。重新计算 provenance 后，必须把 contract、测试和文档放在同一逻辑 commit 中。运行时
事件的持久化顺序由 PostgreSQL ledger/receipt owner 保证，Redis 只是可重放传输，不是公开协议事实源。


## HITL 4 receipt 与发布门

HTTP 202 只表示 command durable admission；成功 receipt 不证明 native 消费、不会清空 waiting。
worker 事务真正抛出的 InteractionConflict 才记 failed/error_code=interaction_conflict；不发布 reason，
不把 missing context、读取故障或 authority_lost 伪装为此 code。正常 waiting head/source 保持。
args/reason 这两个允许 nullable 的 optional 字段可 null/omitted 同义归一；revision/ref/item 无默认。
RunResume 严格验证后唯一 model_dump_json body 与规范化 request_digest 进入同一 command ledger。

本工作树是完整 owner 切换候选，仍待 Root 真 HTTP/PG、默认全门与固定 artifact 验收。
发布次序为 Agent owner → BFF 固定版本/commit/digest → Web 消费 BFF；无 /v2、兼容开关或先改消费者 pin。
HTTP 4 不代表完整 scope/retry/effective-native/retention 或 P3B 已完成。
