# kokoro-agent 契约治理

## Owner

`kokoro-agent` 拥有本仓 Agent execution boundary：Run admission、Run control、Run evidence、
identity-scoped session history/replay，以及本仓 Redis control/event envelope。

- HTTP 机器事实：[`openapi/v1/openapi.json`](openapi/v1/openapi.json)。
- Execution proof decoded profile：[`execution-proof/v1/schema.json`](execution-proof/v1/schema.json)；
  跨语言 canonical/JWS/JWK 正负向事实：[`execution-proof/v1/vectors.json`](execution-proof/v1/vectors.json)。
- Redis 命令和事件的运行时模型：[`src/kokoro_agent/protocol/`](../src/kokoro_agent/protocol/)。
- PostgreSQL 事实：[`database/schema.sql`](../database/schema.sql)，不属于 API contract。

## Pinned Storage consumer input

`storage/v3/provenance.json` 固定 kokoro-storage commit
`d13f33acfc7a482d61ebffc58e7b2667b58ae197` 的 common v1 与 Storage v3 Proto；owner
组合 SHA-256 为 `47e2659f577cf5c0a5e44a9ee9165d0fee17471bb3d7abb80095a956f655c1f9`。
`scripts/generate_storage_consumer.py --check` 在隔离 Python 3.11 环境重生成并逐字节比较
`generated/kokoro/storage/v3`。生成面含 owner 全部 17 RPC，生产 Artifact delivery dispatch
只开放原七操作。v2 pin/generated 已删除；无 alias、fallback 或双 pin。

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

本契约所有 HTTP operation 都是 `internal-owner`，浏览器不直连。
`/healthz` 与 exact `GET|HEAD /v1/execution-proof/jwks` 匿名；普通业务入口使用
Agent service bearer 与受信 tenant/subject/actor/assertion。两个 model-call reader
使用独立 Billing bearer，仅收 tenant 与 request-id，不接受人类身份／service 覆盖头或正文。
每项 operation 的 `x-kokoro-*` 是本 owner 机器治理元数据。

## Version

当前候选 HTTP contract version 为 `7.1.0`，URL 仍为 `/v1`，未据此声称已合并或正常激活。
本次只新增 `GET /v1/model-call-attempts/{attempt_id}/prepared` 与两个响应 schema；
完整 HTTP7 父图由固定 SHA256 的精确逆证保护，原 proof2、usage1/2 及 sealed evidence
读取保持。prepared 快照是当次有界活租约观察，不是消费许可、成本保证或历史恢复入口。
详见 [API 契约](../docs/API_CONTRACT.md) 与 [技术方案](../docs/TECHNICAL_DESIGN.md)。

`info.version` 与内部 URL major 分别计数。既有 required typed Skill selection、project_ref、
完整 interaction.state／resume 决策全集和严格 failure 形状不变；不增加旧输入 alias 或 fallback。
消费者各自固定 owner artifact，Root 仅聚合已验证已合精确版本，不把配置读取冒真实执行。

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
