# kokoro-agent 契约治理

## Owner

`kokoro-agent` 拥有本仓 Agent execution boundary：Run admission、Run control、Run evidence、
identity-scoped session history/replay，以及本仓 Redis control/event envelope。

- HTTP 机器事实：[`openapi/v1/openapi.json`](openapi/v1/openapi.json)。
- Execution proof decoded profile：[`execution-proof/v1/schema.json`](execution-proof/v1/schema.json)；
  跨语言 canonical/JWS/JWK 正负向事实：[`execution-proof/v1/vectors.json`](execution-proof/v1/vectors.json)。
- Redis 命令和事件的运行时模型：[`src/kokoro_agent/protocol/`](../src/kokoro_agent/protocol/)。
- PostgreSQL 事实：[`database/schema.sql`](../database/schema.sql)，不属于 API contract。

BFF 是浏览器 Product API 和 AG-UI projection 的 owner；Capability、Storage、IAM、Model 等仓库的
业务模型不复制到本目录。Agent 只通过各 owner 的版本化 public contract 接入外部能力。

## Visibility

本契约所有 HTTP operation 都是 `internal-owner`，只供 BFF 或受信服务调用；除 `/healthz` 与 exact `GET|HEAD /v1/execution-proof/jwks` 外需要
Agent service bearer credential，`/v1/*` 还需要由受信调用方传入的 tenant、subject、actor 和 IAM
assertion headers。浏览器不得直接调用此服务。每个 operation 的 `x-kokoro-*` 扩展是机器可审查治理元数据。

## Version

当前 HTTP contract version 为 `1.1.0`，路径版本为 `/v1`。Protobuf/RPC 不在本仓发布；Redis envelope
的 `kind` 集合由 `protocol/control.py` 和 `protocol/events.py` 的严格模型定义。Breaking change 必须
新建 `/v2` 或新的消息版本，并在 ADR 中记录，不通过修改文档标题伪装成兼容变更。

## Generation

OpenAPI 文件是本仓手工审查的 canonical source，不从 Root、BFF 或数据库生成。Pydantic 模型、HTTP
handler 和 contract tests 必须与它同步；禁止手改任何未来生成的 client。检查命令：

```bash
uv run kokoro-agent-contract-check
uv run pytest -q tests/contract/test_machine_contract.py
uv run pytest -q tests/contract/test_execution_proof_artifact.py
```

Execution proof schema 是 decoded `{protected_header,claims}` 字段的唯一可编辑事实；OpenAPI、Pydantic
或消费者仓不得复制 claims schema。Vectors 固定 raw/canonical UTF-8、unpadded base64url、signing input、
独立 oracle signature、public JWK/thumbprint、one-bit tampered signature 和语义拒绝事实；A1 验证结构、重组、canonical bytes与
tamper 差异，数学验签与 tampered rejection 属于后续 signer/verifier 切片。

## Breaking policy

变更顺序固定为：先修改本文件同目录的 OpenAPI source 或 protocol model，运行 JSON/OpenAPI 结构校验和
contract tests，再更新 handler、client、示例和文档。字段删除、必填化、枚举收窄、错误 code 重命名、
路由语义改变均视为 breaking change。发布前针对上一个固定 release artifact 做 schema diff，并由
BFF consumer contract test 验证。

## Provenance

contract source 与实现属于同一个 Git commit；`contract/provenance.json` 的 `source_files` 必须精确等于
checker 内置的完整有序 owner inventory，同时记录 HTTP `1.1.0` direct path/SHA、execution-proof schema/vector 各自 digest 和 aggregate
digest。消费者固定 `repository + commit + version + schema path/hash + vectors path/hash`，不把会随无关
OpenAPI/protocol 变化的 aggregate 当作 proof digest。重新计算 provenance 后，必须把 contract、测试和文档放在同一逻辑 commit 中。运行时
事件的持久化顺序由 PostgreSQL ledger/receipt owner 保证，Redis 只是可重放传输，不是公开协议事实源。
