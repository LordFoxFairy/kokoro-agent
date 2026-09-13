# kokoro-agent API 契约

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

Agent A1 artifact 与 A2a signer 已提交：owner machine fact `contract/execution-proof/v1/schema.json` version=`1.0.0`、
canonical/negative/tampered-signature vectors 和 immutable Ed25519 signer 已分别验收。A2b 当前是待复审候选，增加隔离 key loader、
public-ring snapshot 与 HTTP JWKS projection；这仍不是端到端授权能力。A2c/consumer、IAM verifier、Platform proof wire 均待实现，
真实 transport 也尚未接线。后续仍按 TECHNICAL_DESIGN §7.5 串行发布；supplier 独立测试、Platform 最终 contract、Agent 真实 client
接线与 Platform server cutover 是不同验收门，不能互相冒充。

IAM `bf160be173ef473bebe8e4a93b74ec52c230f180` 是当前 owner/方向基线，不是本候选已经发布的消费者契约：该提交仍只写正整数
`lease_generation`，尚无本节 safe-integer/token 矩阵，也未把六段交付全部展开。第 2 步必须在 IAM ADR、API、verifier、OpenAPI/SDK 与
测试中消费 Agent 固定 artifact 和同一拒绝向量；不得由 IAM 复制一份可独立漂移的 claims schema。

1. Agent A1 machine artifact 与 A2a signer 已验；A2b key/JWKS 是当前待复审候选，A2c run-scoped supplier/consumer 尚待实现；
2. IAM ADR/API/安全设计对齐后实现 verifier/OpenAPI/generated SDK，并消费同一 strict profile、数字矩阵与 canonical vectors；
3. Platform owner 发布最终 compact-proof wire、request-binding 与 generated helper；
4. Agent 真实 Platform client 逐 call 接入 supplier；
5. Platform consumer 删除旧 wire并完成 fresh/completed-replay receipt；
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

Launch identity remains trusted `X-Kokoro-*` headers. It is not accepted from the launch body. A2c proof supplier, IAM verifier, Platform proof wire, and real transport are not part of HTTP `1.1.0`.
