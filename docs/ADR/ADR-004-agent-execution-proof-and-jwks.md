# ADR-004：Agent execution proof 与 public JWKS

- 状态：Accepted；A1 machine artifact 已实现；A2a runtime profile/signer 已通过 SPEC/QUALITY 与 Root 验证；A2b/A2c 待串行实现
- 日期：2026-09-11
- 决策 owner：`kokoro-agent`（proof schema/canonical bytes、lease-aware signer、private key 与 public JWKS）
- 关联 owner：`kokoro-iam@bf160be173ef473bebe8e4a93b74ec52c230f180`（proof verification/current authorization/audit）；
  `kokoro-platform`（Skills/MCP operation/request binding/resource policy/receipt）

## 上下文与当前态

Agent 已持久化 canonical `RunRequest`、typed `ExecutionIdentity` 与 monotonic `LeaseFence.generation`，并在 A1 发布 proof machine
schema、canonical/negative/one-bit-tampered vectors、provenance 和静态 checker。A2a pure exact profile 与 Ed25519 signer 已通过
SPEC/QUALITY 与 Root 验证，但没有 production caller；仍没有 private signing-key provider、
JWKS route、lease-aware supplier 或 Platform client 接线。
Capability 当前的临时 attestation/wire 不是 Agent/IAM 已发布契约；IAM ADR-005 已裁决
新 verifier 必须等待 Agent owner artifact，Platform 必须等待 IAM generated SDK。

关联的 IAM 固定提交已裁决 owner、proof/JWKS 方向、TTL/skew 与 current authorization，但目前仍只描述正整数 `lease_generation`，没有
本 ADR 的 JSON safe-integer/token 拒绝矩阵；其交付段也未拆出 Agent 真实 Platform client 接线和 Platform fresh/completed-replay receipt。
因此该提交是输入基线，不是已经逐项同步的消费者契约。Agent artifact 固定后，IAM 阶段必须先同步 ADR/API/安全设计，再由 verifier、
OpenAPI/SDK 与 consumer tests 消费同一 schema/vectors；IAM 不拥有或复制 claims schema。

现有 `is_lease_current` 在打开 PostgreSQL connection 前读取应用 clock。连接池或 statement 排队跨过 lease expiry 时，SQL 仍用旧 instant，
可能把已过期 lease 判断为 current。`build_deep_agent` 已接收 `RunRequest + LeaseFence`，但当前 Skill/MCP resolve/read call 没有 fence；在 build
时只签一次会让后续调用绕过 current lease。

## 决策

### 1. Owner 与机器事实

Agent 已新增唯一 decoded-profile machine fact `contract/execution-proof/v1/schema.json`，version=`1.0.0`，并将 schema/vectors 纳入
`contract/provenance.json` 与 `kokoro-agent-contract-check`；固定 test vector验证 protected header、claims、JCS UTF-8 bytes、unpadded
base64url signing input、64-byte Ed25519 signature shape、JWK 与 one-bit tampered signature recomposition。A2a runtime test 现以 RFC 8032
seed复现 exact positive compact proof，并用派生 public key自验、拒绝 one-bit tamper；未来 OpenAPI 只描述 JWKS HTTP projection，不复制claims schema。

protected header 精确为 `typ=kokoro-agent-execution+jwt`、`alg=EdDSA`、`kid`。claims 精确为 `contract_version`、`iss`、`aud`、
`tenant_ref`、typed `actor`/`subject`、`run_id`、`execution_session_id`、JSON safe integer `lease_generation`、exact `operation`、lowercase 64-hex
`request_binding_sha256`、integer `iat`/`exp` 与 unique `jti`。`kid`、`iss`、`tenant_ref`、actor/subject `opaque_ref`、`run_id` 与
`execution_session_id` 只要求非空字符串，不附加 pattern/maxLength。`jti` 是每次签发新生成的 128-bit CSPRNG value，以匹配
`^[A-Za-z0-9_-]{21}[AQgw]$` 的 canonical 22-character unpadded base64url 表达；decode 必须恰好 16 bytes并 re-encode 相等。
`contract_version="1.0.0"`；audience exact
`https://kokoro.dev/resources/iam-execution-authorization`；TTL 不超过 60 秒，clock skew 前后 5 秒。V1 不允许其他 header/claim，尤其不携带
`identity_assertion_ref`、`nbf`、owner scope、permission、Skill/MCP ID 或 key-source URL。

`lease_generation` 精确为 JSON integer `1..9007199254740991`；`iat/exp` 精确为 JSON integer `0..9007199254740991` 并继续满足
`exp>iat`、TTL、clock 与 lease-expiry policy。strict parser 在 crypto 前拒绝 bool、任何 float、负数、`2^53` 及更大值，禁止
truncate、round、string conversion 或 coercion。Schema 的 integer/min/max、pre-coercion runtime type guard 与 canonical-byte
equality 共同执行；尤其 JSON Schema 可能按数学值把 `1.0` 视为 integer，后两层必须仍拒绝该 float token。
当前 schema 因此对三字段标记 `x-kokoro-require-integer-token=true`，contract-check从原始JSON token执行该扩展。

header/payload 分别按 RFC 8785/JCS 生成 UTF-8 bytes；compact JWS 三段都使用 unpadded base64url。解码拒绝 duplicate member、额外字段、
非 canonical bytes、float NumericDate 与隐式 Unicode normalization。IAM exact验证 issuer/audience/profile/signature/time；Platform 不解析 proof。

### 2. Per-call current lease gate

Agent 为每个 canonical `LeasedRun` 构造 run-scoped proof supplier，但不在 build 时签发。Platform generated adapter先校验 owner-defined
operation并从当前请求计算 binding，然后在每个真实 HTTP/RPC call 前调用 supplier。Supplier 每次生成新 `jti`，查询 current lease，签发一次并
立即附加；proof 不缓存。Skill package cache/MCP snapshot只缓存其既有数据，不缓存或延长 proof。

目标 current-lease reader 由 signer 消费方定义最小 Protocol，并在连接获取后用单一 PostgreSQL statement 的同一个
`clock_timestamp()` instant验证 `run_id + owner + generation + lease_expires_at > db_now + terminal=false`，返回 `db_now` 与 expiry。
应用签名 clock与 db_now 超过 5 秒、paused/terminal/expired、owner/generation不同或依赖失败均 fail closed。`exp` 为
`min(iat+60s, floor(lease_expires_at))`，没有至少 1 秒正有效期则不签。IAM verifier只把 5 秒 skew用于
`iat <= verifier_now+5s` 与 `verifier_now < exp+5s` 的时钟容差；minted `exp-iat`仍不超过60秒，最坏接受时间可到`iat+65s`。

lease query 与 Ed25519 signature/网络请求不是原子事务；generation 在查询返回后失效时，已签 proof 仍可能在剩余 TTL 内被验证。
这是真实在途 race，不声明实时撤销。需要即时撤销时必须新增 Agent-owned introspection/assertion contract，不能由 IAM 查询 Agent 数据库或
由 Platform 推断。

### 3. 文件放置与进程隔离

纯 claims/canonicalization、signer、worker-only private-key provider与 run-scoped supplier 放在既有 `src/kokoro_agent/execution/`；
public-ring reader/JWKS handler 放在既有 `src/kokoro_agent/interfaces/http/`；client call site在既有 Skills/MCP adapter中接收 supplier。
不新建顶层 auth/attestation/ports，不污染 `protocol/`，不让 supervisor 组装 JWT。

worker 与 HTTP 是同一 package 的两个进程，但 key 配置严格分离。worker 只解析 private key file 及 worker active
`kid/thumbprint`；HTTP 只解析 public ring 及 HTTP active `kid/thumbprint`。worker 从 private key 派生并核对，HTTP 从 ring 核对；
不一致分别阻止 worker 消费和 HTTP readiness。两个 descriptor 在 rotation 中间阶段允许不同，跨进程组合由部署阶段门验证。HTTP 对象、
配置摘要与错误路径永远不持有 private path/bytes/provider。

private key 必须以 `O_NOFOLLOW` 打开并在同一 fd 上 `fstat`，来自 effective UID持有、mode `0400`或`0600`的regular file，内容非空、
有界且只含单一 PKCS#8 Ed25519 key，不允许 environment plaintext。HTTP public ring也从受信non-symlink regular file有界读取，
只允许 strict OKP/Ed25519 signing JWK且绝不含 `d`。所有 key/proof/signature/binding/identity assertion 均按 SECURITY 脱敏。

### 4. JWKS contract 与 rotation

版本化 route 为 `GET|HEAD /v1/execution-proof/jwks`。它是仅限内部网络的 `internal-owner` public-key endpoint，也是现有 service bearer
规则的具名匿名例外；不接受 tenant、identity、query或 body。GET 返回标准 JWKS，HEAD 同 status与representation headers（包括GET
表示长度）但不写 body；known path其他 method为
405并发送 `Allow: GET, HEAD`，unknown为404。所有响应 `Cache-Control: no-store`，无 ETag/304，避免中间 cache 与 IAM 30秒 snapshot叠加。
route不重定向，完整canonical response不超过64 KiB，兼容IAM的2秒total timeout、64 KiB limit、no redirect、per-issuer single-flight、
known-key 30秒freshness和unknown-kid最多一次refresh。OpenAPI operation固定`x-kokoro-permission=none`。

正常多副本 rotation 的允许组合依次为：

1. `ring{old}/HTTP descriptor old/worker descriptor+private old`；
2. 全 HTTP `ring{old,new}/HTTP descriptor old`，全部 worker 仍 old；
3. 全 worker 切 new，HTTP 保持 `ring{old,new}/descriptor old`，记录最后一次 old 签名时间；
4. 全 HTTP descriptor 切 new，ring 仍 `{old,new}`；
5. 自最后一次 old 签名至少 70 秒后，全 HTTP 移除 old，最终 `ring{new}/HTTP descriptor new/worker descriptor+private new`。

每阶段逐 replica 验证；任一落后、失败或 thumbprint 不一致都不推进。紧急轮换先停 signer 后移除公钥；IAM fresh snapshot 下旧 key 的
撤销上界仍为 30 秒 freshness + 2 秒 refresh timeout，不能描述为即时。

### 5. Skills/MCP 与 IAM 边界

operation/binding由 Platform owner contract定义，signer只接受 Agent adapter已按固定 generated artifact验证的值，不复制 IAM 的 5 Skill/16 MCP
catalog或permission映射。proof不携带 Platform的 `series_id/skill_id/installation_id`、
`connector_id/server_id/connection_id/authorization_id/invocation_grant`；这些 typed opaque reference由 Platform request binding覆盖。
这与已核验 Manus v2 的 list-first/opaque-reference原则一致，但不复用 Manus wire、ID或 owner。

IAM仍独占 Platform caller authentication、operation -> current permission catalog、direct-user policy与decision audit。Agent不读取IAM数据库，
IAM不读取Run/lease，Platform不验Agent签名或根据subject补 scope。交付顺序不得并行倒置：

1. Agent machine artifact/signer/JWKS/run-scoped supplier 独立验收，supplier unit/fake-client 不冒充真实 call 接线；
2. IAM ADR/API/安全设计对齐后实现 verifier/OpenAPI/generated SDK，并消费同一 strict profile、数字矩阵与 canonical vectors；
3. Platform owner 发布最终 compact-proof wire、request-binding 与 generated helper；
4. Agent 真实 Platform client 逐 call 接入 supplier 并验证 transmitted proof；
5. Platform consumer 切 IAM SDK、删除旧 wire 并完成 fresh/completed-replay receipt；
6. Root 真实 Agent -> IAM -> Platform -> PostgreSQL receipt sandbox。

任何阶段都不产生临时 wire、fallback、alias 或双协议。

### 6. JWT/crypto 依赖

采用直接声明 PyJWT 与实际直接使用的 `cryptography`，不依赖其他 package 的 transitive 安装。理由是让成熟 JOSE/key algorithm实现处理
EdDSA选择、PKCS#8/JWK parsing与signature verification边界，Agent只实现受限 profile的canonical bytes与业务 lease gate。实现前必须核验
当前稳定版本、Python 3.11支持、许可证、公开 API能否对预先 canonicalized bytes签名、供应链与退出路径；不允许使用 private API。

被否决的 `cryptography`-only手工 compact JWS方案虽然依赖更少，但会让本仓自行承担JOSE algorithm/header/key dispatch与format规则，增加
安全审计面。若实现核验发现 PyJWT 的公开 API不能保持本 ADR 的 exact bytes，必须先回到 ADR比较成熟 JOSE候选，不能静默手写或放宽
canonical contract。

### 7. A1 schema/canonicalization 依赖核验（2026-09-12）

A1 直接声明 `jsonschema>=4.26.0` 与 `rfc8785>=0.1.4`，lock 实际解析为 `jsonschema==4.26.0`、
`rfc8785==0.1.4`。前者是 MIT、PyPI 标记 Production/Stable、要求 Python >=3.10，支持本仓 Python 3.11
目标与 Draft 2020-12 meta-schema；后者是 Apache-2.0、纯 Python/无依赖、要求 Python >=3.8，公开 `dumps`
直接返回 RFC 8785 UTF-8 bytes。两者均从 PyPI wheel/sdist hash 固定，未引入 PyJWT/cryptography 的 A2
生产依赖。

候选比较：继续使用 Pydantic 会在解析前丢失 duplicate-member/token spelling；只用 stdlib `json` 不实现
RFC 8785；在本仓手写 JCS 会扩大安全审计面。因此 A1 组合 strict stdlib raw parser + jsonschema + rfc8785，
并用 owner vectors 固定边界。`rfc8785` 当前仍标记 Beta、最新版本停留在 2024-09 且维护者规模较小，升级风险
由固定 vectors、canonical-byte equality 和 package drift 门隔离；退出路径是在相同 vectors 下评估另一成熟
RFC 8785 实现，或经独立审计后内置最小 canonicalizer，不改变 schema/wire。`jsonschema` 的退出路径是在保持
Draft 2020-12 meta-schema与同一负向矩阵的前提下替换 validator；依赖升级必须重新核验 Python 3.11、许可证、
供应链 attestation、API 与完整 contract gate。

### 8. A2a signer 依赖核验（2026-09-12）

A2a 实施日重新核验上游 PyPI、官方 API reference 与 cryptography Ed25519 文档：最新稳定 PyJWT 为 `2.14.0`，MIT、
Python >=3.9、PyPI Production/Stable；最新稳定 cryptography 为 `50.0.1`，`Apache-2.0 OR BSD-3-Clause`、
Python `>=3.9, !=3.9.0, !=3.9.1`、PyPI Production/Stable。两者均支持本仓 Python 3.11，现作为直接依赖声明
`PyJWT>=2.14.0` 与 `cryptography>=50.0.1`，lock 精确固定 `2.14.0`/`50.0.1` 及 wheel/sdist hashes；PyPI 显示
PyJWT 2.14.0 通过 Trusted Publishing 发布并提供 provenance，cryptography 由 PyCA 维护并提供 CPython 3.11 ABI wheel。

生产实现只调用 PyJWT 顶层公开 `jwt.encode(payload, key, algorithm, headers, json_encoder)`；自定义 encoder 将 header/payload
绑定到既有 RFC 8785 bytes。签后不信任 library serialization：逐段 canonical base64url 解码、与预计算 bytes exact 比较、
检查64-byte signature，并通过 cryptography `Ed25519PrivateKey.public_key().verify` 自验。源码与 AST 门拒绝 `jwt.api_*` 私有 API。

维护/供应链风险：PyJWT 当前 PyPI 仅列一名 maintainer，JOSE serialization/default header 行为可能在 minor 升级变化；
cryptography 包含 native/Rust/OpenSSL wheel 与频繁 major release，平台 wheel、ABI、构建工具链和安全公告需要持续跟进。
升级必须重跑 A1 exact compact vector、独立 RFC 8032 KAT、tamper/alg/curve/length 矩阵、wheel build 和完整 contract gate。
退出路径是在相同 public API、exact pre/post bytes与向量下评估另一成熟 JOSE library；若替换 cryptography，则必须保留成熟的
Ed25519 provider与同一KAT。不得回退到 `jwt.api_*`、自研 JOSE/Ed25519 或放宽 canonical wire。

## 替代方案

- IAM签 proof：倒置 Run/lease owner并要求IAM复制Agent事实。
- Platform签或本地验签：复制Agent key/IAM授权责任，无法重验current permission。
- build时签一次并缓存：后续Skill/MCP调用可能跨lease expiry/generation接管。
- 复用现有应用时钟 `is_lease_current`：连接排队可跨expiry，不能证明实际签发前current。
- 用数据库/Redis保存key或nonce：首版无需持久化；exact binding、短TTL、IAM每次current-state复验与Platform receipt已定义replay边界。
- 给JWKS加bearer或可缓存CDN：增加bootstrap secret/缓存撤销层；public key只需内部网络限制和明确的no-store响应。

## 影响与验证

优点是 proof、identity authorization与Capability资源仍各有唯一 owner；typed actor/subject和lease generation不会在Platform手写wire中丢失。
代价是每个真实Platform调用多一次current lease查询和签名，且compact proof仍有显式在途race。

实现验收覆盖：canonical bytes/vector；数字矩阵 `2^53-1`、`2^53`、`2^53+1`、0、负数、bool、float及无coercion；
header/claim/alg/typ/kid/aud/iss/TTL/skew/jti；private file type/owner/mode/symlink/empty/malformed；worker/HTTP descriptor与各自key不一致；
所有replica的ring/HTTP descriptor/worker descriptor阶段组合、正常rotation和紧急32秒边界；same run/owner/generation与stale/paused/terminal；真实PostgreSQL
连接排队跨expiry RED/GREEN；签后generation race；每call新proof/no cache；无secret日志；JWKS GET/HEAD/no input/405/404/503/no-store；
schema/OpenAPI/provenance/package drift；以及真实 Agent signer -> IAM verifier -> Platform receipt sandbox。所有证据绑定同一commit。

本 ADR 的 A1 machine artifact/source/dependency/test 已实现；A2a runtime profile/signer 已通过 SPEC/QUALITY 与 Root 验证，
且没有 production caller。private loader、key/JWKS、statement-time lease supplier、Platform client、
IAM verifier 与跨仓 sandbox 仍按既定顺序待实现。
