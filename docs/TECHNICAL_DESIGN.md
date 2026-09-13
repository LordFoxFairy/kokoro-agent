# kokoro-agent 技术设计

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
不杀死长驻调度循环。

## 6. System 模型路由接线（2026-09-08，已接线、live smoke待验）

本节是 ADR-031 的窄消费者切片，不把上文当前四层目录当作新增代码模板。Root 为本仓唯一 writer；
基线 `70a38138f42f29e8a482fde7890fe0e2d0c27e34`，工作树干净。跨仓任务表是 System 的 IMPLEMENTATION_PLAN G6-Agent。

| 放置项   | 决定                                                                                                                   |
| -------- | ---------------------------------------------------------------------------------------------------------------------- |
| Owner    | System 拥有 label/policy/availability→route；Agent 拥有执行、模型实例及进程凭据                                        |
| 当前事实 | `agent_factory.py` 创建 DeepAgents 时调用 `select_model_label`；没有 System client，默认硬编码 anthropic/claude        |
| 目标职责 | 创建实际模型前以 trusted tenant、feature、可选 label 调用 System；失败关闭，不绕过回本地名称                           |
| 目录比较 | 采用已有 `clients/system.py`，与现有 owner clients 邻接；拒绝新 `infrastructure/system` 或第二 runtime 层              |
| 粒度     | 一个单一 HTTP 边界模块含窄 Protocol/内部路由结果；`model/factory.py` 负责路由到模型实例映射                            |
| 依赖     | worker 入口创建进程级 HTTPX client 并关闭，Factory 依赖窄 ModelResolver；不跨仓 import/SQL，不导出 HTTPX Response      |
| 数据/API | 本仓 schema/Run wire 不变；System 契约 pin 见 API_CONTRACT；解析不处于数据库事务内                                     |
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
proof-checker 编排；proof 专项校验集中在 `src/kokoro_agent/execution_proof_contract.py`。A2a 已实现 pure runtime profile/signer；A2b 当前实现 private loader、anchored public-ring snapshot、JWKS HTTP projection 与独立 HTTP root。worker 尚未装配 private loader，database-clock lease reader、run-scoped supplier、IAM verifier 与 Platform adapter 接线仍未实现，不能作为端到端授权能力。

该 IAM 固定提交目前只把 `lease_generation` 描述为正整数，尚未固定本节的 JSON safe-integer 上界、原始 integer-token/
`1.0` 拒绝规则；其 ADR-005 的交付段也尚未拆出 Agent 真实 Platform client 接线与 Platform fresh/completed-replay receipt 两个门。
因此它是 owner/方向基线，不是已经与本 R2 候选逐字一致的机器契约。§7.5 第 2 步必须让 IAM ADR、verifier、OpenAPI/SDK 和测试按
Agent 已固定 artifact 消费同一数字矩阵与六段依赖，再称跨仓对齐；IAM 不复制或另行发明 proof schema。

### 7.1 放置与依赖

| 项       | 决定                                                                                                                                                                                                                                      |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Owner    | `kokoro-agent/execution` 唯一写 proof profile 与签发语义；`interfaces/http` 只投影 public JWKS                                                                                                                                            |
| 当前事实 | A1 schema/vectors/checker/provenance 已发布；`protocol/control.py` 已有 typed `ExecutionIdentity`；`RunRequest.session_id` 是目标 claim 的 `execution_session_id`；`LeaseFence` 已有 owner/generation，但现有 `is_lease_current` 使用连接前取得的应用时钟，不能作为签发 freshness 证据 |
| 目标职责 | 每次真实 Platform 出站调用前，使用 canonical Run、当前 execution identity、同一 run/owner/generation 的未过期 lease、Platform 边界已校验的 operation/binding 即时签发；proof 不缓存                                                       |
| 采用位置 | `execution/` 保存纯 profile/canonicalization、signer、worker-only private-key provider 与 run-scoped proof supplier；现有 Platform client adapter 接收 supplier；`interfaces/http/` 保存 HTTP-only public-ring reader/JWKS handler        |
| 淘汰位置 | 不新建顶层 `auth`/`attestation`/`ports`，不把签名塞进 `protocol/`，不在 supervisor 或 `build_deep_agent` 中拼 compact JWT，不让 IAM 反向读取 Run，也不让 Platform 自签或验签                                                              |
| 进程隔离 | A2b 提供 worker-only signer private-key settings/provider，但尚不接入 worker；HTTP 已只装配 public-ring settings/handler。两者使用不同配置类型、启动检查、readiness 状态和脱敏日志，HTTP 对象图不得包含 private path/bytes/provider |
| 数据     | 复用 canonical Run/lease；本设计不加表、列、Redis key、nonce receipt 或 key store                                                                                                                                                         |

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
查询排队跨过 `lease_expires_at` 时仍可能返回 true，因此目标 signer 不复用该语义。目标 reader 在获取连接后以一个 SQL statement
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

This slice still has zero production `issue_execution_proof` calls. Worker signer gating, statement-time lease supplier, Platform transport/call sites, IAM verification, and the real cross-owner transport remain A2c/later work.
