# kokoro-agent 安全设计

## Trust boundary

浏览器只能访问 Web/BFF；Agent HTTP 只接受部署注入的 service bearer。BFF/IAM 负责认证与授权断言，Agent
验证断言所需的 tenant、subject、actor 和 assertion ref，并从受信 context 派生 namespace。body 中的
tenant、user、workspace、namespace 或 permission 不作为授权真源。

## 数据隔离

每个 repository 查询显式接收 namespace/tenant scope；run、session、history、replay、control 和 evidence
都先做 scope 检查。无外键时由应用事务、锁、状态机、唯一约束和 reconciliation 维护引用完整性。跨 owner
不读数据库、不做 JOIN。

## Secret 与出站

API key、service token、MCP header 和 Storage credential 只来自环境/secret provider，不进入日志、Run
request、checkpoint、event payload 或公开 response。MCP egress policy 在 worker 启动时解析一次；外部
HTTP client 必须有 connect/read/overall timeout、取消传播、响应大小上限和错误归一。

## 日志与输入

结构化日志使用 service、operation、request_id、trace_id、结果和耗时；对 token、密码、原始 prompt、
provider payload 和文件内容做脱敏。JSON body 有大小上限，未知字段拒绝，动态 SQL 标识符使用白名单，值
全部参数绑定。错误只返回稳定 code/message。

## 发布

CI 阻断依赖、源码和 secret 扫描；镜像使用不可变 base digest、非 root、healthcheck、SBOM、provenance、
漏洞扫描和签名/attestation。第三方 GitHub Action 固定完整 commit SHA。

## Execution proof contract、signer 与 A2b key/JWKS boundary

A1 已有 Agent-owned strict schema、canonical/negative/one-bit-tampered vectors、provenance 和静态 contract checker。A2a pure runtime
profile 与 Ed25519 signer 已通过 SPEC/QUALITY 与 Root 验证：owner-controlled immutable config 只持有
issuer/kid/private key，caller input 不能覆盖 header/version/
issuer/audience/kid；签发前预计算 exact JCS bytes，PyJWT 签发后逐段核对、限制 16 KiB、验证64-byte signature并用派生公钥自验。
A2c 新增的唯一 production signer caller 位于 standalone run-scoped supplier；它没有 worker/client composition。A2b 已实现隔离 private loader、public-ring snapshot 与 JWKS route；worker 装配、IAM verifier 与 Platform consumer 仍不存在，不能据此接受端到端授权。

Agent 独占 execution proof 私钥与 canonical signing bytes。worker private-key 配置和 HTTP public-ring 配置必须是不同类型、不同对象图：
worker 进程不加载 public JWKS ring，HTTP 进程不读取或持有 private key/path/provider。worker 与 HTTP 各有独立、非 secret 的
`kid + RFC 7638 public JWK thumbprint` active descriptor：worker 对 private key 派生值核对，HTTP 对当前 public ring 核对；任一自身
不一致都 fail closed/readiness 失败。跨进程组合只由部署阶段门核验，不要求两个 descriptor 在 rotation 中间阶段始终相等。

私钥只从受控 file/secret mount 读取，不允许环境变量明文。使用 `O_NOFOLLOW` 打开后在同一 fd 上 `fstat`：文件必须由 effective UID
持有、是 regular file、mode 为 `0400` 或 `0600`，内容非空、有界且仅含一把 PKCS#8 Ed25519 private key；解析失败、错误曲线、
多 key 或同一 fd 校验失败时 standalone loader 本身 fail closed。只有后续 A2c/consumer composition 接入它之后，这才成为 worker pre-consumption gate。public ring同样从受信、non-symlink regular file读取并做有界 strict parse。private key bytes、
compact proof、signature、完整 binding、原始 identity assertion、token 和 key parse error detail 不得写日志、trace、metric label、checkpoint、
event 或 exception response。结构化日志只记录 operation、run/request/trace correlation、`kid`、contract version、结果码与耗时；不记录 `jti`。
A2a signer 将所有 profile/JOSE/crypto failure 收敛为稳定脱敏错误，不含 private key/path、compact proof/signature、JTI或完整 binding；
它不读取环境、文件、数据库或网络。private file parser/permission/symlink与真实派生 thumbprint 核验已由 A2b loader 实现，但该 loader 尚未接入 worker 启动。

public ring 只接受 strict Ed25519 public JWK，不包含 `d`、certificate chain、URL 或 provider metadata。JWKS route
`GET|HEAD /v1/execution-proof/jwks` 是现有“除 health 外都要求 bearer”规则的第二个具名例外：public key 可匿名读取，但部署层只允许 IAM
所在内部网络访问；route 不接受 tenant、identity、body 或 query。所有状态都 `Cache-Control: no-store` 且无 ETag/304，防止中间 cache
把 IAM 30 秒 freshness 扩大。route 不重定向且 JWKS representation body 不超过 64 KiB（不含 HTTP headers），与 IAM client 对 response body 的2秒/no-redirect/64 KiB/single-flight计数边界一致。
HTTP public ring错误时 `/readyz` 和 route 都返回 503，不发布部分集合；private loader 尚未接入 worker，因此 A2b 没有建立 worker private-key startup/consumption gate，该边界属于后续 A2c/consumer composition。
HTTP YAML scalar 遵循 SafeLoader tag 语义：implicit scalar 与显式 core `str/null/bool/int/float/timestamp/binary` tag 只在 HTTP allowlist leaf 上构造，自定义或不支持 tag fail loud，再由 composition boundary 收敛为稳定脱敏配置错误；worker-only subtree 的 value 不被 HTTP root 构造或持有。

HTTP lifecycle 在 SIGINT/SIGTERM 后先启用 draining admission gate，停止新的业务/readiness admission，再在固定 2 秒 deadline 内等待已登记 handler；期限内完成的声明响应保持完整。超时后进程有界退出，不强制取消超时 Python handler thread，也不把 daemon thread 退出冒充业务 drain 成功。

正常多副本 rotation 只允许依次出现：`ring{old}/HTTP descriptor old/worker descriptor+private old` -> 全 HTTP
`ring{old,new}/HTTP descriptor old` 且全部 worker old -> 全 worker descriptor/private 切 new 且 HTTP 保持
`ring{old,new}/descriptor old` -> 全 HTTP descriptor 切 new 且 ring 保持 `{old,new}` -> 从最后一次 old 签名至少 70 秒后全 HTTP
移除 old，最终 `ring{new}/HTTP descriptor new/worker descriptor+private new`。每阶段检查所有 replica，任一落后、失败或 thumbprint
不一致都停止推进；禁止同一 deployment 中随机选择 active key。紧急事件先停止受影响 signer，再移除 public key；IAM 已缓存 snapshot
仍可能在 30 秒 freshness 加 2 秒 refresh timeout 边界内接受旧 key，这不是实时撤销保证。

A2c standalone proof supplier 每次 issue 都重新读 lease、nonce 与签名；未来 production composition 只可在实际 Platform 出站调用前触发。已落地的 lease reader 在连接获取后以 PostgreSQL database clock验证 same run/owner/generation、
unexpired、not paused、not terminal；不得复用连接前应用时钟的现有 helper，也不得在 build 阶段签发后供多个 Skill/MCP 调用复用。
签发后 generation race 的 claim TTL 受 `exp <= min(iat+60s, floor(lease_expiry))` 限制；IAM 5 秒 verifier skew仍可能接受到
`exp+5s`。IAM 仍验证固定 issuer/audience/header/time/signature并重验当前 permission；Platform 仍重算 exact binding并执行资源策略，
三者任何失败均 fail closed。

canonical header/claims 只含 API_CONTRACT 列出的 exact fields。`lease_generation` 只接受 JSON integer `1..9007199254740991`，
`iat/exp` 只接受 JSON integer `0..9007199254740991` 并执行正向时间语义；在 crypto 前拒绝 `2^53`、`2^53+1`、0 generation、
负数、boolean、float 与任何 coercion/truncation/stringification。另拒绝 duplicate/extra claim、非 canonical JSON、
`none`/symmetric/错误 alg、unknown kid、`jku`/`x5u`/embedded `jwk`、跨 tenant、operation/binding 篡改和超 TTL/clock skew。Signer 不复制
IAM permission catalog或Platform Skills/MCP资源目录；typed actor/subject kind不可丢弃或默认成 user。

`kid`、`iss`、`tenant_ref`、actor/subject `opaque_ref`、`run_id` 与 `execution_session_id` 只要求非空字符串；不得在 proof contract
另加 ingress 不具备的 pattern/maxLength。`jti` 必须匹配 `^[A-Za-z0-9_-]{21}[AQgw]$`，canonical decode 为恰好 16 bytes且 re-encode
相等；trailing pad-bit alias 与 `=` padding 均在进入 crypto 前拒绝。

当前 machine schema 对 `lease_generation/iat/exp` 的 `x-kokoro-require-integer-token=true` 是安全规则，不是说明性注释；
contract-check必须对原始JSON token执行，并以`1.0`失败样本防止解析器先coerce后通过。IAM 当前固定基线尚未写入该矩阵；后续 verifier
必须在通用 JSON number coercion 丢失 token 形态前或通过 canonical-byte equality 执行同一拒绝规则，并用 Agent canonical vectors 做
跨仓 consumer test，不能只验证签名或只依赖数学 integer schema。

### A2b private/public material boundary (current)

The worker-only loader accepts only an euid-owned exact `0400/0600` regular final file opened with native `O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK`, reads at most 16 KiB, compares complete pre/post fd metadata, admits one unencrypted PKCS#8 Ed25519 block, verifies the RFC 7638 thumbprint in constant time, and performs a sign/verify challenge before constructing the A2a signer. Its parent directory is explicitly a trusted secret-mount boundary.

The HTTP-only reader anchors at `/`, retains nofollow directory fds, rejects unsafe owners/write bits and symlinks, reads a bounded regular public file, verifies full pre/post/path identity, strict UTF-8/duplicate-free exact Ed25519 JWKs, active descriptor, UTF-8 kid ordering, and immutable JCS output. FIFO and all failures close every fd and publish only an unavailable state. Safe startup logs contain only allowlisted booleans/numerics—never URLs, bearer, path, kid, thumbprint, key/proof bytes, or underlying exception text.

A2b does not wire the private loader into worker startup. A2c now supplies standalone statement-time lease proof minting, while worker composition, IAM verification, Platform policy/binding, and real transport remain absent.
