# kokoro-agent 当前实现

状态日期：2026-09-12。本文件只记录当前代码、canonical schema、contract 和已执行证据；目标值与未来
设计分别见 `SLO.md`、`TECHNICAL_DESIGN.md` 和 ADR。

## 已落地

- Agent 使用 DeepAgents 原生 loop/state/checkpoint；GA 只有一个构造入口 `agent_factory.py`。
- Redis launch/control 是可重放通知；PostgreSQL 保存 dispatch intent、lease generation、receipt、
  outbox、chat facts、tool journal 和 cleanup intent。
- `database/schema.sql` 是唯一当前 DDL；没有 `database/migrations`、迁移 ledger、外键或跨仓 SQL。
- 数据库时间列使用 `TIMESTAMPTZ(3)`；PostgreSQL adapter 在数据库与内部 epoch-millisecond 边界间转换。
- HTTP ingress 先做 service bearer 与 trusted identity 校验，再打开 PostgreSQL/Redis；控制命令使用
  `Idempotency-Key` 和 request digest；Run 查询按 trusted tenant 与派生 namespace 双重 predicate 隔离，
  chat 查询只接受同一 identity 派生的 namespace。
- worker 具备 dispatch CAS、lease generation fencing、终态 claim、outbox republish、control reapply、
  sandbox cleanup retry 和 graceful drain。
- Skill/MCP/Storage 通过窄 client port 接入；Agent 不读取 Capability/Storage 私库。
- 生产发行包不包含本地 MCP/Skill fixture；缺少可选 Capability 时使用显式 `None`/unavailable
  状态，不组装伪 client。LangGraph checkpoint locator 使用受信 identity 派生 namespace 加 session
  id；本地 profile 默认复用 `127.0.0.1:55433/kokoro_worker_agent` 和 Redis
  `127.0.0.1:56380/9`，并设置连接/读写超时；CI 由 workflow 显式注入 service 地址。
- canonical schema 的 operator use case 位于 `application/schema.py`；`cli.py` 与 `worker/main.py` 从该稳定边界导入，
  不再让 CLI 依赖 worker transport。
- OpenAPI、protocol model、canonical database schema、contract test 和 provenance 已进入本仓。
- Execution proof A1 已发布 Draft 2020-12 decoded-profile schema 与跨语言 canonical/negative/one-bit-tampered vectors；checker 以硬编码
  有序 owner inventory、逐 artifact digest、aggregate digest、strict duplicate/token parser、expected schema pointer/keyword、RFC 8785、
  canonical unpadded base64url/JTI、16 KiB 上限和单差异负向语义校验防止漂移。A2a 独立 runtime exact profile 与
  Ed25519 signer 已通过 SPEC/QUALITY 与 Root 验证，并以 A1 positive vector 和第二个 RFC 8032 KAT
  固定数学签名；A2b 当前实现 HTTP route、private loader 与 JWKS snapshot，但 worker gate、lease supplier、IAM verifier 与 Platform client 仍未实现。

## 当前证据

以最近一次主工作区验证为准，提交前重新执行：

```bash
uv run ruff check src tests
uv run pyright
uv run pytest -q
uv run kokoro-agent-contract-check
uv build --wheel --sdist
```

真实 PostgreSQL/Redis 验收必须显式提供 `KOKORO_AGENT_DATABASE_URL` 和 `KOKORO_REDIS_URL`，不能用内存
替身代替 integration/acceptance。

## 仍需收敛的工程项

1. 历史包目录中的部分执行编排仍较大，需按 use case、repository adapter、outbox 和 supervisor 生命周期
   语义拆分，不能按行号机械切割。
2. `domain/`、`application/`、`infrastructure/`、`interfaces/` 是当前目标架构边界；叶子运行模块按真实职责保留，
   新代码不得恢复顶层 `repositories/`、`services/` 或 `http/` 重复入口。
3. Capability/Storage 真实 HTTP/RPC client 的生产装配需在部署配置中显式启用；未配置可选能力时应返回
   明确 unavailable，而不是创建伪实现。
4. CI/release 的 action SHA、镜像 digest、SBOM、provenance、签名和候选镜像 health gate 需要全部落地。
5. 内部 HTTP DTO 的时间字段仍是 epoch milliseconds；对外 BFF/AG-UI 投影必须转换为 RFC 3339 UTC，
   并在协议升级切片中删除重复时间语义。
6. Agent execution proof A2a 已通过 SPEC/QUALITY 与 Root 验证；A2b candidate 已包含独立 private-key loader、public-ring snapshot 与
   anonymous JWKS route，但尚未把 private loader 装配进 worker，且仍无 production caller 或 statement-time lease supplier。现有 lease helper在数据库连接前读取应用 clock，不能作为 proof freshness gate；
   当前 Skill/MCP client 也没有 run-scoped supplier。因此 focused signer 可复现 vector 不等于 proof 已可签发/传输，不得跳过
   key/JWKS/supplier 串行门直接放行 IAM verifier 或 Platform consumer。

这些条目是代码工作的清单，不以文档声明替代实现或验证。

## System 模型解析消费切片（2026-09-08，消费者切片静态/单元已验）

System HTTP client已注入真实worker/AgentFactory，先解析可信tenant/feature/可选label，再构造DeepAgents模型；
移除select_model_label与anthropic/claude兜底。CLI需System服务凭据及LiteLLM配置，嵌入部署可显式注入ModelResolver。
System revision/digest/generation记结构化日志；本仓DB与公开Run wire未变，源契约pin见contract/provenance.json。
尚未运行live System/网关smoke，不声称完整执行链路通过；本轮验证见ACCEPTANCE，commit随Root交付记录。

## Agent execution proof 设计门（2026-09-11，已验收）

目标边界已记录在 TECHNICAL_DESIGN §7、API_CONTRACT、DATA_MODEL、SECURITY 与 ADR-004：Agent 将拥有 exact v1 proof
schema/canonical bytes、每次调用前的 database-clock lease gate、Ed25519 signer、分离的 worker private-key/HTTP public-ring 配置和
`GET|HEAD /v1/execution-proof/jwks`。数据库与 Redis 均不变化；Skills/MCP typed opaque identity 仍属于 Platform，proof 不携带资源 ID。

IAM `bf160be173ef473bebe8e4a93b74ec52c230f180` 已对齐 owner、proof/JWKS 方向、TTL/skew 与 current authorization，但仍只写正整数
`lease_generation`，尚未纳入本 R2 的 safe-integer/token 拒绝矩阵，且交付段尚未拆成下面六个独立门。该差异属于第 2 步的显式输入；
在 IAM 文档、verifier、OpenAPI/SDK 和 consumer tests 固定消费 Agent artifact 前，不记录为跨仓契约已对齐。

2026-09-12 的 A1 切片已新增 owner machine schema/vectors、contract test、checker/provenance gate，并直接声明
`jsonschema>=4.26.0` 与 `rfc8785>=0.1.4`；未修改 OpenAPI route、数据库、Redis、signer/key/JWKS/lease 或
Platform/IAM/Capability。后续必须串行推进：

2026-09-12 的 A2a runtime profile 与 signer 已通过 SPEC/QUALITY 与 Root 验证；直接声明
`PyJWT>=2.14.0`、`cryptography>=50.0.1`，lock 为 `2.14.0`/`50.0.1`；没有读取或装配 private key、发布 JWKS、
查询 lease、修改 wire 或调用 Platform。

1. Agent machine artifact/signer/JWKS/run-scoped supplier 独立验收；supplier 单元/fake-client 证据不称为真实 Platform call 接线；
2. IAM ADR/API/安全设计先对齐 exact profile、数字矩阵与六段依赖，再实现 verifier/OpenAPI/generated SDK；
3. Platform owner 发布最终 compact-proof wire/request-binding/generated helper，不发布临时 wire 或 fallback；
4. Agent 固定 Platform artifact，在真实 Skills/MCP client 每个 owner call 边界完成 supplier 接线与 transmitted proof 验证；
5. Platform consumer 切 IAM SDK、删除旧手写 wire 并完成 fresh/completed-replay receipt 闭环；
6. Root 真实 Agent -> IAM -> Platform -> PostgreSQL receipt sandbox。

实现验收必须覆盖 canonical header/payload/signing-input/vector、malformed key/file权限/symlink、worker/HTTP descriptor 与各自 key 不一致、
JSON safe integer矩阵（2^53-1、2^53、2^53+1、0、负数、bool、float、无coercion）、多副本descriptor/ring正常与紧急rotation、
stale/paused/terminal/owner或generation变化、数据库连接排队跨 expiry、签后 lease race、clock skew、nonce唯一、
unknown kid/禁止URL header、无敏感日志、JWKS GET/HEAD/400/404/405/503/no-store、OpenAPI/provenance drift，以及真实 IAM verifier消费。
此设计门通过不等于上述能力已经实现。

## Execution proof A2b current candidate (2026-09-12)

The working implementation now contains the isolated private Ed25519 loader, anchored strict public-ring/JWKS snapshot, HTTP-only configuration root, anonymous exact JWKS GET/HEAD handling, OpenAPI `1.1.0`, and direct HTTP provenance pin. `kokoro-agent-http` points only to `interfaces.http.main:main`; the legacy worker HTTP entry is removed. Invalid or missing public material degrades JWKS and authenticated readiness before dependencies while health stays 200.

A2a remains the accepted pure signer. There is still no production signing call: worker private-loader composition, A2c statement-time lease supplier, IAM verifier, Platform owner contract/call site, and real Agent→IAM→Platform transport remain incomplete.
