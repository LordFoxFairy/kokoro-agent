# kokoro-agent 验收矩阵

## W3 typed Skill reader 当前候选验收（2026-09-29）

基线`dd34a4800b4ce0cc61eb80dd715e528b9d4517da`，完整reader已实施，待Root独立审查/固定SHA复验。
RED→GREEN包括v4 owner pin、29ZIP profile向量、GET边界/日志/显式:0与timeout、只读路径与批输出预算；
typed reader重新授权/撤权、binary/nonUTF8、exact path、空选择零依赖、factory模型前preflight已覆盖。
新owned loopback验证真实GET/取消；真实worker signer/OAuth+fake Platform/object响应证明fresh JTI/lease拒绝，非真实owner授权组合。
全门的最终实测数量由本节收尾记录；下文旧候选数量只是历史，不代替当前SHA。共享PG/Redis与3310未操作，
v4激活、安装/启用产品链、Storage退役仍后续owner，不能将本片单仓门称完整产品闭环。

### 本候选实际门禁（基线dd34a48，2026-09-29）

- `uv lock --check` / `uv sync --frozen`：132 resolved / 128 audited，通过；锁文件未变。
- `uv run ruff format --check .`：248 files already formatted；`uv run ruff check .`：通过。
- `uv run pyright`：0 errors / 0 warnings；`uv run kokoro-agent-contract-check`：通过，含29个owner ZIP向量。
- `uv run python scripts/generate_platform_consumer.py --check`：隔离Python3.11生成无漂移。
- `uv run pytest -q -rs`：**1399 passed / 6 skipped / 172 deselected**，56.36s，364条上游beta/deprecation警告。
  1 skip为parent-repo examples缺失，5 skip为MinIO9100未配置；172为默认排除的外部integration/acceptance/e2e，未放宽门禁。
- `uv run pytest -q tests/integration/skills/test_backend.py -o addopts=''`：3 passed；此文件是本地DeepAgents组件测试，非真实owner集成。
- `uv build --wheel --sdist`：两种产物通过；`uv run python scripts/check_platform_wheel.py dist/kokoro_agent-2.0.0-py3-none-any.whl`：隔离Python3.11生成消费smoke通过；wheel含两个新增reader模块。
- `git diff --check`：通过；构建产生的本次`build/`副本已清理，无锁/SQL/其他仓修改。

日志：`/tmp/agent-reader-final-pytest.log`、`/tmp/agent-reader-final-pyright.log`、`/tmp/agent-reader-final-contract.log`、
`/tmp/agent-reader-generation-check.log`、`/tmp/agent-reader-build.log`、`/tmp/agent-reader-wheel.log`。
独立审查、Root固定SHA复跑与真实IAM/Platform/Storage组合仍待Root负责；未启动/重置共享PG/Redis或3310。

## 历史设计验收矩阵：W3 typed Skill source（2026-09-29）

Agent launch 契约/Run fence 的 RED→GREEN 单元/契约片已完成，真实 PostgreSQL/Redis roundtrip 待验；下表 Platform/包读取/正式产品链各项仍是目标，不是通过记录。

**本次只有设计文档，以下均为待验收条件，不是通过记录。** 实施顺序是 Agent launch 契约/Run fence → Platform v4 owner 原字节 pin/生成与六 RPC 兼容证明 → Skill Resolve/Get adapter → Storage signed GET/ZIP → 只读 backend/删除 name 双轨 → BFF consumer/用户安装选择 → Root 真 owner 组合。Platform `6a09913` v4 inactive，Agent 当前 v3 `5b6eb2c` 不能视为 transfer reference 已可用；Storage 固定 `16a6c1c` v2。Platform `InstallSkill` 具名 RPC 已在 owner Proto，但 BFF 面向用户的安装/启用和个人 Skill 选择未接，个人 ACTIVE 发布列表不等于 installed+enabled。

| RED 起点（先证旧行为失败） | GREEN 与负例门 |
| --- | --- |
| 旧 launch/RunRequest 不接受 typed refs，name `music` 不在持久 fence | 本仓 OpenAPI、Pydantic、Redis roundtrip、两 Run 表 canonical JSON 同值；exact `skill:<id>`，最多 16/4 KiB、空数组，重项/错 wrapper/超限 400；同 `run_id` 换 refs/tenant/subject 409；并发 claim/resume/lease takeover 不从当前 Feature 或另一个请求替换选择。 |
| Agent v3 generated 没有完整 GET transfer | 固定 Platform v4 Proto/manifest/ZIP profile 原字节与 digest/生成 drift；断言 `read_reference` tag 4 reserved、`transfer_reference` tag 5 全字段；旧 v3/裸 URL 不可在 production client fallback；24 个 proof binding 与 Agent 六操作逐条投影同值。 |
| name/scope/hash Skill client 和 `/.skills/` 缓存 | 正常 exact ref→Resolve→GetApproved→签名 GET→只读 `SKILL.md`；同 display name、不同 exact SkillId/同 series 不同版本并列时，`/.skills/<无填充 base64url(SkillId 原始 ASCII)>/` 一一映射且内容绝不覆盖；同 ref 跨重试/lease 路径稳定，191-byte SkillId 的单段上限为 255，错误大小写/非 canonical padding/URL 解码 alias/恶意相对 entry 不可穿越根目录；跨 tenant/subject、非 active、未安装/disabled、withdrawn、错 ref/response asset/digest/manifest、撤权后的二次读取和旧缓存均拒绝；无 declared ref 的基础 Chat 不取 IAM token/Platform/Storage。 |
| 无真正包体边界 | 签名 GET 只向批准 origin 以 method GET/owner headers 发、无 Cookie/Bearer/redirect/userinfo/fragment；200、未来 expiry、identity encoding、总/idle deadline、32 MiB 压缩、SHA-256，超限/截断/错摘要/坏 header/非允许 host/过期一律拒绝。按 v4 ZIP profile 验 entry/展开/manifest/路径/CRC/ZIP32 与 exact skill_id/revision/manifest identity；拒绝 zip bomb、遍历、符号链接、重复/同名前缀、BOM、未知 manifest 键，不向 backend 暴露部分包。 |
| ACK/取消/lease/scan 失败会落入旧回空或缓存 | Platform RPC ACK 未知只同 frozen ref/logical request_id 重读且逐次 fresh proof；GET 断线/URL 过期重新向 Platform 取当次签名；pending/unknown/infected scan 或 owner 5xx/429/timeout 与取消 fail closed；取消不重试，旧 lease 不发请求；Tool/模型继续之前必须有完整有效 Skill 或既有 Run failed 事实，零额外外部副作用。 |
| BFF personal 发布列表只有 ACTIVE 可见 | 真 IAM→BFF 用户安装/enable→Agent launch typed ref→Platform current Source→Storage CLEAN→Agent leased Run 读包；卸载/撤权/感染/跨用户/跨 tenant 与完成 receipt replay 均在再次读包前拒绝；不能用 BFF 列表、假 client 或静态 Playwright 代替。 |

下一片目标文件集：现有 `src/kokoro_agent/protocol/control.py`、`interfaces/http/ingress.py`、`agents/definition.py`/`agents/music.py`、`agent_factory.py`、`clients/skills.py`、`skills/backend.py`、`worker/platform.py`/`worker/dependencies.py` 与已有 Run repository JSON 路径；`contract/openapi/v1/openapi.json`、`contract/platform/v1/`、`generated/`、`contract/provenance.json` 仅在各自 owner 源固定/可再生时更新。测试沿已有 `tests/unit`、`tests/contract`（含 `test_architecture.py`）、`tests/integration` 职责放置，不为此创建第二套 `services/ports`。实现片须 `uv lock --check`、`uv sync --frozen`、Ruff format/check、Pyright、默认 pytest、contract checker、生成 drift、wheel/sdist；真实隔离 PostgreSQL/Redis 与 IAM/Platform/Storage/ObjectStore sandbox 另门验。测试不启动或清理用户 3310/共享服务，不把 CORS 501 当 Agent Python 包读取的完成或失败证据。

## W1E Platform consumer 生成/projector 门与后续 runtime 门

当前切片固定 owner artifact、生成 client/projector 与离线 contract；它不是 Agent→Platform transport 验收。已固定
Platform main `ee25c1f4d6df08be183ca10f7f5e852e0b21f641` 原始 Proto/manifest/
schema/vector/provenance、IAM main `a4c2b61467f1fc1772d6b6d8e98f081c090289fb` 当前
verifier/ingress，以及 Agent 当前 `SkillClient`/`McpClient` 调用点；声明 typed ref 和 MCP
connector/connection/credential 映射缺口必须 owner-first 关闭。不得以文档或 fake-client 证据
声称可激活。

独立门禁：

1. Provenance：代码内固定 repository/commit 及 exact 14 条
   consumer path/owner path/direct SHA（含 owner provenance raw SHA），再校验 13-payload
   aggregate、`kokoro.platform.v1` descriptor 与 31/24/15 manifest 生成 drift。build-time artifact
   checker 验证 owner 24 positive、134 projected-JSON binding negative 与 7 raw-parser negative
   vectors；runtime typed projector 另以 constructor/setattr permissive matrix 拒绝错 scalar、错
   protobuf message class 及六类实际 ID wrapper 交叉注入，不把两层证据混称。
   官方 Beta 候选 `connectrpc==0.12.1`、`protoc-gen-connectrpc==0.11.1`（Apache-2.0、
   Python ≥3.10）须先做固定 Proto SHA、隔离生成/wheel 与真实 Platform Express Connect/gRPC-Web
   互操作 spike；验证 async/per-call headers/timeout_ms/typed ConnectError、取消恢复、1 MiB/错误语义。
   Express 当前不支持原生 grpcio；Beta 候选验过再 pin lockfile，失败才另 ADR 比较限定
   Protobuf+HTTPX unary adapter。现有 `httpx` 不等于 Connect 实现，不手写未经验证的 framing。此项
   的固定生成、wheel import 和 owned Express loopback 已通过；正式 worker adapter/cancel 仍属后续门。
2. Skill：typed source ref 正常解析与包引用/Storage digest；name-only/错 typed ID、停用/移除/
   跨 tenant、owner 不可用、包扫描变化均在包读取前拒绝，不以空 Skill 列表继续已声明能力。
3. MCP：typed connector/connection 与 server/selector 边界；每次 `mcp_call` 新 proof，
   raw typed arguments/approval/idempotency binding 正反例；授权拒绝、撤权、禁用、重放/重试
   先于远端 MCP socket/provider 副作用；本地部署 YAML 不能替代 Platform current decision。
4. Worker：private-key/credential-file owner-only、缺失/权限/descriptor 错误在消费前 fail closed；
   HTTP public ring 不加载 private material；tenant-machine token audience/scope/caller/current
   generation、A→B→C 轮换、late exchange 丢弃、no stale-on-error 与 readback barrier。
5. 真组合：test-owned 同一 PostgreSQL 实例的 Agent/Platform 独立 schema、隔离 Redis、真实 IAM
   client-credentials/JWKS/Platform Connect、Storage package 与 MCP stub；每次 send DB-clock
   lease/fresh JTI/真实 transmitted bytes；过期、paused、terminal、generation takeover、
   query/连接排队跨 expiry、签后 race、IAM/Platform 401/403/429/503、网络 timeout/cancel、
   completed receipt replay 撤权均有零下游副作用/正确 Run 终态和资源清理证据。

实现完成后仍须运行 `uv lock --check`、`uv sync --frozen`、`uv run ruff format --check .`、
`uv run ruff check .`、`uv run pyright`、`uv run pytest`、`uv run kokoro-agent-contract-check`、
`uv build --wheel --sdist` 与真实依赖完整门；Root 在主仓固定六 owner commit 的 sandbox
重新验证后才可称 consumer/activation 通过。用户 3310 与 Billing 不在本片范围。

## 快速门禁

```bash
uv sync --frozen
uv run ruff check src tests
uv run pyright
uv run pytest -q
uv run kokoro-agent-contract-check
uv build --wheel --sdist
```

## 真实依赖门禁

先复用 Root 提供的单个 PostgreSQL 和单个 Redis，不重复启动容器；将连接串注入后执行：

```bash
KOKORO_AGENT_DATABASE_URL=TARGET \
KOKORO_REDIS_URL=TARGET \
uv run pytest -q -o addopts='' -m 'integration or acceptance'
```

覆盖：canonical schema fresh install、tenant scope、dispatch recovery、同 owner generation fencing、control
幂等、outbox replay、HITL resume、chat history/replay、health/ready 和真实 HTTP ingress。

## Contract/schema 门禁

```bash
test ! -d database/migrations
! grep -RInE 'FOREIGN[[:space:]]+KEY|REFERENCES|schema_migrations' database src scripts
uv run kokoro-agent-contract-check
```

## 发布候选

构建候选镜像后执行漏洞扫描、非 root/healthcheck 检查、health/ready smoke、SBOM/provenance 生成和 digest
签名；任何失败都不能进入 push 阶段。验证报告必须记录 Git commit、命令、依赖版本和失败项。

## G6-Agent System 消费接线

基线70a38138f42f29e8a482fde7890fe0e2d0c27e34；Root唯一writer。已先完成TECH/API/DATA窄设计门，既有contract checker通过。
测试先RED缺client/mapper/worker注入，再实现边界和实际Factory接线；目标测试命令：

```bash
uv run --frozen --no-sync pytest -q tests/unit/model/test_system_client.py tests/unit/model/test_model_selection.py tests/unit/agents/test_factory.py tests/unit/worker/test_dependencies.py
```

Root 主工作树实跑证据（2026-09-08）：

- `uv lock --check`、`uv sync --frozen`：通过，127 resolved / 123 audited；没有升级锁文件。
- 上述 focused 命令：38 passed；补 UUID version/variant 和 revision safe-integer 上界时先 3 failed / 21 passed，再全部通过。
- `uv run ruff check .`、`uv run pyright`、`uv run kokoro-agent-contract-check`：全部通过，Pyright 0 errors / 0 warnings。
- `uv run pytest -q`：611 passed、6 skipped、77 deselected、66 上游 warning；不是完整外部依赖集成通过。
- `uv build`：wheel/sdist 通过；wheel 已确认包含 `kokoro_agent/clients/system.py`。
- 本切片10个 Python 文件 `ruff format --check` 全通过；全仓 format 仍有80个未触碰旧文件失败。
  Root 用 `git archive HEAD` 隔离副本复现基线81个失败，未为本切片批量格式化其他文件。
- 独立只读 system_capability_review 审查无 P1/P2；两个非阻断契约边界已追加 RED/GREEN 修正。
- `git diff --check`：通过；Root 串行提交，实际 SHA 由交付报告和 System 唯一任务表记录。

live System / LiteLLM smoke 随 System G5/G6 验收；不会把 HTTPX MockTransport 称为真实 owner 集成。

### A2b execution-proof acceptance

Acceptance requires strict private/public file matrices (including nonblocking FIFO and fd closure), RFC 7638 A1 KAT, immutable/degraded snapshots, raw-socket JWKS method/framing/header precedence, no auth/dependency calls for JWKS, readiness-before-dependencies, health continuity, OpenAPI/provenance mutation gates, separated installed entrypoint smoke, full default tests, and existing real PostgreSQL/Redis ingress regression. SIGINT/SIGTERM must stop new business admission, report readiness as draining, let each active handler drain within the fixed bound, preserve any declared response that completes inside it, and release the port; active handler 在固定 2 秒 deadline 内 drain，但 Python threads are not claimed to be forcibly cancellable after timeout. A2b does not count A2c supplier, IAM/Platform verification, or real proof transport as complete. A2c acceptance separately requires exact pair snapshots, integer-only time math, per-call fresh read/JTI/sign, controlled cancellation/deadline gates, and a real PostgreSQL ACCESS EXCLUSIVE cross-expiry race; it still does not count production composition or transport as complete.

## W1E authenticated transport 候选验收（2026-09-28）

基线 `cf3d9ef103b5f1c3c005ad8fb45862ba83f9a0f8`，工作树候选，Root 审查与提交仍待完成。

- RED→GREEN：credential 文件/singleflight/rotation/cancel；declared Skill outage；多 peer sandbox-before-preflight；
  sender fresh proof/headers；聚合前 1 MiB streaming limit；secret exception context。
- `uv lock --check`、`uv run ruff format --check .`、`uv run ruff check .`、`uv run pyright`、
  `uv run kokoro-agent-contract-check`、`uv run python scripts/generate_platform_consumer.py --check`、
  `uv build --wheel --sdist`、既有 fresh Python 3.11 generated-wheel gate 已执行通过；最终全 pytest **1230 passed / 6 skipped / 166 deselected**。
- `tests/contract/test_platform_transport_http.py`：5 个 owned loopback HTTP 测试通过，验证实际 OAuth Basic/form、
  Connect protobuf/tag100/Bearer、真实签名/binding、fresh JTI、302/1 MiB/deadline/cancel。服务为 fixture，非真 owner。
- 同文件 `test_real_postgres_lease_with_worker_owned_http`：设置独立测试 admin URL，以随机数据库安装 canonical schema，
  真 repository claim→worker原样 signer/lease reader→重复 send→pause 拒绝；1 passed，自有数据库清理完成。
- fresh Python 3.11 wheel 按 frozen runtime requirements 安装，credential/token/Connect/worker 模块从 wheel import 通过。

未验且阻断激活：production IAM token HTTP→production Platform Connect 的当前授权/rotation/revoke 矩阵；typed Skill/MCP
产品选择与持久 Run fence、Storage v2 包体、认证 MCP 执行凭据、BFF/Web 传递。库存在此片保持 broken；
不启动用户 3310，不用本地测试 fixture 冒充 production owner，也不把生成 artifact inactive 状态改为 active。
