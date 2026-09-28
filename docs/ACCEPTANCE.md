# kokoro-agent 验收矩阵

## W1E Platform consumer 文档门与后续实现门

本片只验文档一致性与 `git diff --check`，不是 Agent→Platform transport 验收。实现前先固定
Platform main `ee25c1f4d6df08be183ca10f7f5e852e0b21f641` 原始 Proto/manifest/
schema/vector/provenance、IAM main `a4c2b61467f1fc1772d6b6d8e98f081c090289fb` 当前
verifier/ingress，以及 Agent 当前 `SkillClient`/`McpClient` 调用点；声明 typed ref 和 MCP
connector/connection/credential 映射缺口必须 owner-first 关闭。不得以文档或 fake-client 证据
声称可激活。

实现片的独立门禁：

1. Provenance：固定 repository/commit/path/direct SHA、`kokoro.platform.v1` descriptor 与
   31/24/15 manifest 的生成 drift；Python typed projector 跑 owner 全量 positive/negative vectors，
   对 method、字段、presence、request ID、raw bytes、Unicode、整数/set tamper 逐项拒绝。
   官方 Beta 候选 `connectrpc==0.12.1`、`protoc-gen-connectrpc==0.11.1`（Apache-2.0、
   Python ≥3.10）须先做固定 Proto SHA、隔离生成/wheel 与真实 Platform Express Connect/gRPC-Web
   互操作 spike；验证 async/per-call headers/timeout_ms/typed ConnectError、取消恢复、1 MiB/错误语义。
   Express 当前不支持原生 grpcio；Beta 候选验过再 pin lockfile，失败才另 ADR 比较限定
   Protobuf+HTTPX unary adapter。现有 `httpx` 不等于 Connect 实现，不手写未经验证的 framing。
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
