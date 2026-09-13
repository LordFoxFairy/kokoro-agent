# kokoro-agent 验收矩阵

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
