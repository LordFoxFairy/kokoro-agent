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
