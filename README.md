# kokoro-agent

Kokoro 三仓里的**执行层**：DeepAgents + LangChain worker。消费 run 请求，跑 agent 循环，
产出**原始执行事件**（13 kind：text / tool / todo / subagent / thinking / run.*），带 per-run
单调 `seq` 与 `segment_id`，写入 redis run-events 流。**不面向浏览器**——只 kokoro-session 消费它。

> 全局架构与起栈见 [根 README](../README.md)。

## 分层（四层 DDD）

```
src/kokoro_agent/
├── domain/          纯实体/契约（agent_event.py 等），零框架依赖
├── application/     编排（run_agent：驱动 agent 循环 → 事件流），依赖抽象
├── infrastructure/  实现：chat_model / stream_translator / stream_port(redis) /
│                    builtin_tools / subagent_registry / local_fake_model
└── interfaces/      worker 入口
```

`domain/agent_event.py` 由 [`contract/generate.py`](../contract/events.yaml) **生成**（`DO NOT EDIT`）；改契约改根 `contract/events.yaml`。

## 运行

```bash
uv sync
# 本地假模型（凭据无关，离线可跑）：
KOKORO_STREAM_BACKEND=redis KOKORO_REDIS_URL=redis://127.0.0.1:6379/10 \
  KOKORO_LOCAL_FAKE_MODEL=1 uv run kokoro-agent-worker
```

接真实模型：去掉 `KOKORO_LOCAL_FAKE_MODEL`，配 `.env`（`KOKORO_MODEL` 如 `anthropic:claude-...` + provider 凭据）。

## 门禁

```bash
uv run pytest          # 单元 + 集成（含 redis 集成，redis 不可达则 skip）
uv run pyright         # 类型零错
uv run ruff check src tests
```

> 注：本仓走 aliyun 镜像，`uv run` 后 `uv.lock` 可能被改写——非依赖变更时 `git checkout uv.lock`；真依赖变更用 `UV_NO_CONFIG=1 uv lock`。

## 关键不变量

- `seq` per-run 单调；`run.completed`/`run.failed` 为终态。
- 内置工具 `now` / `fetch_url`（带 SSRF 防护 + 撞名守卫 + 字节/墙钟限流）。
- 异常 → `run.failed` 终态，worker 存活（不崩调度循环）。

测试用例总账见 [测试总目录](../docs/superpowers/specs/2026-06-13-test-case-catalog.md) §5。
