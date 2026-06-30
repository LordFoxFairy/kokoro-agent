# kokoro-agent

Kokoro 三仓里的**执行层**：DeepAgents + LangChain worker。消费 run 请求，跑 agent 循环，
产出**原始 AgentEvent**（`agent_status` / `text_chunk` / `reasoning_chunk` /
`tool_call_*` / `agent_done` / `agent_error`），带 `request_id`、`timestamp` 和
`data`，写入 redis run-events 流。**不面向浏览器**——只 kokoro-session 消费它。

> 全局架构与起栈见 [根 README](../README.md)。

## 分层（四层 DDD）

```text
src/kokoro_agent/
├── domain/          纯实体/配置对象，零框架依赖
├── application/     run supervisor / invoke / projection
├── infrastructure/  model / transport / checkpoint / tools / permission
└── interfaces/      worker 入口与 agent wire envelope
```

`interfaces/envelope.py` 是 agent 原始 wire 事件单源；
[`contract/generate.py`](../contract/generate.py) 会从它生成
`kokoro-session/src/domain/agent-event.ts`。AGUI/render 契约仍改根
`contract/events.yaml`。

## 运行

```bash
uv sync
# 本地假模型（凭据无关，离线可跑）：
KOKORO_STREAM_BACKEND=redis KOKORO_REDIS_URL=redis://127.0.0.1:6379/10 \
  KOKORO_LOCAL_FAKE_MODEL=1 uv run kokoro-agent-worker
```

接真实模型：去掉 `KOKORO_LOCAL_FAKE_MODEL`，配 `.env` 中的
`KOKORO_MODEL` 和 provider 凭据。

## 可观测性（Langfuse，opt-in）

agent 的 LLM/工具/子代理执行可经 [Langfuse](https://langfuse.com) 链路追踪。**完全 opt-in**：
不配置 env 即 tracing 关闭，行为零变化（离线/CI 不受影响）。

```bash
export LANGFUSE_PUBLIC_KEY=pk-...
export LANGFUSE_SECRET_KEY=sk-...
export LANGFUSE_HOST=https://cloud.langfuse.com   # 自托管改成你的地址（默认 cloud）
```

配齐后，每个 run 的 trace 自动带 `session_id`、执行风格 tag、
`kokoro_run_id` / `kokoro_conversation_id` 元数据。实现见
`infrastructure/observability.py` 和 `application/run_agent.py::trace_config`。

## 门禁

```bash
uv run pytest          # 单元 + 集成（含 redis 集成，redis 不可达则 skip）
uv run pyright         # 类型零错
uv run ruff check src tests
```

> 注：本仓走 aliyun 镜像，`uv run` 后 `uv.lock` 可能被改写。
> 非依赖变更时恢复 `uv.lock`；真依赖变更用 `UV_NO_CONFIG=1 uv lock`。

## 关键不变量

- agent 事件不暴露排序字段；Redis Stream cursor 只是 agent → session 的传输位点，session 落库后用
  `event_id` 做 SSE 幂等与续传锚点。
- agent raw 终态是 `agent_done`/`agent_error`；
  session 归一化后才是 `run.completed`/`run.failed`。
- 内置工具 `current_time` / `web_fetch` / `ask_user_question`。
  `web_fetch` 带 SSRF 防护、撞名守卫、字节/墙钟限流；`ask_user_question` 只通过 HITL
  `respond` 解决，不直接执行。
- 异常 → `agent_error`，session 归一化为 `run.failed`，worker 存活（不崩调度循环）。

测试用例总账见
[测试总目录](../docs/superpowers/specs/2026-06-13-test-case-catalog.md) §5。
