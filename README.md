# kokoro-agent

Kokoro 三仓里的**执行层**：DeepAgents + LangChain worker。消费 run 请求，跑 agent 循环，
产出**原始执行事件**（text / tool / todo / subagent / thinking / run.*），写入 redis run-events 流。
**不面向浏览器**，只供 `kokoro-session` 消费。

> 全局架构与起栈见 [根 README](../README.md)。

## 目录

```
src/kokoro_agent/
├── worker/       worker 入口与 Redis wire message
├── run/          RunRequest、RunContext、Capabilities、生命周期
├── execution/    创建 DeepAgents graph、执行、resume、HITL、RawAgentEvent 输出
├── tools/        Kokoro 自有工具集合、工具权限、ask_user
├── subagents/    本次 run 可用子代理定义
├── skills/       已授权 skill mounts
├── mcp/          已授权 MCP server/tool
├── sandbox/      backend 和执行环境策略
├── storage/      checkpoint、memory、run lease
├── streams/      Redis/memory stream
└── model/        chat model factory 和 local fake
```

目录按 Agent 执行链路组织，不使用 DDD 四层模板，也不使用 `deepagents/`、`runtime/`、`adapters/`
作为目录名。DeepAgents 是执行底座，可以 import，但不是 Kokoro 的目录语言。

## 运行

```bash
uv sync
# 本地假模型（凭据无关，离线可跑）：
KOKORO_STREAM_BACKEND=redis KOKORO_REDIS_URL=redis://127.0.0.1:6379/10 \
  KOKORO_LOCAL_FAKE_MODEL=1 uv run kokoro-agent-worker
```

接真实模型：去掉 `KOKORO_LOCAL_FAKE_MODEL`，配 `.env`（`KOKORO_MODEL` 如 `anthropic:claude-...` + provider 凭据）。

## Runtime capability

默认 runtime backend 是 DeepAgents 的 `state`，不启用宿主机 shell。需要本地开发型 shell/backend 时显式开启：

```bash
KOKORO_AGENT_BACKEND=local_shell \
KOKORO_AGENT_LOCAL_SHELL_ROOT=/path/to/workdir \
KOKORO_AGENT_LOCAL_SHELL_INHERIT_ENV=0 \
uv run kokoro-agent-worker
```

DeepAgents 原生 skills / memory 通过逗号分隔路径配置：

```bash
KOKORO_AGENT_SKILLS=/skills/user,/skills/project
KOKORO_AGENT_MEMORY=/memory/AGENTS.md
```

`local_shell` 是宿主机执行能力，只适合本地开发或受控 CI；生产、多租户、用户输入不可信场景必须使用隔离 backend。

## 可观测性（Langfuse，opt-in）

agent 的 LLM/工具/子代理执行可经 [Langfuse](https://langfuse.com) 链路追踪。**完全 opt-in**：
不配置 env 即 tracing 关闭，行为零变化（离线/CI 不受影响）。

```bash
export LANGFUSE_PUBLIC_KEY=pk-...
export LANGFUSE_SECRET_KEY=sk-...
export LANGFUSE_HOST=https://cloud.langfuse.com   # 自托管改成你的地址（默认 cloud）
```

配齐后，每个 run 的 trace 自动带：`session_id`（= 会话 id，归组多轮）、tag（执行风格 fast/thinking）、
`kokoro_run_id` / `kokoro_conversation_id` 元数据。实现见 `observability.py`。

## 门禁

```bash
uv run pytest          # 单元 + 集成（含 redis 集成，redis 不可达则 skip）
uv run pyright         # 类型零错
uv run ruff check src tests
```

> 注：本仓走 aliyun 镜像，`uv run` 后 `uv.lock` 可能被改写——非依赖变更时 `git checkout uv.lock`；真依赖变更用 `UV_NO_CONFIG=1 uv lock`。

## 关键不变量

- Agent raw event 不负责浏览器排序、cursor 或 replay；这些由 `kokoro-session` 持久化后处理。
- 默认不注入自研时间、网页抓取、网页搜索工具；工具能力由 DeepAgents/LangChain/MCP 或运行配置装配。
- 默认 Kokoro 自有工具表为空；`write_todos`、`task`、`execute` 等来自 DeepAgents，`execute` 在 default 权限档默认进入 HITL。
- 子代理只能来自内建或配置声明目录，不允许模型运行时提交 system prompt 创建新子代理。
- `skills` / `memory` / `backend` 只走 DeepAgents 原生参数；未知 backend fail loud。
- 异常 → `run.failed` 终态，worker 存活（不崩调度循环）。

测试用例总账见 [测试总目录](../docs/superpowers/specs/2026-06-13-test-case-catalog.md) §5。
