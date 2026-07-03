# kokoro-agent

Kokoro 三仓里的**执行层**：DeepAgents + LangChain worker。以 consumer-group 消费 run 请求流，
跑 agent 循环，产出契约事件（`run.* / text.* / thinking.* / tool.* / todo.* / subagent.*`，
共 14 kind），写入 per-run 事件流。**不面向浏览器**，只供 `kokoro-session` 消费。

> 全局架构与起栈见 [根 README](../README.md)。协议单源见根仓 `contract/`（生成物勿手改）。

## 目录

```
src/kokoro_agent/
├── contract/         ⚙ 生成物（DO NOT EDIT）：事件/控制/流名的唯一协议词汇
├── config.py         AppConfig：env 唯一解析点，仅 worker/main.py 调用
├── observability.py  Langfuse trace config（吃注入 settings）
├── worker/           main 入口装配 + supervisor 长驻调度（XREADGROUP + 租约心跳/重拾）
├── run/              builder / invoke / pump / emit / hitl / prompts：单次 run 执行域
├── ports.py          LangGraph/DeepAgents 窄 Protocol，私有泛型止步于此
├── model/            chat model factory + local fake（离线 e2e）
├── subagents.py      内建 + 配置自定义子代理目录，source 标签解析
├── tools/            ask_user + 工具名注册/interrupt_on 构造
├── sandbox.py        filesystem 权限 + 执行 backend 选择
├── streams/          StreamProtocol + memory/redis（XADD maxlen、XREADGROUP/XACK、断线退避）
└── storage/          RunStateStore（TTL 租约 + 终态原子认领，sqlite/mongo）+ checkpointer 工厂
```

目录按 Agent 执行链路组织，不使用 DDD 四层模板。DeepAgents 是执行底座，可以 import，
但不是 Kokoro 的目录语言。

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

不配置 env 即 tracing 关闭，行为零变化（离线/CI 不受影响）。

```bash
export LANGFUSE_PUBLIC_KEY=pk-...
export LANGFUSE_SECRET_KEY=sk-...
export LANGFUSE_HOST=https://cloud.langfuse.com   # 自托管改成你的地址（默认 cloud）
```

配齐后，每个 run 的 trace 自动带：`session_id`、tag（执行风格 fast/thinking）、
`kokoro_run_id` / `kokoro_conversation_id` 元数据。实现见 `observability.py`。

## 门禁

```bash
uv run pytest          # 单元 + 集成（redis/mongo 不可达则显式 skip）
uv run pyright         # 类型零错
uv run ruff check src tests
```

> 注：本仓走 aliyun 镜像，`uv run` 后 `uv.lock` 可能被改写——非依赖变更时 `git checkout uv.lock`；真依赖变更用 `UV_NO_CONFIG=1 uv lock`。

## 关键不变量

- wire 词汇 = `contract/` 生成物，一套 kind 从 agent 到像素同名同拼写；agent 信封含 per-run
  单调 `index`（`run/emit.py` 单点递增），event_id 幂等链的根基。
- 请求流经 XREADGROUP 消费、parse 后 XACK；崩溃恢复权在 RunStateStore 的 TTL 租约
  （`try_claim`/`renew`/`reclaim_expired`），HITL 暂停置哨兵永不被重拾重跑。
- claim-before-emit：cancel/自然完成/异常三路共用同一原子认领键，恰好一个终态事件。
- HITL 权威唯一在 `run/hitl.py`：pending 集合、resume 按 tool_id fail-loud 对齐、
  reject/respond 快照直发；`tool.awaiting_approval` 携带 `pending_tool_ids`（同帧完整待批集合）。
- 终态 = `run.completed{status}` / `run.failed`；cancel 补发 `run.completed{status:cancelled}`。
- 默认 Kokoro 自有工具表只有 `ask_user`；`write_todos`、`task`、`execute` 等来自 DeepAgents，
  `execute` 在 default 权限档默认进入 HITL。
- `skills` / `memory` / `backend` 只走 DeepAgents 原生参数；未知 backend/枚举 fail loud。
- 异常 → `run.failed` 终态，worker 存活（单消息隔离，不崩调度循环）。
