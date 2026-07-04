# kokoro-agent

Kokoro 三仓里的**执行层**：DeepAgents + LangChain worker。以 consumer-group 消费 run 请求流，
跑 agent 循环，产出契约事件（`run.* / message.* / thinking.* / tool.* / todo.* / subagent.*`，
共 14 kind），写入 per-run 事件流。**不面向浏览器**，只供 `kokoro-session` 消费。

> 全局架构与起栈见 [根 README](../README.md)；技术法律见根仓 `docs/kokoro-handbook/`；
> 协议单源见根仓 `contract/`（本仓 `src/kokoro_agent/contract/` 是生成镜像，勿手改）。

## 目录（按执行链路组织）

```
src/kokoro_agent/
├── contract/         ⚙ 生成物（DO NOT EDIT）：事件/控制/流名的唯一协议词汇
├── config.py         AppConfig：环境变量唯一解析点，仅 worker/main.py 调用一次
├── worker/           main=装配入口；supervisor=长驻调度（请求流消费、per-run control 流、
│                     租约心跳/过期重拾、暂停 run 的 control 监听收养）
├── execution/        单次 run 执行域：build_agent（DeepAgents 装配收窄为 InvokableAgent 端口）、
│                     run_agent（invoke/终态认领/recursion 熔断）、events（RunEmitter：index 单点
│                     递增、wire 截断、review 抑制）、approvals（HITL/审核帧构造与 resume 对齐）
├── run/context.py    RunContext：namespace/session/run/thread 身份，经 langgraph runtime context
│                     注入，工具/middleware 用 get_runtime 读（不进 checkpoint）
├── model/            chat model 工厂（openai/anthropic/DeepSeek 包装抽 reasoning）+ LocalFake
├── tools/            底层工具与治理：ask_user_question、memory（save/search，scope 装配注入）、
│                     web_fetch（SSRF 防御）、web_search（协议+provider 注册表同文件）、
│                     registry（名字治理）、permissions（interrupt_on 构造）、
│                     middleware（工具授权 fail-closed / 委派执法 / 结果审核）
├── skills/           SKILL.md lock 校验 + 全文渲染进 system prompt（backend 无关）
├── subagents/        目录（内建=空，原则：只收带真实工具的真能力；预设走 namespace wire）
├── mcp/              langchain-mcp-adapters 接入：白名单过滤 + mcp__{server}__{tool} 命名
├── sandbox/          执行 backend 工厂（state / local_shell；e2b 待落地 fail-loud）
├── streams/          StreamProtocol（cursor 不透明）+ redis（XADD maxlen、XREADGROUP/XACK、
│                     XAUTOCLAIM 死信收养）/ memory
├── storage/          RunStateStore（TTL 租约/暂停哨兵/终态原子认领/审核结果 keep-first，
│                     sqlite/mongo）+ checkpointer 工厂 + 记忆 store 工厂（随 checkpoint 对齐）
└── observability.py  Langfuse trace config（三 env 齐备才开，缺任一静默关闭）
```

## 运行

```bash
uv sync
# 本地假模型（凭据无关，离线可跑）：
KOKORO_STREAM_BACKEND=redis KOKORO_REDIS_URL=redis://127.0.0.1:6379/10 \
  KOKORO_LOCAL_FAKE_MODEL=1 uv run kokoro-agent-worker
```

接真实模型：去掉 `KOKORO_LOCAL_FAKE_MODEL`，`.env` 配 provider 凭据（见 `.env.example`，
含 reasoning 抽取/web 工具/Langfuse/熔断等全部开关的注释）。**模型档位、sandbox backend、
skills/MCP/子代理预设、权限**全部由 kokoro-session 的 namespace profile 决定并经 wire 下发
——agent 只消费 RuntimeConfig，不自造政策。

## 能力面（全部经 单测 → 跨栈 e2e → 真模型 验证）

- **HITL 双拦截**：工具前审批（approve/edit/reject）+ 工具后结果审核（approve/respond/reject，
  keep-first 缓存防双跑）；`ask_user_question` 恒为 respond 暂停点。
- **长期记忆**：`save_memory`/`search_memory`，store 前缀 =(namespace, "memories")，
  隔离政策装配注入，工具体零租户概念。
- **web 双件**：`web_fetch` 恒挂载（SSRF：DNS 解析后拒非公网/逐跳复检/15s/1MB/24k）；
  `web_search` 配置即挂载（tavily/searxng/zhipu 注册表，无 provider 不挂空壳）。
- **skills**：namespace/入口级挂载，lock（sha256）fail-closed，全文注入 prompt。
- **子代理**：wire 预设（tools 按名解析实例、model 工厂化，未知名 fail-loud）；
  委派三档 `subagent_create=deny|ask|allow`（deny 只放行声明集）。
- **thinking**：openai 兼容端点带 `reasoning_content` 时 `KOKORO_OPENAI_REASONING=1`
  切 DeepSeek 包装抽取，thinking.delta 全链上 wire。
- **韧性**：TTL 租约重拾、PEL 死信收养、暂停 run control 监听收养（worker 崩溃后 HITL
  不卡死）、`KOKORO_RECURSION_LIMIT` 失控熔断（默认 100）。

## 门禁与验证

```bash
uv run ruff check . && uv run pyright && uv run pytest   # 本仓三件套
python3 ../scripts/e2e-v21-gate.py        # 跨栈确定性门禁（LocalFake，30 项）
python3 ../scripts/chaos-verify.py        # 崩溃混沌：worker 收养 + session 恢复（11 项）
python3 ../scripts/trace-verify.py        # Langfuse HITL trace 连续性（7 项）
python3 ../scripts/real-model-verify.py   # 真模型五场景（thinking/subagent/search/skills/execute）
```

## 关键不变量

- wire 词汇 = `contract/` 生成物，一套 kind 从 agent 到像素同名同拼写；per-run 单调 `index`
  由 `execution/events.py` 的 RunEmitter 单点递增（event_id 幂等链根基）；
  契约 optional 字段缺席=省略（exclude_none），null 永不上 wire。
- 请求流 XREADGROUP 消费、parse 后即 XACK；崩溃恢复权在 RunStateStore TTL 租约，
  HITL 暂停置哨兵永不被重拾重跑，其 control 监听由存活 worker 心跳收养。
- claim-before-emit：cancel/自然完成/异常三路共用同一原子认领键，恰好一个终态事件。
- HITL 帧构造唯一在 `execution/approvals.py`：resume 按 tool_id fail-loud 对齐，
  `tool.awaiting_approval` 携带 `pending_tool_ids`（同帧凑齐才提交的契约依据）。
- 第三方类型豁免锁死于 `tests/test_boundary_pragmas.py` allowlist（现仅 2 处），
  行内 `type: ignore` 全仓为零（同测执法）。
- 异常 → `run.failed` 终态 fail-loud，worker 存活（单消息隔离，不崩调度循环）。

> 注：本仓走 aliyun 镜像，`uv run` 后 `uv.lock` 可能被改写——非依赖变更时 `git checkout uv.lock`；
> 真依赖变更用 `UV_NO_CONFIG=1 uv lock`。
