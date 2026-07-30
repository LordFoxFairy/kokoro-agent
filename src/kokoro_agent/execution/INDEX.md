---
architectureIndex: 1
rootId: agent.execution
owners:
  - "@LordFoxFairy"
---

# execution — 执行链路域（装配、编排、投影泵、事件发射、HITL 应用层）

## Responsibilities

单次 run 的执行核心：DeepAgents 装配收窄为窄端口、v3 四投影并发消费合流、
wire 事件唯一构造点（per-run 单调 index）、HITL 暂停帧构造与 resume 对齐。

## Non-responsibilities

本域不拥有会话、Site、计费、模型路由策略、浏览器投影或跨服务鉴权事实。

## Public boundary

- `build_agent.py`：`build_agent(**deps) → InvokableAgent`。包住 `create_deep_agent`
  的未解泛型（object 边界 + TypeGuard 一次收窄），state_schema=KokoroAgentState。
- `run_agent.py`：`invoke_once(emitter, agent, thread_id, payload, ...) → bool`。
  run.started（仅 index==0）→ pump_run → interrupt 则发 generic awaiting 或专用 plan.proposed
  owner，并入账用量返 False；
  否则 claim_terminal 原子认领后发 run.completed/failed 返 True。recursion_limit 熔断失控循环。
- `events.py`：`RunEmitter`（一次 run 的唯一发射口；`attach()` 从流重建 index 续段与
  tool_id→segment 归属；审核工具 raw returned 按名抑制）；`AgentEventPayload` 联合；
  投影→payload 映射函数族（`tool_returned_payload` 等）；`failure_code`/`run_failed_payload`
  （三层错误语义）；`clip_result`/`TOOL_RESULT_MAX_CHARS`（wire 4000 字截断护栏）。
  R4：`CRITICAL_KINDS`/`TERMINAL_KINDS` + emitter 注入 outbox（RunLedger）——critical 帧经
  `stage_critical_frame` 分配 durable_seq/event_id、落 queued 行、发布后 published；live 序（index）
  不动、durable_seq 独立并行（浏览器面透明）；post-fence superseded 不发布、index 不前进。
  有 durable-output 映射的事件先以独立 output_seq/hash chain 落 Agent Mongo authority，再发布 live；
  append/replay conflict 统一抛 `DurableOutputCommitError`，该异常穿透 projection pump 并阻止成功终态；
  单纯 live bus publish 失败仍按事件隔离且不回滚 output。semantic 事件按语义身份，非 semantic 事件按 persisted live index
  派生 source identity；后者严格只保证 output append 成功、live publish 尚未成功这一崩溃窗口。
  同一事件的多条 output 在一个事务内连续分配，并持久化 batch cardinality/ordinal，重放增删、
  重排或 payload 漂移均 fail-closed，不重复、不产生半批 output。live 已发布后的 graph checkpoint
  replay 尚无 persisted projection-event identity，因此不宣称去重；扩展该保证前必须由 Root contract
  提供 `projection_event_ref`，不得用内容 hash 猜测（相同 delta 可能是合法重复文本）。
  `outbox_wire_event(frame)`：queued 行→补发 wire 帧（复用固定身份，幂等不漂移）。
  `plan.proposed` 额外以 `plan.proposed:<tool_call_id>` 作 semantic key；marker 不随 receipt GC 删除，
  同 key 同 payload 返回 duplicate/no-publish，同 key异 payload fail-loud。
- `publish_agent_events.py`：`pump_run(emitter, run, source_for)`。四路 typed 投影
  （messages/tool_calls/subagents/custom）并发抽干 → queue 合流 → 单点发布。
- `approvals.py`：HITL 权威唯一实现。`awaiting_payloads`（review/input/approval 三分支）、
  `plan_proposed_payload`（只读真实主图 interrupt，owner_ref=tool_call_id，拒 nested/mixed）、
  `approval_frame`/`review_frame`/`input_frame`/`PendingFrame`（nested=子代理内暂停合成帧）、
  `align_decisions`/`align_review_decisions`/`align_input_decisions`（缺/多/重复/越界 fail-loud）、
  `resume_command_decisions`/`submit_resume_value`/`review_resume_value`（契约词汇→框架词汇）、
  `resolution_payloads`/`review_resolution_payloads`/`nested_approved_payloads`（直发 returned）。
- `protocols.py`：LangGraph/DeepAgents 的窄 runtime_checkable 端口（`InvokableAgent`、
  `AgentRunStream`、`ModelStream`、`ToolCallView`、`SubagentRunStream`、`StateView`）——
  框架私有泛型止步于此。

## Callers and dependencies

- 上游调用：`worker/supervisor.py`（dispatch/resume/cancel 全经此域）、`agents/`（装配配方）。
- 下游依赖：`contract`（wire payload strict 模型）、`hitl`（HumanRequest 信封反解）、
  `streams/protocol`（StreamProtocol 发布）、deepagents/langgraph/langchain。

## Data ownership and events

本域构造 raw Agent wire events，并通过 ledger/outbox 管理 critical frame 交付状态；Session 拥有浏览器消息与投影。

## Runtime and security

- v3 四路投影必须并发消费：任一通道缓冲满会回压整图直至死锁；queue 只为合流保序。
- 终态发射前必经 `claim_terminal` 原子认领：cancel/自然完成/异常三路共用认领键，多 pod 恰好一个终态。
- emit 用 `exclude_none` 上 wire：null 会被 session 的 zod `.optional()` 拒收。
- 工具中途 interrupt 被 langgraph 浮现为 error=Interrupt repr：按前缀识别、抑制伪 returned。

## Idempotency, failure, and recovery

per-run index、durable sequence、terminal claim 与固定 event identity 共同处理重放、补发、竞态终态和 worker 恢复。

## Extension rules and forbidden dependencies

- 新 wire kind：contract 定 payload → events.py 扩联合与 `_KIND_BY_PAYLOAD` → 泵/映射函数。
- 新 HITL 形态优先表达为 `hitl.request_human` 的 kind 预设，awaiting/align 在 approvals.py 加分支。
- 计划 proposal 不走 todo.updated 推断；`propose_plan` 必须是主 agent 的唯一 tool call，模型响应
  守卫在 ToolNode/HITL 前 fail-loud，子代理不挂此工具。
- 不得绕过 RunEmitter 直接 publish 事件（index 单点递增是幂等链前提）。

## Current gotchas

四路投影必须并发抽干；任何串行消费都可能因框架缓冲回压而锁死整次执行。

## Verification

运行 `uv run pytest tests/test_invoke.py tests/test_deliver_event.py -q`、`uv run pyright` 与 `uv run ruff check src tests`。
