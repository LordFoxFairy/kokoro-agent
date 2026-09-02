# execution — Run 执行链路（投影泵、事件发射、HITL、终态）

## 职责

单次 Run 的执行核心：消费 DeepAgents 原生调用结果、v3 四投影并发消费合流、
wire 事件唯一构造点（per-run 单调 index）、HITL 暂停帧构造与 resume 对齐。

## 公开 API

- Agent 的创建不属于本目录；由 `agent_factory.py` 接线
  DeepAgents `create_deep_agent`。本目录只接收返回的 native runnable，不定义第二套 runtime。
- `run_agent.py`：`invoke_once(emitter, agent, thread_id, payload, ...) → bool`。
  run.started（仅 index==0）→ pump_run → interrupt 则发 awaiting 并入账用量返 False；
  否则 claim_terminal 原子认领后发 run.completed/failed 返 True。recursion_limit 熔断失控循环。
- `events.py`：`RunEmitter`（一次 run 的唯一发射口；`attach()` 从流重建 index 续段与
  tool_id→segment 归属；审核工具 raw returned 按名抑制）；`AgentEventPayload` 联合；
  投影→payload 映射函数族（`tool_returned_payload` 等）；`failure_code`/`run_failed_payload`
  （三层错误语义）；`clip_result`/`TOOL_RESULT_MAX_CHARS`（wire 4000 字截断护栏）。
  R4：`CRITICAL_KINDS`/`TERMINAL_KINDS` + emitter 注入 outbox（RunRepository）——critical 帧经
  `stage_critical_frame` 分配 durable_seq/event_id、落 queued 行、发布后 published；live 序（index）
  不动、durable_seq 独立并行（浏览器面透明）；post-fence superseded 不发布、index 不前进。
  `outbox_wire_event(frame)`：queued 行→补发 wire 帧（复用固定身份，幂等不漂移）。
  安全产品投影另经 `chat/projection.py` 写入 `chat_events`；thinking、
  tool args/results、subagent 内文与内部错误不会进入聊天事实。
- `publish_agent_events.py`：`pump_run(emitter, run, source_for)`。四路 typed 投影
  （messages/tool_calls/subagents/custom）并发抽干 → queue 合流 → 单点发布。
- `approvals.py`：HITL 权威唯一实现。`awaiting_payloads`（review/input/approval 三分支）、
  `approval_frame`/`review_frame`/`input_frame`/`PendingFrame`（nested=子代理内暂停合成帧）、
  `align_decisions`/`align_review_decisions`/`align_input_decisions`（缺/多/重复/越界 fail-loud）、
  `resume_command_decisions`/`submit_resume_value`/`review_resume_value`（契约词汇→框架词汇）、
  `resolution_payloads`/`review_resolution_payloads`/`nested_approved_payloads`（直发 returned）。
- `protocols.py`：LangGraph/DeepAgents 的窄调用契约（`AgentRunnable`、
  `AgentRunStream`、`ModelStream`、`ToolCallView`、`SubagentRunStream`、`NativeStateSnapshot`）——
  框架私有泛型止步于此。

## 关键协作者

- 上游调用：`worker/supervisor.py`（dispatch/resume/cancel 全经此域）、`agents/`（装配配方）。
- 下游依赖：`contract`（wire payload strict 模型）、`hitl`（HumanRequest 信封反解）、
  `streams/protocol`（StreamProtocol 发布）、deepagents/langgraph/langchain。

## 运行时约束

- v3 四路投影必须并发消费：任一通道缓冲满会回压整图直至死锁；queue 只为合流保序。
- 终态发射前必经 `claim_terminal` 原子认领：cancel/自然完成/异常三路共用认领键，多 pod 恰好一个终态。
- emit 用 `exclude_none` 上 wire：null 会被 BFF Chat 的契约校验拒收。
- 工具中途 interrupt 被 langgraph 浮现为 error=Interrupt repr：按前缀识别、抑制伪 returned。

## 扩展规则

- 新 wire kind：contract 定 payload → events.py 扩联合与 `_KIND_BY_PAYLOAD` → 泵/映射函数。
- 新 HITL 形态优先表达为 `hitl.request_human` 的 kind 预设，awaiting/align 在 approvals.py 加分支。
- 不得绕过 RunEmitter 直接 publish 事件（index 单点递增是幂等链前提）。
