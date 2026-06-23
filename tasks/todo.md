# tasks/todo.md — ACL v2→v3 全量重写（方案 A + 守则D=D1）

**Goal**：把 kokoro-agent 的 ACL 消费层从 `astream_events(version="v2")` 扁平 dict 流，
全量重写到 v3 typed projections（`AsyncGraphRunStream`）。守则A(v3 强类型投影) +
守则C(彻底删 isinstance/as_mapping) + 守则D=get_stream_writer()(D1)。**不留旧代码、不留问题。**

**实证地基（已验，langchain-core 1.4.0 / langgraph，真实 deepagents agent）**：见 memory `kokoro-agent-v3-acl-goal`。
- `run = await agent.astream_events(payload, version="v3", config, transformers=[CustomTransformer])`
- `run.messages`→每模型 `AsyncChatModelStream`（默认迭代=分块 dict；`.text`/`.reasoning`/`.output_message`(AIMessage,带 usage_metadata)；`.namespace`/`.node` 给子代理归属）
- `run.tool_calls`→`ToolCallStream(tool_call_id, tool_name, input, output_deltas, output, error, completed, status)`
- `run.subagents`（deepagents `_subagent_factory`）/`run.custom`（CustomTransformer，承 `get_stream_writer()`）
- `await run.interrupted()`/`run.interrupts()`→`[Interrupt(value={action_requests,review_configs})]`；`await run.output()`；`aget_state` 仍可用；`Command(resume=)` 不变

**架构简化（v3 红利）**：`AsyncChatModelStream.namespace` 结构化携带子代理归属 → **删整个 `SubagentAttribution` 侧信道**。

---

## 步骤（TDD，逐步全工程门 mypy0/pyright0/pytest 绿）

- [ ] 1. `protocols/agent.py`：`InvokableAgent` 改 v3 契约——`astream_events(payload,*,version,config,transformers)`→`AsyncGraphRunStream`(用 `object` 窄返回 or langgraph 类型) + `aget_state`。
- [ ] 2. `projection/transformer.py`：重写为纯映射 typed 投影元素→AgentEvent。删尽 isinstance/as_mapping/TypeGuard。
       - `message_events(stream: AsyncChatModelStream)`→text_chunk（增量+final，subagent_id 来自 namespace）
       - `tool_call_events(tc: ToolCallStream)`→tool_call_start(input)→tool_call_end(output/error)；write_todos→agent_status{todo_updated}
       - `subagent_events(...)`→agent_status{subagent_started/finished}
       - `custom_event(payload)`→agent_status 或映射
       - `usage_of(stream)`→output_message.usage_metadata
- [ ] 3. `projection/awaiting.py`：从 `Interrupt.value.action_requests` 直接喂（不再 snapshot digging）；保留 fail-loud 对齐。
- [ ] 4. `run/invoke.py`：重写引擎——`async with run:` 并发消费投影→共享 asyncio.Queue→单 drainer 保序 publish；终态 output()/interrupts()/异常 agent_error；usage 聚合。
- [ ] 5. `infrastructure/agent_builder.py`：v3 返回型对齐 InvokableAgent；CustomTransformer 接线（invoke 传 transformers）。
- [ ] 6. 删 `projection/attribution.py` + 其 export/测试。
- [ ] 7. `run/supervisor.py`：terminal/resume 不变，仅适配 invoke_once 签名（若变）。
- [ ] 8. 测试全量重写：test_transformer / test_invoke / test_hitl_e2e / test_supervisor / test_awaiting；删 test_attribution。
- [ ] 9. 守则D 示例：给一个内置工具加 `get_stream_writer()` 埋点 + transformer custom 映射 + e2e 测试覆盖 run.custom→agent_status。
- [ ] 10. 全工程门 + import 冒烟 + rg 残留扫描（`version="v2"`/`SubagentAttribution`/`as_mapping` 清零）。

## 进度（2026-06-23）
- [x] 1. protocols/agent.py — v3 契约（AgentRunStream/SubagentRunStream/ModelStream/ToolCallView/InvokableAgent）。mypy0/pyright0。
- [x] 2. transformer.py — 纯映射重写，删尽 isinstance 路由（仅保 adapter 内归一化），tool_call 块不混入 text_chunk。
- [x] 3. awaiting.py — segment_id 改内部从 last_ai.id 推；action_requests 喂自 run.interrupts()。
- [x] 4. invoke.py — v3 递归引擎（async with run + 四投影并发→asyncio.Queue→单 drainer 保序；interrupt 经 run.interrupted()+aget_state；usage 聚合）。
- [x] 5. agent_builder/factory — invoke 传 transformers=[CustomTransformer]；返回型经 Any 视图收敛（无改动需要）。
- [x] 6. 删 attribution.py。
- [x] pyproject [tool.pyright] reportMissingTypeStubs=false（langgraph core stream/pregel 缺 pyright stub 的真实依赖缺口；mypy 仍全 strict）。
- [x] **e2e 冒烟通过**：真实 agent+LocalFake→invoke_once 产 started/todo_updated/tool_start/tool_end/text(delta+final)/done，无噪声。src 全 **mypy0/pyright0**。

## 剩余（全部完成 ✅ 2026-06-24）
- [x] 7. supervisor.py — `_has_pending_interrupt` 改 typed 一行 `bool(snapshot.interrupts)`；aget_state→StateView。
- [x] 8. 测试全重写：删 test_attribution；test_transformer(dataclass fake 窄 Protocol)；test_awaiting(typed ActionRequest)；test_invoke/test_supervisor/test_hitl_e2e(v3 fake run-stream + 真实 Interrupt/StateView)。
- [x] 9. 守则D e2e：tests/run/test_custom_event_e2e.py — 真实 agent + get_stream_writer 工具 → 断言 agent_status{status:custom} 全链路。
- [x] 10. 清理：删 specs.py 死的 agent_name 注入 + 删 test_agent_name_injection；残留扫描 v2/SubagentAttribution/as_mapping 清零。
- [x] **typed 收尾（用户反馈）**：interrupt 路径删光 `_is_object_mapping`/`_is_seq`/`_snapshot_messages`/`_request_args`/`_last_ai_message`，改用框架 typed `ActionRequest`/`Interrupt`/`StateView`(StateSnapshot) 直读。见 lessons L1。

## 终态门禁（2026-06-24，全绿）
- pytest: **198 passed**，无 skip/xfail
- mypy src/kokoro_agent: **0**
- pyright (全项目含 tests): **0**
- ruff: clean；import kokoro_agent: OK
- 跨仓 contract/session/web 对齐 = deferred（agent-only，用户后续单独做）。

## 关键事实（实测，详见 memory kokoro-agent-v3-acl-goal）
- 事件 data 形状保持与旧 wire 契约一致（跨仓对齐 deferred 不被打乱）。
- v3 ToolCallStream.tool_call_id = canonical AIMessage tool id（顺手解决 deferred 的 tool_id 关联 bug）。
- 子代理递归：run.subagents→AsyncSubagentRunStream（.trigger_call_id=子代理id, .name, .task_input, .messages/.tool_calls/.subagents 递归）。
- text_chunk 流式 delta type=text-delta，final type=text（web reducer 累积后 final 覆盖）。
