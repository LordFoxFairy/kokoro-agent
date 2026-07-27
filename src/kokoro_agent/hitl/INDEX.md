---
architectureIndex: 1
rootId: agent.hitl
owners:
  - "@LordFoxFairy"
---

# hitl —— 通用人机暂停原语

## Responsibilities

一切人机暂停的统一抽象与原语。把"工具在任意执行点请求人"收敛为一个 `request_human`
调用（包装 langgraph 原生 interrupt/resume）；现状三场景（approval/question/review）是其预设形态。

## Non-responsibilities

本包不拥有浏览器交互、Session control receipt、权限策略或 Agent 执行编排。

## Public boundary

- `request_human(*, kind, request_id, schema=None, context=None) -> JsonValue`
  人机暂停原语。同步调用（与 `langgraph.interrupt` 一致，async 工具体内无需 await）。
  首跑挂起、resume 后原地返回回应值续跑。
- `request_input(*, request_id, schema=None, context=None) -> HumanInput`
  kind=input 的消费侧包装：request_human(kind="input") + jsonschema 校验 + 重问循环。
  submit 通过校验 → `InputSubmitted(value)`；reject → `InputRejected(reason)`；submit 不合法 →
  附 `validation_error` 原地重新 interrupt（同 request_id，人重填）。MCP elicitation 桥的消费点。
  resume 载荷形态 = `list[{request_id, type, value?/reason?}]`（supervisor 的 `submit_resume_value` 产出）。
- `HumanInput = InputSubmitted | InputRejected`：`request_input` 返回值的具名联合，消费方
  按分支穷尽；`InputSubmitted(value: dict[str, JsonValue])` 与 `InputRejected(reason: str | None)`
  都是 strict/frozen/extra=forbid 的边界模型。
- `HumanRequest`：统一载荷（`request_id` 幂等锚 / `kind` / `response_schema` / `context`）。
  `to_interrupt_value()` 生成 interrupt 信封，`from_interrupt_value()` 供投影层反解（非本信封返回 None）。
- `HumanKind = Literal["approval", "question", "review", "input"]`。
- 预设决策词汇：`APPROVAL_DECISIONS` / `QUESTION_DECISIONS` / `REVIEW_DECISIONS` / `INPUT_DECISIONS`。

## Callers and dependencies

- 下游消费：`tools/permissions.py`（interrupt_on 声明取 approval/question 决策集）、
  `tools/middleware.py`（`ToolResultReviewMiddleware` 经 `request_human(kind="review")` 发起审核暂停）、
  `mcp/tools.py`（`mcp_call` 把 MCP elicitation 桥为 `request_input`）、
  `execution/approvals.py`（`review_entries`/`input_entries` 经 `HumanRequest.from_interrupt_value` 反解信封投影 wire）。
- 上游依赖：`contract`（`AllowedDecision` 词汇）、`langgraph`（`interrupt`）、`jsonschema`（input 校验）。

## Data ownership and events

`HumanRequest` 只拥有 checkpoint 中的暂停信封；Session 拥有用户决策记录和浏览器可见状态。

## Runtime and security

- `request_human` 必须在带 checkpointer 的 langgraph 图执行上下文内调用（挂起点由 checkpoint 承载）。
- 本包是依赖叶子：只依赖 `contract` 与 `langgraph`，不得反向依赖 `execution` / `tools`。

## Idempotency, failure, and recovery

`request_id` 是同一暂停的幂等锚；无效 input 保持同一 request 原地重问，resume 由 checkpoint 恢复。

## Extension rules and forbidden dependencies

- 新暂停场景优先表达为 `request_human` 的 kind 预设，不新造独立 interrupt 载荷形态。
- wire 兼容期：新 kind 的 wire 投影是契约变更（`contract` 的 `AwaitingKind` 闭集），须走契约流程，
  不在本包私自扩 wire。

## Current gotchas

`request_human` 是同步 checkpoint 原语；async 工具内不得误加 `await` 或创建第二种暂停信封。

## Verification

运行 `uv run pytest tests/test_hitl.py tests/e2e/test_mcp_elicitation.py -q`、`uv run pyright` 与 `uv run ruff check src tests`。
