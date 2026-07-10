# hitl —— 通用人机暂停原语

## 职责

一切人机暂停的统一抽象与原语。把"工具在任意执行点请求人"收敛为一个 `request_human`
调用（包装 langgraph 原生 interrupt/resume）；现状三场景（approval/question/review）是其预设形态。

## 公开 API

- `request_human(*, kind, request_id, schema=None, context=None) -> JsonValue`
  人机暂停原语。同步调用（与 `langgraph.interrupt` 一致，async 工具体内无需 await）。
  首跑挂起、resume 后原地返回回应值续跑。
- `HumanRequest`：统一载荷（`request_id` 幂等锚 / `kind` / `response_schema` / `context`）。
  `to_interrupt_value()` 生成 interrupt 信封，`from_interrupt_value()` 供投影层反解（非本信封返回 None）。
- `HumanKind = Literal["approval", "question", "review", "input"]`。input 为 kind 预留位，H1 无生产调用方。
- 预设决策词汇：`APPROVAL_DECISIONS` / `QUESTION_DECISIONS` / `REVIEW_DECISIONS`。

## 关键协作者

- 下游消费：`tools/permissions.py`（interrupt_on 声明取 approval/question 决策集）、
  `tools/middleware.py`（`ToolResultReviewMiddleware` 经 `request_human(kind="review")` 发起审核暂停）、
  `execution/approvals.py`（`review_entries` 经 `HumanRequest.from_interrupt_value` 反解 review 信封投影 wire）。
- 上游依赖：`contract`（`AllowedDecision` 词汇）、`langgraph`（`interrupt`）。

## 运行时约束

- `request_human` 必须在带 checkpointer 的 langgraph 图执行上下文内调用（挂起点由 checkpoint 承载）。
- 本包是依赖叶子：只依赖 `contract` 与 `langgraph`，不得反向依赖 `execution` / `tools`。

## 扩展规则

- 新暂停场景优先表达为 `request_human` 的 kind 预设，不新造独立 interrupt 载荷形态。
- wire 兼容期：新 kind 的 wire 投影是契约变更（`contract` 的 `AwaitingKind` 闭集），须走契约流程，
  不在本包私自扩 wire。
