"""工具策略中间件：未授权 fail-closed 拒绝，授权放行并审计。"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse, ToolCallRequest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.errors import GraphInterrupt
from langgraph.types import Command
from pydantic import BaseModel, ConfigDict, TypeAdapter

from langchain.agents.middleware.types import AgentState
from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from kokoro_agent import metrics
from kokoro_agent.hitl import request_human
from kokoro_agent.persistence.repository import RunRepository
from kokoro_agent.tools.registry import JOURNAL_EXEMPT_TOOLS, SUBAGENT_TOOL_NAME

_logger = logging.getLogger(__name__)

# Command[Any] 对齐框架基类签名：其类型参数是运行时动态图更新，属真实边界。
_ToolHandler = Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]]


class ToolPolicyMiddleware(AgentMiddleware):
    def __init__(
        self,
        authorized: frozenset[str],
        *,
        declared_subagents: frozenset[str] = frozenset(),
        subagent_create: str = "deny",
    ) -> None:
        super().__init__()
        self._authorized = authorized
        self._declared_subagents = declared_subagents
        self._subagent_create = subagent_create

    def _delegation_denial(self, call_args: dict[str, Any]) -> str | None:
        # 委派执法（handbook 12：模型静默创建同权限子代理不可作生产默认）：
        # deny=只放行声明集内的 subagent_type（general-purpose 属临时创建，不在声明集即拒）；
        # ask 走 interrupt_on 审批（不在此层）；allow 放行任意。
        if self._subagent_create == "allow":
            return None
        requested = call_args.get("subagent_type")
        if isinstance(requested, str) and requested in self._declared_subagents:
            return None
        return (
            f"subagent delegation to {requested!r} is not allowed: "
            f"declared subagents are {sorted(self._declared_subagents)}"
        )

    async def awrap_tool_call(
        self, request: ToolCallRequest, handler: _ToolHandler
    ) -> ToolMessage | Command[Any]:
        call = request.tool_call
        name = call["name"]
        if name not in self._authorized:  # 未授权：不触碰 handler。
            _logger.warning("tool_policy denied unauthorized tool %r", name)
            return ToolMessage(
                content=f"tool {name!r} is not authorized",
                tool_call_id=call["id"] or "",
                name=name,
                status="error",
            )
        if name == SUBAGENT_TOOL_NAME:
            denial = self._delegation_denial(dict(call["args"]))
            if denial is not None:
                _logger.warning("tool_policy denied delegation: %s", denial)
                return ToolMessage(
                    content=denial, tool_call_id=call["id"] or "", name=name, status="error"
                )
        result = await handler(request)
        is_error = isinstance(result, ToolMessage) and result.status == "error"
        _logger.info("tool_policy audit tool=%r error=%s", name, is_error)
        return result


class _ReviewDecision(BaseModel):
    """resume 值的洗净模型：与 approvals.review_resume_value 产出的 dict 同构。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    tool_id: str
    type: str
    response: str | None = None
    reason: str | None = None


_REVIEW_DECISIONS_ADAPTER: TypeAdapter[list[_ReviewDecision]] = TypeAdapter(list[_ReviewDecision])


class RunSupersededError(RuntimeError):
    """run 已被他处终态（cancel）：模型轮边界静默熔断，不再产生事件与副作用。"""


class TerminalGuardMiddleware(AgentMiddleware):
    """跨 worker cancel 的执行侧闸：每个模型轮前查终态，命中即熔断（invoke 的
    claim_terminal 已被 cancel 方拿走 → 异常路径不再发任何事件）。"""

    def __init__(self, *, store: RunRepository, run_id: str) -> None:
        super().__init__()
        self._store = store
        self._run_id = run_id

    async def awrap_model_call(
        self, request: ModelRequest, handler: Callable[[ModelRequest], Awaitable[ModelResponse]]
    ) -> ModelResponse:
        if await self._store.is_terminal(self._run_id):
            raise RunSupersededError(f"run {self._run_id!r} was terminated elsewhere")
        return await handler(request)


class TokenBudgetExceeded(RuntimeError):
    """run 级 token 预算超限：fail-loud 收口为 run.failed，绝不静默继续烧钱。"""


class TokenBudgetMiddleware(AgentMiddleware):
    """token 预算熔断：每次模型调用后累计 usage（store 背书，跨 HITL 段不清零），超限即炸。"""

    def __init__(self, *, budget: int, store: RunRepository, run_id: str) -> None:
        super().__init__()
        self._budget = budget
        self._store = store
        self._run_id = run_id

    async def awrap_model_call(
        self, request: ModelRequest, handler: Callable[[ModelRequest], Awaitable[ModelResponse]]
    ) -> ModelResponse:
        response = await handler(request)
        spent = sum(
            usage.get("total_tokens", 0)
            for message in response.result
            if isinstance(message, AIMessage) and (usage := message.usage_metadata) is not None
        )
        total = await self._store.add_tokens(self._run_id, spent)
        if total > self._budget:
            raise TokenBudgetExceeded(
                f"run token budget exceeded: spent {total} > budget {self._budget}"
            )
        return response


class ToolResultReviewMiddleware(AgentMiddleware):
    """工具后结果审核：执行完暂停，结果经人裁决（采纳/替换/废弃）后才回流模型。

    resume 后 langgraph 从节点头重跑：首跑结果 keep-first 落进 RunRepository，
    重入命中缓存即跳过工具执行——审核暂停绝不导致工具双跑。
    """

    def __init__(self, review: frozenset[str], store: RunRepository, run_id: str) -> None:
        super().__init__()
        self._review = review
        self._store = store
        self._run_id = run_id

    async def awrap_tool_call(
        self, request: ToolCallRequest, handler: _ToolHandler
    ) -> ToolMessage | Command[Any]:
        call = request.tool_call
        name = call["name"]
        if name not in self._review:
            return await handler(request)
        tool_id = call["id"] or ""
        cached = await self._store.get_tool_result(self._run_id, tool_id)
        if cached is None:
            result = await handler(request)
            if not isinstance(result, ToolMessage):
                # Command 形态结果（状态更新）无文本可审：直接放行，不进入审核。
                return result
            # .text 是框架的文本收窄口（content 联合 → str），不自拆 content 块。
            first = (result.text, result.status == "error")
            await self._store.put_tool_result(self._run_id, tool_id, first[0], first[1])
            cached = first
        content, is_error = cached
        # 结果审核 = request_human(kind="review") 预设：request_id=tool_id（工具边界幂等锚），
        # context 携已执行结果供人裁决。resume 值语义不变（list[decision dict]），wire 投影不变。
        decisions = request_human(
            kind="review",
            request_id=tool_id,
            context={
                "name": name,
                "args": dict(call["args"]),
                "result": content,
                "is_error": is_error,
            },
        )
        return _apply_review_decision(decisions, tool_id=tool_id, name=name, content=content)


def _apply_review_decision(
    decisions: object, *, tool_id: str, name: str, content: str
) -> ToolMessage:
    # resume 值与 HIL 同构：list[decision dict]，Pydantic 洗净后按 tool_id 取己项；形状不符 fail-loud。
    parsed = _REVIEW_DECISIONS_ADAPTER.validate_python(decisions)
    mine = next((d for d in parsed if d.tool_id == tool_id), None)
    if mine is None:
        raise ValueError(f"result review resume missing decision for tool {tool_id!r}")
    if mine.type == "approve":
        return ToolMessage(content=content, tool_call_id=tool_id, name=name)
    if mine.type == "respond":
        if not mine.response:
            raise ValueError("result review respond requires a non-empty response")
        return ToolMessage(content=mine.response, tool_call_id=tool_id, name=name)
    if mine.type == "reject":
        suffix = f": {mine.reason}" if mine.reason else ""
        # 普通文本而非 error：模型感知结果被废弃，可自行换路，不视为工具故障。
        return ToolMessage(
            content=f"[result rejected by user{suffix}]", tool_call_id=tool_id, name=name
        )
    raise ValueError(f"result review got unsupported decision type {mine.type!r}")


_UNKNOWN_OUTCOME_PREFIX = "[unknown_outcome]"


class ToolEffectJournalMiddleware(AgentMiddleware):
    """R3 tool effect journal：副作用工具的执行前记账 + checkpoint 重放守门。

    最靠近副作用发生处（tool-call 链最内层）：执行前落 started 行（keep-first，锚=tool_call_id），
    返回后置 succeeded|failed。checkpoint takeover/resume 重放到工具节点时先查 journal：
      无行         → 正常执行（先落 started）。
      succeeded/failed → 不重执行，以记录结果短路（幂等重放，绝不双写）。
      started（unknown-outcome：上次崩在执行中）→ 默认不自动重放：返回 is_error 结果交模型/HITL 决策。
    纯读/幂等工具经 JOURNAL_EXEMPT_TOOLS 整体豁免（白名单即豁免表，重执行天然收敛）；
    MCP 工具不在表内 = 一律按非幂等守门。
    """

    def __init__(
        self,
        *,
        store: RunRepository,
        run_id: str,
        exempt: frozenset[str] = JOURNAL_EXEMPT_TOOLS,
    ) -> None:
        super().__init__()
        self._store = store
        self._run_id = run_id
        self._exempt = exempt

    async def awrap_tool_call(
        self, request: ToolCallRequest, handler: _ToolHandler
    ) -> ToolMessage | Command[Any]:
        call = request.tool_call
        name = call["name"]
        if name in self._exempt:
            # 纯读/幂等 / Command 形态工具：不落 journal，直接放行（重执行安全）。
            return await handler(request)
        tool_id = call["id"] or ""
        recorded = await self._store.get_tool_journal(self._run_id, tool_id)
        if recorded is not None:
            return self._replay(recorded, tool_id=tool_id, name=name)
        await self._store.journal_tool_started(self._run_id, tool_id, name)
        try:
            result = await handler(request)
        except GraphInterrupt:
            # 工具内 interrupt（MCP elicitation / request_input 等 HITL 暂停）≠崩溃：GraphInterrupt
            # 穿透中间件、resume 后工具按设计从头重进。撤销本次 started 行（视同无行）再原样重抛，
            # 否则合法重入会被守门误判 unknown-outcome。真进程死不走 except 路径，守门语义不变。
            await self._store.clear_tool_journal(self._run_id, tool_id)
            raise
        if isinstance(result, ToolMessage):
            # .text 是框架文本收窄口；Command 形态（状态更新）无文本结果可短路，留 started 行——
            # 重放守门对其保守判 unknown-outcome（非幂等 Command 副作用工具应入豁免表，此处不双写）。
            await self._store.journal_tool_finished(
                self._run_id, tool_id, result.text, result.status == "error"
            )
        return result

    def _replay(self, recorded: object, *, tool_id: str, name: str) -> ToolMessage:
        # recorded: ToolJournalRecord（persistence 层导出）——按状态短路。
        status = getattr(recorded, "status", "started")
        if status == "started":
            metrics.record_tool_unknown_outcome()
            return ToolMessage(
                content=(
                    f"{_UNKNOWN_OUTCOME_PREFIX} tool {name!r} started but its outcome was never "
                    "recorded (crash mid-execution); not re-executed automatically to avoid a "
                    "duplicate side effect. Decide whether to retry or take another path."
                ),
                tool_call_id=tool_id,
                name=name,
                status="error",
            )
        result = getattr(recorded, "result", "")
        is_error = bool(getattr(recorded, "is_error", False))
        return ToolMessage(
            content=result,
            tool_call_id=tool_id,
            name=name,
            status="error" if is_error else "success",
        )


class SteeringMiddleware(AgentMiddleware):
    """运行中插话：模型轮前排空信箱，按到达序注入 HumanMessage——协作式转向，
    不打断进行中的工具；稳定 id=message_id 保 checkpoint 重放幂等。只挂主链。"""

    def __init__(self, *, store: RunRepository, run_id: str) -> None:
        super().__init__()
        self._store = store
        self._run_id = run_id

    async def abefore_model(
        self, state: AgentState[Any], runtime: Runtime[Any]
    ) -> dict[str, Any] | None:
        # peek + 下一轮见证 ack（审计缺口：排空与 checkpoint 落盘非原子会窄窗丢插话）——
        # 只有已出现在 state["messages"]（即已随 checkpoint 落定）的插话才从信箱删除；
        # 未落定的每轮重注入，稳定 id 由 add_messages 去重，任意崩溃点收敛且绝不丢。
        steers = await self._store.peek_steers(self._run_id)
        if not steers:
            return None
        for message_id, content in steers:
            if not content:
                raise ValueError(f"empty steer content: message_id={message_id!r}")
        seen_ids = {message.id for message in state["messages"]}
        landed = [message_id for message_id, _ in steers if message_id in seen_ids]
        if landed:
            await self._store.ack_steers(self._run_id, landed)
        fresh = [(mid, content) for mid, content in steers if mid not in seen_ids]
        if not fresh:
            return None
        return {
            "messages": [
                HumanMessage(content=content, id=message_id) for message_id, content in fresh
            ]
        }
