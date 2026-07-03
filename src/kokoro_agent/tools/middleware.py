"""工具策略中间件：未授权 fail-closed 拒绝，授权放行并审计。"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.types import Command, interrupt
from pydantic import BaseModel, ConfigDict, TypeAdapter

from kokoro_agent.storage.run_state import RunStateStore

_logger = logging.getLogger(__name__)

# Command[Any] 对齐框架基类签名：其类型参数是运行时动态图更新，属真实边界。
_ToolHandler = Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]]


class ToolPolicyMiddleware(AgentMiddleware):
    def __init__(self, authorized: frozenset[str]) -> None:
        super().__init__()
        self._authorized = authorized

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


class ToolResultReviewMiddleware(AgentMiddleware):
    """工具后结果审核：执行完暂停，结果经人裁决（采纳/替换/废弃）后才回流模型。

    resume 后 langgraph 从节点头重跑：首跑结果 keep-first 落进 RunStateStore，
    重入命中缓存即跳过工具执行——审核暂停绝不导致工具双跑。
    """

    def __init__(self, review: frozenset[str], store: RunStateStore, run_id: str) -> None:
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
        decisions = interrupt(
            {
                "kokoro_result_review": {
                    "tool_id": tool_id,
                    "name": name,
                    "args": dict(call["args"]),
                    "result": content,
                    "is_error": is_error,
                }
            }
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
