"""R3 tool effect journal 中间件规格：执行前记账、重放守门（短路/unknown-outcome）、白名单豁免。"""

from __future__ import annotations

from typing import Any

import pytest
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.errors import GraphInterrupt
from langgraph.prebuilt.tool_node import ToolRuntime

from fakes import FakeLedger
from kokoro_agent.tools.middleware import ToolEffectJournalMiddleware


def _runtime() -> ToolRuntime[Any, Any]:
    return ToolRuntime(
        state=None,
        context=None,
        config={},
        stream_writer=lambda _chunk: None,
        tool_call_id="c1",
        store=None,
    )


class _Handler:
    """记账执行次数的工具替身：可注入结果、is_error，或抛错模拟崩溃。"""

    def __init__(self, content: str = "wrote file", *, is_error: bool = False) -> None:
        self.calls = 0
        self._content = content
        self._is_error = is_error

    async def __call__(self, request: ToolCallRequest) -> ToolMessage:
        self.calls += 1
        return ToolMessage(
            content=self._content,
            tool_call_id="c1",
            name=str(request.tool_call["name"]),
            status="error" if self._is_error else "success",
        )


def _request(name: str = "write_file", tool_id: str = "c1") -> ToolCallRequest:
    return ToolCallRequest(
        tool_call={"name": name, "args": {"path": "a.txt"}, "id": tool_id, "type": "tool_call"},
        tool=None,
        state=None,
        runtime=_runtime(),
    )


def _mw(store: FakeLedger) -> ToolEffectJournalMiddleware:
    return ToolEffectJournalMiddleware(store=store, run_id="rn")


async def test_exempt_read_tool_never_journaled() -> None:
    # 纯读工具（白名单）：不落 journal，直接执行。
    store = FakeLedger()
    handler = _Handler("file body")
    result = await _mw(store).awrap_tool_call(_request("read_file"), handler)
    assert isinstance(result, ToolMessage) and result.text == "file body"
    assert handler.calls == 1
    assert store.tool_journal == {}


async def test_first_execution_records_started_then_finished() -> None:
    # 副作用工具首跑：执行前落 started，返回后 succeeded 附结果。
    store = FakeLedger()
    handler = _Handler("wrote file")
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.text == "wrote file"
    assert handler.calls == 1
    entry = store.tool_journal[("rn", "c1")]
    assert entry["status"] == "succeeded"
    assert entry["result"] == "wrote file" and entry["is_error"] is False


async def test_failed_tool_records_failed_status() -> None:
    store = FakeLedger()
    handler = _Handler("boom", is_error=True)
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.status == "error"
    entry = store.tool_journal[("rn", "c1")]
    assert entry["status"] == "failed" and entry["is_error"] is True


async def test_replay_succeeded_short_circuits_without_reexecuting() -> None:
    # 重放守门核心：succeeded 行 → 不重执行副作用，短路回灌记录结果（双写防护）。
    store = FakeLedger()
    store.tool_journal[("rn", "c1")] = {
        "name": "write_file",
        "status": "succeeded",
        "result": "wrote file",
        "is_error": False,
    }
    handler = _Handler("SHOULD NOT RUN")
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.text == "wrote file"
    assert result.status == "success"
    assert handler.calls == 0


async def test_replay_failed_short_circuits_as_error() -> None:
    store = FakeLedger()
    store.tool_journal[("rn", "c1")] = {
        "name": "write_file",
        "status": "failed",
        "result": "disk full",
        "is_error": True,
    }
    handler = _Handler("SHOULD NOT RUN")
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.text == "disk full"
    assert result.status == "error"
    assert handler.calls == 0


async def test_replay_unknown_outcome_started_does_not_reexecute() -> None:
    # started 行=上次进程死在执行中（unknown-outcome，未走 except 撤销路径）：非幂等工具默认不自动
    # 重放，返回 is_error 交决策——真崩溃窗口守门语义不变。
    store = FakeLedger()
    store.tool_journal[("rn", "c1")] = {
        "name": "write_file",
        "status": "started",
        "result": None,
        "is_error": None,
    }
    handler = _Handler("SHOULD NOT RUN")
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.status == "error"
    assert "unknown_outcome" in result.text
    assert handler.calls == 0


async def test_tool_internal_interrupt_clears_started_then_resume_reenters() -> None:
    # 工具内 GraphInterrupt（MCP elicitation / request_input 等 HITL 暂停）≠崩溃：撤销 started 行、
    # 原样重抛 interrupt；resume 后工具从头重进不被守门拦，正常执行并记账。
    store = FakeLedger()

    async def interrupting(_request: ToolCallRequest) -> ToolMessage:
        raise GraphInterrupt()

    with pytest.raises(GraphInterrupt):
        await _mw(store).awrap_tool_call(_request(), interrupting)
    # started 行已撤销（视同无行）。
    assert ("rn", "c1") not in store.tool_journal

    # resume 重进：守门无行 → 正常执行并落 succeeded（合法重入未被误判 unknown-outcome）。
    handler = _Handler("wrote after resume")
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.text == "wrote after resume"
    assert handler.calls == 1
    assert store.tool_journal[("rn", "c1")]["status"] == "succeeded"
