"""结果审核中间件规格：双执行防护、决策应用、resume 值洗净 fail-loud。"""

from __future__ import annotations

from typing import Any

import pytest
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolRuntime

import kokoro_agent.hitl.request as request_module
from support.fakes import FakeLedger
from kokoro_agent.tools.middleware import ToolResultReviewMiddleware


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
    def __init__(self, content: str = "raw result") -> None:
        self.calls = 0
        self._content = content

    async def __call__(self, request: ToolCallRequest) -> ToolMessage:
        self.calls += 1
        return ToolMessage(content=self._content, tool_call_id="c1", name="lookup")


def _request(name: str = "lookup") -> ToolCallRequest:
    return ToolCallRequest(
        tool_call={"name": name, "args": {"q": "x"}, "id": "c1", "type": "tool_call"},
        tool=None,
        state=None,
        runtime=_runtime(),
    )


def _mw(store: FakeLedger) -> ToolResultReviewMiddleware:
    return ToolResultReviewMiddleware(frozenset({"lookup"}), store, "rn")


def _patch_interrupt(monkeypatch: pytest.MonkeyPatch, value: object) -> list[object]:
    # 中间件经 request_human 发起审核暂停，request_human 包装 hitl 的 interrupt：
    # 单测在此打桩（真实 interrupt/resume 由 e2e 覆盖），seen 收下信封供形状断言。
    seen: list[object] = []

    def fake_interrupt(payload: object) -> object:
        seen.append(payload)
        return value

    monkeypatch.setattr(request_module, "interrupt", fake_interrupt)
    return seen


async def test_non_review_tool_bypasses(monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeLedger()
    handler = _Handler()
    seen = _patch_interrupt(monkeypatch, [])
    result = await _mw(store).awrap_tool_call(_request("other"), handler)
    assert isinstance(result, ToolMessage) and result.text == "raw result"
    assert handler.calls == 1 and seen == [] and store.tool_results == {}


async def test_first_pass_caches_then_interrupts(monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeLedger()
    handler = _Handler()
    seen = _patch_interrupt(
        monkeypatch, [{"tool_id": "c1", "type": "approve"}]
    )
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.text == "raw result"
    assert handler.calls == 1
    assert store.tool_results[("rn", "c1")] == ("raw result", False)
    assert len(seen) == 1
    payload = seen[0]
    assert isinstance(payload, dict)
    # review 预设的 HumanRequest 信封：request_id=tool_id，结果进 context 供人裁决。
    assert payload["kokoro_human_request"] == {
        "request_id": "c1",
        "kind": "review",
        "response_schema": None,
        "context": {
            "name": "lookup",
            "args": {"q": "x"},
            "result": "raw result",
            "is_error": False,
        },
    }


async def test_resume_reentry_skips_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    # resume 后节点从头重跑：缓存命中即不再执行工具——双执行防护的核心断言。
    store = FakeLedger()
    store.tool_results[("rn", "c1")] = ("first run result", False)
    handler = _Handler("second run result")
    _patch_interrupt(monkeypatch, [{"tool_id": "c1", "type": "approve"}])
    result = await _mw(store).awrap_tool_call(_request(), handler)
    assert isinstance(result, ToolMessage) and result.text == "first run result"
    assert handler.calls == 0


async def test_respond_replaces_result(monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeLedger()
    _patch_interrupt(
        monkeypatch, [{"tool_id": "c1", "type": "respond", "response": "curated"}]
    )
    result = await _mw(store).awrap_tool_call(_request(), _Handler())
    assert isinstance(result, ToolMessage) and result.text == "curated"


async def test_reject_discards_result(monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeLedger()
    _patch_interrupt(
        monkeypatch, [{"tool_id": "c1", "type": "reject", "reason": "wrong data"}]
    )
    result = await _mw(store).awrap_tool_call(_request(), _Handler())
    assert isinstance(result, ToolMessage)
    assert result.text == "[result rejected by user: wrong data]"
    assert result.status != "error"


@pytest.mark.parametrize(
    "resume_value",
    [
        "not-a-list",
        [{"tool_id": "someone-else", "type": "approve"}],
        [{"tool_id": "c1", "type": "edit", "response": None, "reason": None}],
        [{"tool_id": "c1", "type": "respond"}],
    ],
)
async def test_bad_resume_values_fail_loud(
    monkeypatch: pytest.MonkeyPatch, resume_value: object
) -> None:
    store = FakeLedger()
    _patch_interrupt(monkeypatch, resume_value)
    with pytest.raises((ValueError, Exception)):
        await _mw(store).awrap_tool_call(_request(), _Handler())
