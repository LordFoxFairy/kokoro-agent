"""MCP elicitation 全链 e2e：进程内 FastMCP 真服务器在工具执行中途 elicit → 桥接为 kind=input 暂停。

覆盖 H3 核心：真 HTTP 传输下 server→client 的 elicitation 请求，经 adapter 后台 receive loop
桥给 mcp_call 执行任务，在图节点栈上 request_input 挂起（checkpoint 承载）；control 流回灌 submit
→ ElicitResult(accept) 让 server 续跑；reject → decline；不合法 value → 重问带 validation_error。

走真实 supervisor.serve + 独立 control 流（与 test_local_fake_run 同形），无外部依赖，可进 CI。
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from collections.abc import Iterator, Sequence
from uuid import uuid4

import pytest
import uvicorn
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession
from pydantic import BaseModel, Field, JsonValue

from kokoro_agent.agents.base import AssembledAgent
from kokoro_agent.storage.execution_context import ExecutionContextAuthority
from kokoro_agent.contract import (
    REQUESTS_MAXLEN,
    REQUESTS_STREAM,
    RUN_CONTROL_MAXLEN,
    FilesystemPerm,
    ExecutionContextIntentRoot,
    McpGrant,
    ModelConfig,
    Permissions,
    RunInput,
    RunRequest,
    RuntimeConfig,
    RuntimeContext,
    run_control_stream,
    run_events_stream,
)
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.mcp.config import McpServerConfig
from kokoro_agent.mcp.egress import configure_egress_mode, current_egress_mode
from kokoro_agent.mcp.tools import make_mcp_tools
from kokoro_agent.model.local_fake import make_local_fake_chat_model
from kokoro_agent.prompts import GENERAL_PROMPT
from kokoro_agent.sandbox import build_filesystem_permissions
from kokoro_agent.storage.ledger import RunLedger
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.subagents import build_catalog
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.registry import resolve_tools
from kokoro_agent.worker.supervisor import RunSupervisor

_MCP_TOOL_ID = "local_mcp"


@pytest.fixture(autouse=True)
def egress_off() -> Iterator[None]:
    # 本地 127.0.0.1 fixture：关连接期 egress 防线放行环回（strict 缺省会拒 loopback）。
    previous = current_egress_mode()
    configure_egress_mode("off")
    try:
        yield
    finally:
        configure_egress_mode(previous)


class OtpForm(BaseModel):
    otp: str


class PatternOtpForm(BaseModel):
    # pattern 让 agent 侧 jsonschema 校验有拒绝空间：非六位数字必被拒（复现"非法→重问"分支）。
    otp: str = Field(pattern=r"^[0-9]{6}$")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _build_fixture_app() -> FastMCP:
    # stateful（非 stateless）：server→client 的 elicitation 需会话内双向关联，stateless 无法对齐响应。
    server = FastMCP("fixture", stateless_http=False)

    async def verify(ctx: Context[ServerSession, None, None]) -> str:
        # 工具执行中途向客户端 elicit：客户端（agent 侧）桥为 kind=input 人机暂停。
        result = await ctx.elicit(message="请输入验证码", schema=OtpForm)
        if result.action == "accept":
            return f"verified:{result.data.otp}"
        return f"declined:{result.action}"

    server.add_tool(verify, name="verify")
    return server


@pytest.fixture(scope="module")
def mcp_base_url() -> Iterator[str]:
    port = _free_port()
    app = _build_fixture_app().streamable_http_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while time.time() < deadline and not server.started:
        time.sleep(0.05)
    if not server.started:
        raise RuntimeError("fixture mcp server failed to start")
    yield f"http://127.0.0.1:{port}/mcp"
    server.should_exit = True
    thread.join(timeout=5)


def _build_pattern_fixture_app() -> FastMCP:
    # 同 _build_fixture_app，唯 elicit schema 带 pattern 约束（agent 侧校验有拒绝空间）。
    server = FastMCP("fixture-pattern", stateless_http=False)

    async def verify(ctx: Context[ServerSession, None, None]) -> str:
        result = await ctx.elicit(message="请输入验证码", schema=PatternOtpForm)
        if result.action == "accept":
            return f"verified:{result.data.otp}"
        return f"declined:{result.action}"

    server.add_tool(verify, name="verify")
    return server


@pytest.fixture(scope="module")
def pattern_mcp_base_url() -> Iterator[str]:
    port = _free_port()
    app = _build_pattern_fixture_app().streamable_http_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while time.time() < deadline and not server.started:
        time.sleep(0.05)
    if not server.started:
        raise RuntimeError("pattern fixture mcp server failed to start")
    yield f"http://127.0.0.1:{port}/mcp"
    server.should_exit = True
    thread.join(timeout=5)


def _request(run_id: str, *, filesystem: FilesystemPerm = "read_only") -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        thread_id=f"{run_id}-thread",
        input=RunInput(message_id=f"{run_id}-m", content="验证一下"),
        runtime=RuntimeConfig(
            agent_catalog_ref=f"agent-catalog:sha256:{'a' * 64}",
            agent_type="general",
            model=ModelConfig(provider="anthropic", name="claude", authorization_handle="model-authz:test"),
            tools=[],
            skills=[],
            # wire 是 McpGrant 授权卡；本 e2e 用 build() 里的 hardcoded registry 装配，grant 只需过契约校验。
            mcp_servers=[McpGrant(option_ref="mcp:fx", scope="official", name="fx", revision=1, config_hash="f" * 64)],
            subagents=[],
            backend="state",
            permissions=Permissions(
                approval_tools=[],
                review_tools=[],
                subagent_create="deny",
                filesystem=filesystem,
            ),
        ),
        context=RuntimeContext(namespace="e2e", session_id="e2e-session"),
        execution_context=ExecutionContextIntentRoot(mode="root"),
    )


def _elicit_script() -> list[AIMessage]:
    return [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "mcp_call",
                    "args": {"server": "fx", "tool": "verify", "arguments": {}},
                    "id": _MCP_TOOL_ID,
                    "type": "tool_call",
                }
            ],
        ),
        AIMessage(content="完成"),
    ]


def _build_supervisor(
    store: RunLedger, saver: BaseCheckpointSaver[str], url: str, script: Sequence[AIMessage]
) -> RunSupervisor:
    catalog = build_catalog(None)
    registry = {
        "fx": McpServerConfig(
            transport="streamable_http", url=url, allowed_tools=["verify"], timeout_s=15
        )
    }

    async def build(request: RunRequest) -> AssembledAgent:
        runtime = request.runtime
        mcp_tools = list(make_mcp_tools(["fx"], registry))
        middleware: list[AgentMiddleware] = []
        return AssembledAgent(
            agent=build_agent(
                model=make_local_fake_chat_model(script),
                tools=[*resolve_tools(runtime.tools), *mcp_tools],
                system_prompt=GENERAL_PROMPT,
                subagents=catalog.definitions(),
                checkpointer=saver,
                permissions=build_filesystem_permissions(runtime.permissions.filesystem),
                interrupt_on=build_interrupt_on(frozenset(runtime.permissions.approval_tools)),
                middleware=middleware,
            ),
            tool_descriptions={},
        )

    def approval_names(request: RunRequest) -> frozenset[str]:
        return frozenset(request.runtime.permissions.approval_tools) | {ASK_USER_TOOL_NAME}

    return RunSupervisor(
        agent_builder=build,
        store=store,
        execution_context=ExecutionContextAuthority(store=store, checkpointer=saver),
        approval_tool_names=approval_names,
        trace_factory=lambda _request: None,
        source_for=catalog.source_for,
        consumer="e2e-elicit",
        heartbeat_s=60.0,
    )


async def _read_kinds(bus: RedisStream, run_id: str) -> list[str]:
    items = await bus.read_all(run_events_stream(run_id))
    return [str(item.event.get("kind")) for item in items]


async def _awaiting_input(bus: RedisStream, run_id: str, timeout: float = 40.0) -> dict[str, JsonValue]:
    async def _poll() -> dict[str, JsonValue]:
        while True:
            for item in await bus.read_all(run_events_stream(run_id)):
                if item.event.get("kind") != "tool.awaiting_approval":
                    continue
                payload = item.event.get("payload")
                if isinstance(payload, dict) and payload.get("kind") == "input":
                    return payload
            if "run.failed" in await _read_kinds(bus, run_id):
                raise AssertionError("run failed before input pause")
            await asyncio.sleep(0.02)

    return await asyncio.wait_for(_poll(), timeout=timeout)


async def _wait_terminal(bus: RedisStream, run_id: str, timeout: float = 40.0) -> None:
    async def _poll() -> None:
        while not ({"run.completed", "run.failed"} & set(await _read_kinds(bus, run_id))):
            await asyncio.sleep(0.02)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def _publish_resume(
    bus: RedisStream, run: RunRequest, decision: dict[str, JsonValue]
) -> None:
    event: dict[str, JsonValue] = {
        "kind": "run.resume",
        # 每次 resume 是一次不同的用户裁决 → 独立 decision_id（R2 inbox 按此去重；
        # 同 id 视作重发被丢弃，故 reprompt/多轮 submit 必须各自唯一）。
        "decision_id": f"dec-{uuid4().hex}",
        "run_id": run.run_id,
        "thread_id": run.thread_id,
        "decisions": [decision],
    }
    await bus.publish(run_control_stream(run.run_id), event, maxlen=RUN_CONTROL_MAXLEN)


async def _mcp_returned(bus: RedisStream, run_id: str) -> dict[str, JsonValue]:
    # resume 后 replay 才产出真实 returned（暂停时不发伪 returned）：取最后一条即续跑后的真值。
    found: dict[str, JsonValue] | None = None
    for item in await bus.read_all(run_events_stream(run_id)):
        if item.event.get("kind") != "tool.returned":
            continue
        payload = item.event.get("payload")
        if isinstance(payload, dict) and payload.get("tool_id") == _MCP_TOOL_ID:
            found = payload
    if found is None:
        raise AssertionError(f"no tool.returned for {_MCP_TOOL_ID!r}")
    return found


async def _run_until_input(
    bus: RedisStream, store: RunLedger, saver: BaseCheckpointSaver[str], url: str, run: RunRequest
) -> tuple[asyncio.Task[None], dict[str, JsonValue]]:
    await bus.delete(REQUESTS_STREAM)
    supervisor = _build_supervisor(store, saver, url, _elicit_script())
    serve_task = asyncio.create_task(supervisor.serve(bus))
    await bus.publish(REQUESTS_STREAM, dict(run.model_dump()), maxlen=REQUESTS_MAXLEN)
    awaiting = await _awaiting_input(bus, run.run_id)
    return serve_task, awaiting


async def _stop(serve_task: asyncio.Task[None]) -> None:
    serve_task.cancel()
    try:
        await serve_task
    except asyncio.CancelledError:
        pass


async def test_elicitation_submit_bridges_to_input_and_resumes(
    stream: RedisStream, ledger: RunLedger, checkpointer: BaseCheckpointSaver[str], mcp_base_url: str
) -> None:
    run = _request(f"e2e-elicit-{uuid4().hex[:8]}")
    serve_task, awaiting = await _run_until_input(
        stream, ledger, checkpointer, mcp_base_url, run
    )
    try:
        # awaiting 帧形状：kind=input、决策集 submit/reject、schema 透传、tool_id=mcp_call 的 tool_id。
        assert awaiting["tool_id"] == _MCP_TOOL_ID
        assert awaiting["allowed_decisions"] == ["submit", "reject"]
        schema = awaiting["input_schema"]
        assert isinstance(schema, dict) and "otp" in str(schema)

        # submit 回灌 value → ElicitResult(accept) 让 server 续跑 → mcp_call 返回携人给的 otp。
        await _publish_resume(
            stream,
            run,
            {"type": "submit", "request_id": _MCP_TOOL_ID, "value": {"otp": "654321"}},
        )
        await _wait_terminal(stream, run.run_id)
        assert (await _read_kinds(stream, run.run_id))[-1] == "run.completed"
        returned = await _mcp_returned(stream, run.run_id)
        assert "verified:654321" in str(returned["result"])

        # 暂停不发伪 returned：wire 上无内层 verify 工具泄漏、无 Interrupt repr 冒充结果。
        for item in await stream.read_all(run_events_stream(run.run_id)):
            payload = item.event.get("payload")
            if item.event.get("kind") == "tool.returned" and isinstance(payload, dict):
                assert "Interrupt(" not in str(payload.get("result"))
                assert payload.get("name") == "mcp_call"
    finally:
        await _stop(serve_task)


async def test_elicitation_reject_maps_to_decline(
    stream: RedisStream, ledger: RunLedger, checkpointer: BaseCheckpointSaver[str], mcp_base_url: str
) -> None:
    run = _request(f"e2e-decline-{uuid4().hex[:8]}")
    serve_task, _awaiting = await _run_until_input(
        stream, ledger, checkpointer, mcp_base_url, run
    )
    try:
        await _publish_resume(
            stream, run, {"type": "reject", "tool_id": _MCP_TOOL_ID, "reason": "拒绝"}
        )
        await _wait_terminal(stream, run.run_id)
        assert (await _read_kinds(stream, run.run_id))[-1] == "run.completed"
        returned = await _mcp_returned(stream, run.run_id)
        assert "declined" in str(returned["result"])
    finally:
        await _stop(serve_task)


async def test_elicitation_invalid_value_reprompts_then_submits(
    stream: RedisStream, ledger: RunLedger, checkpointer: BaseCheckpointSaver[str], mcp_base_url: str
) -> None:
    run = _request(f"e2e-reprompt-{uuid4().hex[:8]}")
    serve_task, _awaiting = await _run_until_input(
        stream, ledger, checkpointer, mcp_base_url, run
    )
    try:
        # ① 不合法回灌（缺必填 otp）：不炸 run，重新 interrupt 带 validation_error。
        await _publish_resume(
            stream, run, {"type": "submit", "request_id": _MCP_TOOL_ID, "value": {}}
        )

        async def _reprompted() -> dict[str, JsonValue]:
            while True:
                for item in await stream.read_all(run_events_stream(run.run_id)):
                    if item.event.get("kind") != "tool.awaiting_approval":
                        continue
                    payload = item.event.get("payload")
                    if not isinstance(payload, dict) or payload.get("kind") != "input":
                        continue
                    args = payload.get("args")
                    if isinstance(args, dict) and "validation_error" in args:
                        return payload
                if "run.failed" in await _read_kinds(stream, run.run_id):
                    raise AssertionError("run failed before reprompt")
                await asyncio.sleep(0.02)

        await asyncio.wait_for(_reprompted(), timeout=40.0)

        # ② 重填合法值：通过校验 → server 续跑。
        await _publish_resume(
            stream,
            run,
            {"type": "submit", "request_id": _MCP_TOOL_ID, "value": {"otp": "111"}},
        )
        await _wait_terminal(stream, run.run_id)
        assert (await _read_kinds(stream, run.run_id))[-1] == "run.completed"
        returned = await _mcp_returned(stream, run.run_id)
        assert "verified:111" in str(returned["result"])
    finally:
        await _stop(serve_task)


async def test_elicitation_pattern_revalidation_double_submit_reaches_terminal(
    stream: RedisStream,
    ledger: RunLedger,
    checkpointer: BaseCheckpointSaver[str],
    pattern_mcp_base_url: str,
) -> None:
    # MCP-REVALIDATION-HANG 回归钉：pattern 夹具下 abc(非法)→246810(合法) 双 control 回灌后，
    # 每次 resume 都 teardown 连接重放 mcp_call（新 bridge+新连接），末轮桥/连接生命周期须收束到终态，
    # 不悬挂。锁死 _ElicitBridge 与 adapter receive-loop 在多重重放下的配对正确性。
    run = _request(f"e2e-revalidate-{uuid4().hex[:8]}")
    serve_task, awaiting = await _run_until_input(
        stream, ledger, checkpointer, pattern_mcp_base_url, run
    )
    try:
        assert awaiting["tool_id"] == _MCP_TOOL_ID

        # ① 非法回灌（pattern 不匹配 "abc"）：不炸 run，重新 interrupt 带 validation_error。
        await _publish_resume(
            stream, run, {"type": "submit", "request_id": _MCP_TOOL_ID, "value": {"otp": "abc"}}
        )

        async def _reprompted() -> dict[str, JsonValue]:
            while True:
                for item in await stream.read_all(run_events_stream(run.run_id)):
                    if item.event.get("kind") != "tool.awaiting_approval":
                        continue
                    payload = item.event.get("payload")
                    if not isinstance(payload, dict) or payload.get("kind") != "input":
                        continue
                    args = payload.get("args")
                    if isinstance(args, dict) and "validation_error" in args:
                        return payload
                if "run.failed" in await _read_kinds(stream, run.run_id):
                    raise AssertionError("run failed before reprompt")
                await asyncio.sleep(0.02)

        await asyncio.wait_for(_reprompted(), timeout=40.0)

        # ② 合法六位数字：通过校验 → server accept 续跑 → 必须到终态并回携人给的 otp 的真实结果。
        await _publish_resume(
            stream, run, {"type": "submit", "request_id": _MCP_TOOL_ID, "value": {"otp": "246810"}}
        )
        await _wait_terminal(stream, run.run_id)
        assert (await _read_kinds(stream, run.run_id))[-1] == "run.completed"
        returned = await _mcp_returned(stream, run.run_id)
        assert "verified:246810" in str(returned["result"])
    finally:
        await _stop(serve_task)
