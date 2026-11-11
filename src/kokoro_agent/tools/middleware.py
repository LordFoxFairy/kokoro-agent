"""工具策略中间件：未授权 fail-closed 拒绝，授权放行并审计。"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any

from deepagents.middleware.filesystem import (
    EditFileSchema,
    ExecuteSchema,
    WriteFileSchema,
)
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import (
    ModelRequest,
    ModelResponse,
    ToolCallRequest,
)
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.errors import GraphInterrupt
from langgraph.types import Command
from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter, ValidationError

from langchain.agents.middleware.types import AgentState
from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from kokoro_agent import metrics
from kokoro_agent.hitl import request_human
from kokoro_agent.domain.run.repository import LeaseFence, RunRepository
from kokoro_agent.domain.run.workspace_output import (
    MAX_OUTPUT_BYTES,
    NativeWorkspaceWrite,
    WorkspaceExecuteIntent,
    WorkspaceOutputGuard,
)
from kokoro_agent.protocol.events import DeliveryCreatedPayload
from kokoro_agent.sandbox.backend import (
    DOCKER_OUTPUT_ID_PREFIX,
    DOCKER_OUTPUT_TEARDOWN_PREFIX,
)
from kokoro_agent.sandbox.docker_backend import DockerShellBackend
from kokoro_agent.sandbox.execution_environment import ExecutionBoundaryError
from kokoro_agent.sandbox.output_edit import capture_output_edit
from kokoro_agent.tools.registry import JOURNAL_EXEMPT_TOOLS, SUBAGENT_TOOL_NAME
from kokoro_agent.tools.workspace_output import native_output_path

_logger = logging.getLogger(__name__)

# Command[Any] 对齐框架基类签名：其类型参数是运行时动态图更新，属真实边界。
_ToolHandler = Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]]

_DELIVERY_CALL: ContextVar[tuple[str, str] | None] = ContextVar(
    "kokoro_delivery_call", default=None
)
_DELIVERY_INTENT = TypeAdapter(dict[str, JsonValue])


def current_delivery_tool_call_id(run_id: str) -> str | None:
    """Return only the journal-bound invocation, never a model-supplied argument."""
    current = _DELIVERY_CALL.get()
    return current[1] if current is not None and current[0] == run_id else None


_MCP_CALL: ContextVar[tuple[str, str] | None] = ContextVar(
    "kokoro_mcp_call", default=None
)


def current_mcp_tool_call_id(run_id: str) -> str | None:
    current = _MCP_CALL.get()
    return current[1] if current is not None and current[0] == run_id else None


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
                    content=denial,
                    tool_call_id=call["id"] or "",
                    name=name,
                    status="error",
                )
        result = await handler(request)
        is_error = isinstance(result, ToolMessage) and result.status == "error"
        _logger.info("tool_policy audit tool=%r error=%s", name, is_error)
        return result


class _ReviewDecision(BaseModel):
    """真实全集 resume map 中结果审核项的唯一内部决策格式。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    request_id: str
    type: str
    response: str | None = None
    reason: str | None = None


_REVIEW_DECISIONS_ADAPTER: TypeAdapter[list[_ReviewDecision]] = TypeAdapter(
    list[_ReviewDecision]
)


class RunSupersededError(RuntimeError):
    """run 已被他处终态（cancel）：模型轮边界静默熔断，不再产生事件与副作用。"""


async def _require_current_lease(
    run_repository: RunRepository, run_id: str, lease: LeaseFence
) -> None:
    if not await run_repository.is_lease_current(run_id, lease):
        raise RunSupersededError(
            f"run {run_id!r} execution lease generation was superseded"
        )


class TerminalGuardMiddleware(AgentMiddleware):
    """跨 worker cancel 的执行侧闸：每个模型轮前查终态，命中即熔断（invoke 的
    terminal 已由 cancel 方原子提交 → 异常路径不再发任何事件）。"""

    def __init__(
        self, *, run_repository: RunRepository, run_id: str, lease: LeaseFence
    ) -> None:
        super().__init__()
        self._run_repository = run_repository
        self._run_id = run_id
        self._lease = lease

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        await _require_current_lease(self._run_repository, self._run_id, self._lease)
        return await handler(request)


class TokenBudgetExceeded(RuntimeError):
    """run 级 token 预算超限：fail-loud 收口为 run.failed，绝不静默继续烧钱。"""


class TokenBudgetMiddleware(AgentMiddleware):
    """token 预算熔断：每次模型调用后累计 usage（RunRepository 背书，跨 HITL 段不清零），超限即炸。"""

    def __init__(
        self,
        *,
        budget: int,
        run_repository: RunRepository,
        run_id: str,
        lease: LeaseFence,
    ) -> None:
        super().__init__()
        self._budget = budget
        self._run_repository = run_repository
        self._run_id = run_id
        self._lease = lease

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        await _require_current_lease(self._run_repository, self._run_id, self._lease)
        response = await handler(request)
        spent = sum(
            usage.get("total_tokens", 0)
            for message in response.result
            if isinstance(message, AIMessage)
            and (usage := message.usage_metadata) is not None
        )
        total = await self._run_repository.add_tokens(self._run_id, self._lease, spent)
        if total is None:
            raise RunSupersededError(
                f"run {self._run_id!r} lost its lease while recording token usage"
            )
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

    def __init__(
        self,
        review: frozenset[str],
        run_repository: RunRepository,
        run_id: str,
        lease: LeaseFence,
    ) -> None:
        super().__init__()
        self._review = review
        self._run_repository = run_repository
        self._run_id = run_id
        self._lease = lease

    async def awrap_tool_call(
        self, request: ToolCallRequest, handler: _ToolHandler
    ) -> ToolMessage | Command[Any]:
        call = request.tool_call
        name = call["name"]
        if name not in self._review:
            return await handler(request)
        await _require_current_lease(self._run_repository, self._run_id, self._lease)
        tool_id = call["id"] or ""
        cached = await self._run_repository.get_tool_result(self._run_id, tool_id)
        if cached is None:
            result = await handler(request)
            if not isinstance(result, ToolMessage):
                # Command 形态结果（状态更新）无文本可审：直接放行，不进入审核。
                return result
            # .text 是框架的文本收窄口（content 联合 → str），不自拆 content 块。
            first = (result.text, result.status == "error")
            persisted = await self._run_repository.put_tool_result(
                self._run_id, self._lease, tool_id, first[0], first[1]
            )
            if persisted is None:
                raise RunSupersededError(
                    f"run {self._run_id!r} lost its lease while caching a tool result"
                )
            cached = persisted
        content, is_error = cached
        # 结果审核 = request_human(kind="review") 预设：request_id=tool_id（工具边界幂等锚），
        # context 携已执行结果供人裁决。内部 resume 项仅按 request_id 寻址。
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
        return _apply_review_decision(
            decisions, tool_id=tool_id, name=name, content=content
        )


def _apply_review_decision(
    decisions: object, *, tool_id: str, name: str, content: str
) -> ToolMessage:
    # 内部请求身份与工具执行身份只在此边界关联；不接受旧 tool_id 决策字段。
    parsed = _REVIEW_DECISIONS_ADAPTER.validate_python(decisions)
    identities = [decision.request_id for decision in parsed]
    if len(identities) != len(set(identities)):
        raise ValueError("result review resume contains duplicate request identities")
    mine = next((d for d in parsed if d.request_id == tool_id), None)
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
            content=f"[result rejected by user{suffix}]",
            tool_call_id=tool_id,
            name=name,
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
        run_repository: RunRepository,
        run_id: str,
        lease: LeaseFence,
        exempt: frozenset[str] = JOURNAL_EXEMPT_TOOLS,
        workspace_thread: str | None = None,
        workspace_guard: WorkspaceOutputGuard | None = None,
        workspace_edit: bool = False,
        workspace_script: DockerShellBackend | None = None,
    ) -> None:
        super().__init__()
        self._run_repository = run_repository
        self._run_id = run_id
        self._lease = lease
        self._exempt = exempt
        self._workspace_thread = workspace_thread
        self._workspace_guard = workspace_guard
        self._workspace_edit = workspace_edit
        self._workspace_script = workspace_script

    def for_workspace_output(
        self,
        thread_id: str,
        guard: WorkspaceOutputGuard | None,
        *,
        edit: bool = False,
        script: DockerShellBackend | None = None,
    ) -> ToolEffectJournalMiddleware:
        return ToolEffectJournalMiddleware(
            run_repository=self._run_repository,
            run_id=self._run_id,
            lease=self._lease,
            exempt=self._exempt,
            workspace_thread=thread_id,
            workspace_guard=guard,
            workspace_edit=edit,
            workspace_script=script,
        )

    def _native_write(self, request: ToolCallRequest) -> NativeWorkspaceWrite | None:
        if self._workspace_thread is None or request.tool_call["name"] != "write_file":
            return None
        path = native_output_path(request, self._workspace_thread, WriteFileSchema)
        content = request.tool_call["args"].get("content")
        if (
            path is None
            or not isinstance(content, str)
            or len(content) > MAX_OUTPUT_BYTES
        ):
            return None
        try:
            raw = content.encode("utf-8")
        except UnicodeEncodeError:
            return None
        return NativeWorkspaceWrite(path, raw) if len(raw) <= MAX_OUTPUT_BYTES else None

    async def _execute_workspace_script(
        self, request: ToolCallRequest, handler: _ToolHandler
    ) -> ToolMessage:
        call, backend, thread = (
            request.tool_call,
            self._workspace_script,
            self._workspace_thread,
        )
        tool_id = call["id"] or ""
        args = call["args"]
        command, timeout = args.get("command"), args.get("timeout")
        path = (
            native_output_path(request, thread, ExecuteSchema, field="output_path")
            if thread is not None
            else None
        )

        def error(message: str) -> ToolMessage:
            return ToolMessage(
                content=message, name="execute", tool_call_id=tool_id, status="error"
            )

        if (
            backend is None
            or path is None
            or not isinstance(command, str)
            or not set(args) <= {"command", "output_path", "timeout"}
        ):
            return error("Invalid isolated workspace output request")
        recorded = await self._run_repository.get_tool_journal(self._run_id, tool_id)
        if recorded is not None and recorded.name != "execute":
            return error("Invalid isolated workspace output invocation")
        if recorded is not None and recorded.status != "started":
            return self._replay(recorded, tool_id=tool_id, name="execute")
        try:
            identity = await backend.aworkspace_output_identity()
            intent = WorkspaceExecuteIntent.bind(
                path=path,
                command=command,
                timeout=timeout,
                run_id=self._run_id,
                tool_call_id=tool_id,
                identity=identity,
            )
        except (ValueError, ExecutionBoundaryError):
            return error("Isolated workspace output is unavailable")
        key = intent.key

        recover = recorded is not None
        cleanup_id: str | None = None
        if recorded is not None:
            if WorkspaceExecuteIntent.from_journal(recorded.result) != intent:
                return self._replay(recorded, tool_id=tool_id, name="execute")
        else:
            won = await self._run_repository.journal_tool_started(
                self._run_id,
                self._lease,
                tool_id,
                "execute",
                intent=intent.model_dump_json(),
            )
            if not won:
                winner = await self._run_repository.get_tool_journal(
                    self._run_id, tool_id
                )
                return self._replay(winner, tool_id=tool_id, name="execute")
            cleanup = await self._run_repository.register_sandbox_cleanup(
                run_id=self._run_id,
                lease_generation=self._lease.generation,
                backend_kind="docker",
                sandbox_id=DOCKER_OUTPUT_ID_PREFIX + key,
                teardown_ref=DOCKER_OUTPUT_TEARDOWN_PREFIX + identity,
                defer_ms=600_000,
            )
            cleanup_id = cleanup.cleanup_id
        await _require_current_lease(self._run_repository, self._run_id, self._lease)
        try:
            with backend.capture_workspace_output(
                path, key, recover=recover
            ) as capture:
                native = await handler(
                    request.override(
                        tool_call={
                            **call,
                            "args": {"command": command, "timeout": timeout},
                        }
                    )
                )
            raw = (
                capture.content
                if isinstance(native, ToolMessage)
                and (native.name, native.tool_call_id, native.status)
                == ("execute", tool_id, "success")
                else None
            )
        except ExecutionBoundaryError:
            return self._replay(recorded, tool_id=tool_id, name="execute")
        if raw is None and recover:
            # A missing or running sandbox is not a success receipt. Preserve
            # started, and never run the old command to manufacture a result.
            return self._replay(recorded, tool_id=tool_id, name="execute")
        write = None if raw is None else NativeWorkspaceWrite(path, raw, script=True)
        result = (
            error("Isolated command did not produce the requested workspace file")
            if write is None
            else ToolMessage(
                content=write.acknowledgement,
                name="execute",
                tool_call_id=tool_id,
                status="success",
            )
        )
        if not await self._run_repository.journal_tool_finished(
            self._run_id,
            self._lease,
            tool_id,
            result.text,
            write is None,
            workspace_write=write,
        ):
            await _require_current_lease(
                self._run_repository, self._run_id, self._lease
            )
            result = error("The workspace output could not be retained")
            if write is None or not await self._run_repository.journal_tool_finished(
                self._run_id, self._lease, tool_id, result.text, True
            ):
                raise RunSupersededError("Workspace output journal no longer active")
        try:
            await backend.acleanup_workspace_output(key)
            if cleanup_id is not None:
                await self._run_repository.complete_sandbox_cleanup(cleanup_id)
        except Exception:
            _logger.warning("workspace output sandbox cleanup deferred to its owner")
        return result

    async def awrap_tool_call(
        self, request: ToolCallRequest, handler: _ToolHandler
    ) -> ToolMessage | Command[Any]:
        call = request.tool_call
        name = call["name"]
        await _require_current_lease(self._run_repository, self._run_id, self._lease)
        if name == "execute" and call["args"].get("output_path") is not None:
            return await self._execute_workspace_script(request, handler)
        if name in self._exempt:
            # 纯读/幂等 / Command 形态工具：不落 journal，直接放行（重执行安全）。
            return await handler(request)
        tool_id = call["id"] or ""
        recorded = await self._run_repository.get_tool_journal(self._run_id, tool_id)
        recovering_delivery = (
            name == "deliver"
            and recorded is not None
            and recorded.status == "started"
            and self._has_delivery_intent(recorded.result)
        )
        if recorded is not None and not recovering_delivery:
            return self._replay(recorded, tool_id=tool_id, name=name)
        won_journal = (
            recovering_delivery
            or await self._run_repository.journal_tool_started(
                self._run_id, self._lease, tool_id, name
            )
        )
        if not won_journal:
            # 首次读取与 keep-first 插入之间存在竞争窗口。输掉 journal 所有权的一方必须重新
            # 读取赢家结果并短路；即使赢家恰好清理了记录，也按 unknown-outcome fail closed，
            # 绝不能让两个 Worker 同时执行同一个非幂等副作用。
            recorded = await self._run_repository.get_tool_journal(
                self._run_id, tool_id
            )
            if recorded is None:
                await _require_current_lease(
                    self._run_repository, self._run_id, self._lease
                )
            return self._replay(
                recorded if recorded is not None else object(),
                tool_id=tool_id,
                name=name,
            )
        token = (
            _DELIVERY_CALL.set((self._run_id, tool_id)) if name == "deliver" else None
        )
        mcp_token = (
            _MCP_CALL.set((self._run_id, tool_id)) if name == "mcp_call" else None
        )
        write = self._native_write(request)
        edit_path = (
            native_output_path(request, self._workspace_thread, EditFileSchema)
            if self._workspace_edit
            and self._workspace_thread is not None
            and name == "edit_file"
            else None
        )
        try:
            with capture_output_edit(edit_path) as capture:
                result = await handler(request)
            if capture is not None:
                write = capture.postimage
        except GraphInterrupt:
            # MCP elicitation is inside an already-sent tools/call. Preserve started:
            # teardown cannot prove zero effects, so resume must not resend the call.
            # Other local pre-effect pauses retain their established re-entry behavior.
            if name != "mcp_call":
                await self._run_repository.clear_tool_journal(
                    self._run_id, self._lease, tool_id
                )
            raise
        finally:
            if token is not None:
                _DELIVERY_CALL.reset(token)
            if mcp_token is not None:
                _MCP_CALL.reset(mcp_token)
        if isinstance(result, ToolMessage):
            if name == "deliver" and result.text.startswith("error:"):
                result = result.model_copy(update={"status": "error"})
            # .text 是框架文本收窄口；Command 形态（状态更新）无文本结果可短路，留 started 行——
            # 重放守门对其保守判 unknown-outcome（非幂等 Command 副作用工具应入豁免表，此处不双写）。
            if write is not None and (
                (result.name, result.tool_call_id, result.status, result.text)
                != (write.tool_name, tool_id, "success", write.acknowledgement)
                or (
                    self._workspace_guard is not None
                    and not await self._workspace_guard(write.path, write.content)
                )
            ):
                write = None
            finished = await self._run_repository.journal_tool_finished(
                self._run_id,
                self._lease,
                tool_id,
                result.text,
                result.status == "error",
                **({"workspace_write": write} if write is not None else {}),
            )
            if not finished:
                raise RunSupersededError(
                    f"run {self._run_id!r} lost its lease while finishing a tool effect"
                )
        return result

    @staticmethod
    def _has_delivery_intent(result: str) -> bool:
        try:
            value = _DELIVERY_INTENT.validate_json(result)
        except ValidationError:
            return False
        return type(value.get("version")) is int and value["version"] == 2

    def _replay(self, recorded: object, *, tool_id: str, name: str) -> ToolMessage:
        # recorded: ToolJournalRecord（repository 层导出）——按状态短路。
        status = getattr(recorded, "status", "started")
        if status == "started":
            metrics.record_tool_unknown_outcome()
            result = getattr(recorded, "result", "")
            if name == "deliver" and isinstance(result, str) and result:
                try:
                    intent = _DELIVERY_INTENT.validate_json(result)
                except ValidationError:
                    intent = None
                if (
                    intent is not None
                    and type(intent.get("version")) is int
                    and intent["version"] == 1
                ):
                    return ToolMessage(
                        content=(
                            f"{_UNKNOWN_OUTCOME_PREFIX} "
                            "DELIVERY_INTENT_VERSION_UNSUPPORTED: durable delivery "
                            "intent version 1 requires operator reconciliation; not "
                            "re-executed automatically."
                        ),
                        tool_call_id=tool_id,
                        name=name,
                        status="error",
                    )
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
        if name == "deliver" and status == "succeeded" and not is_error:
            try:
                parsed = _DELIVERY_INTENT.validate_json(result)
                if parsed.get("status") != "delivered":
                    raise ValueError("delivery journal status is not delivered")
                DeliveryCreatedPayload.model_validate(
                    {key: value for key, value in parsed.items() if key != "status"}
                    | {"tool_call_id": tool_id}
                )
            except (ValidationError, ValueError) as exc:
                raise RuntimeError(
                    "successful delivery journal has no valid final receipt"
                ) from exc
        return ToolMessage(
            content=result,
            tool_call_id=tool_id,
            name=name,
            status="error" if is_error else "success",
        )


class SteeringMiddleware(AgentMiddleware):
    """运行中插话：模型轮前排空信箱，按到达序注入 HumanMessage——协作式转向，
    不打断进行中的工具；稳定 id=message_id 保 checkpoint 重放幂等。只挂主链。"""

    def __init__(
        self, *, run_repository: RunRepository, run_id: str, lease: LeaseFence
    ) -> None:
        super().__init__()
        self._run_repository = run_repository
        self._run_id = run_id
        self._lease = lease

    async def abefore_model(
        self, state: AgentState[Any], runtime: Runtime[Any]
    ) -> dict[str, Any] | None:
        await _require_current_lease(self._run_repository, self._run_id, self._lease)
        try:
            steers = await self._run_repository.peek_steers(self._run_id)
        except Exception:
            # A supplemental message does not decide the health of this Run.
            _logger.exception("steer mailbox read deferred run_id=%s", self._run_id)
            return None
        if not steers:
            return None
        for message_id, content in steers:
            if not content:
                raise ValueError(f"empty steer content: message_id={message_id!r}")
        seen_ids = {message.id for message in state["messages"]}
        # In-memory state only deduplicates. The independently committed
        # checkpoint settles the command and deletes the mailbox row.
        fresh = [(mid, content) for mid, content in steers if mid not in seen_ids]
        if not fresh:
            return None
        return {
            "messages": [
                HumanMessage(content=content, id=message_id)
                for message_id, content in fresh
            ]
        }
