"""Opaque execution-context authority over exact LangGraph checkpoint identities."""

from __future__ import annotations

import hashlib
import json
import secrets
from typing import Protocol

from langchain_core.runnables.config import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import RunRequest


class ExecutionContextConflict(ValueError):
    """A fail-closed context reference, binding, or continuation conflict."""


class ExecutionCheckpoint(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    thread_id: str = Field(min_length=1)
    checkpoint_ns: str
    checkpoint_id: str = Field(min_length=1)


class ExecutionContextBinding(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    namespace: str = Field(min_length=1)
    intent_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    physical_thread_id: str = Field(min_length=1)
    base_checkpoint: ExecutionCheckpoint | None = None
    active_checkpoint: ExecutionCheckpoint | None = None


class CompletedExecutionContext(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    anchor: str = Field(min_length=1, max_length=256)
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    owner_revision: int = Field(gt=0)
    checkpoint: ExecutionCheckpoint
    continuation_run_id: str | None = None


class CompletionEventDraft(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    kind: str = Field(min_length=1)
    index: int = Field(ge=0)
    timestamp: int
    payload_json: str = Field(min_length=1)


class DurableCompletionFrame(CompletionEventDraft):
    durable_seq: int = Field(gt=0)
    event_id: str = Field(min_length=1)


class ClaimedCompletionFrames(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    owner: DurableCompletionFrame
    terminal: DurableCompletionFrame


class ExecutionContextStore(Protocol):
    async def get_execution_context_binding(
        self, run_id: str
    ) -> ExecutionContextBinding | None: ...

    async def bind_execution_context(
        self, run_id: str, binding: ExecutionContextBinding
    ) -> ExecutionContextBinding: ...

    async def update_execution_checkpoint(
        self, run_id: str, checkpoint: ExecutionCheckpoint
    ) -> None: ...

    async def resolve_execution_parent(
        self,
        *,
        namespace: str,
        anchor: str,
        digest: str,
        continuation_run_id: str | None,
    ) -> ExecutionCheckpoint | None: ...

    async def try_complete_execution_context(
        self,
        completion: CompletedExecutionContext,
        owner_event: CompletionEventDraft,
        terminal_event: CompletionEventDraft,
    ) -> ClaimedCompletionFrames | None: ...


class ExecutionContextAuthorityPort(Protocol):
    async def open(self, request: RunRequest) -> RunnableConfig: ...

    async def config_for_run(self, run_id: str) -> RunnableConfig: ...

    async def capture(self, run_id: str) -> ExecutionCheckpoint: ...

    async def prepare_completion(self, run_id: str) -> CompletedExecutionContext: ...


class ExecutionContextAuthority:
    """The only component allowed to resolve opaque refs into checkpoint configs."""

    def __init__(
        self,
        *,
        store: ExecutionContextStore,
        checkpointer: BaseCheckpointSaver[str],
    ) -> None:
        self._store = store
        self._checkpointer = checkpointer

    async def open(self, request: RunRequest) -> RunnableConfig:
        intent_digest = _intent_digest(request)
        existing = await self._store.get_execution_context_binding(request.run_id)
        if existing is not None:
            if existing.namespace != request.context.namespace or existing.intent_digest != intent_digest:
                raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_CONFLICT")
            return _config(request.run_id, existing)

        intent = request.execution_context
        parent: ExecutionCheckpoint | None = None
        if intent.mode == "root":
            physical_thread_id = _physical_thread_id(request)
        else:
            parent = await self._store.resolve_execution_parent(
                namespace=request.context.namespace,
                anchor=intent.parent_anchor,
                digest=intent.parent_digest,
                continuation_run_id=request.run_id if intent.mode == "continue" else None,
            )
            if parent is None:
                raise ExecutionContextConflict("EXECUTION_CONTEXT_PARENT_UNAVAILABLE")
            physical_thread_id = parent.thread_id

        binding = await self._store.bind_execution_context(
            request.run_id,
            ExecutionContextBinding(
                namespace=request.context.namespace,
                intent_digest=intent_digest,
                physical_thread_id=physical_thread_id,
                base_checkpoint=parent,
            ),
        )
        if binding.namespace != request.context.namespace or binding.intent_digest != intent_digest:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_CONFLICT")
        return _config(request.run_id, binding)

    async def config_for_run(self, run_id: str) -> RunnableConfig:
        binding = await self._store.get_execution_context_binding(run_id)
        if binding is None:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_MISSING")
        return _config(run_id, binding)

    async def capture(self, run_id: str) -> ExecutionCheckpoint:
        binding = await self._store.get_execution_context_binding(run_id)
        if binding is None:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_MISSING")
        lookup: RunnableConfig = {"configurable": {"thread_id": binding.physical_thread_id}}
        checkpoint: ExecutionCheckpoint | None = None
        async for item in self._checkpointer.alist(
            lookup,
            filter={"kokoro_run_id": run_id},
            limit=1,
        ):
            checkpoint = _checkpoint(item.config)
            break
        if checkpoint is None:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_CHECKPOINT_NOT_FOUND")
        await self._store.update_execution_checkpoint(run_id, checkpoint)
        return checkpoint

    async def prepare_completion(self, run_id: str) -> CompletedExecutionContext:
        checkpoint = await self.capture(run_id)
        binding = await self._store.get_execution_context_binding(run_id)
        if binding is None:
            raise ExecutionContextConflict("EXECUTION_CONTEXT_BINDING_MISSING")
        anchor = f"ctx_{secrets.token_hex(16)}"
        digest = _completion_digest(binding.namespace, anchor, checkpoint)
        completion = CompletedExecutionContext(
            run_id=run_id,
            namespace=binding.namespace,
            anchor=anchor,
            digest=digest,
            owner_revision=1,
            checkpoint=checkpoint,
        )
        return completion


def _intent_digest(request: RunRequest) -> str:
    canonical = json.dumps(
        request.execution_context.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _physical_thread_id(request: RunRequest) -> str:
    material = "\0".join(
        (request.context.namespace, request.thread_id, request.run_id)
    ).encode()
    return f"kctx_{hashlib.sha256(material).hexdigest()}"


def _completion_digest(
    namespace: str, anchor: str, checkpoint: ExecutionCheckpoint
) -> str:
    canonical = json.dumps(
        {
            "anchor": anchor,
            "checkpoint_id": checkpoint.checkpoint_id,
            "checkpoint_ns": checkpoint.checkpoint_ns,
            "namespace": namespace,
            "owner_revision": 1,
            "thread_id": checkpoint.thread_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _checkpoint(config: RunnableConfig) -> ExecutionCheckpoint:
    configurable = config.get("configurable")
    if not isinstance(configurable, dict):
        raise ExecutionContextConflict("EXECUTION_CONTEXT_CHECKPOINT_INVALID")
    thread_id = configurable.get("thread_id")
    checkpoint_ns = configurable.get("checkpoint_ns", "")
    checkpoint_id = configurable.get("checkpoint_id")
    if (
        not isinstance(thread_id, str)
        or not thread_id
        or not isinstance(checkpoint_ns, str)
        or not isinstance(checkpoint_id, str)
        or not checkpoint_id
    ):
        raise ExecutionContextConflict("EXECUTION_CONTEXT_CHECKPOINT_INVALID")
    return ExecutionCheckpoint(
        thread_id=thread_id,
        checkpoint_ns=checkpoint_ns,
        checkpoint_id=checkpoint_id,
    )


def _config(run_id: str, binding: ExecutionContextBinding) -> RunnableConfig:
    checkpoint = binding.active_checkpoint or binding.base_checkpoint
    configurable: dict[str, object] = {"thread_id": binding.physical_thread_id}
    if checkpoint is not None:
        configurable.update(
            checkpoint_ns=checkpoint.checkpoint_ns,
            checkpoint_id=checkpoint.checkpoint_id,
        )
    return {
        "configurable": configurable,
        "metadata": {"kokoro_run_id": run_id},
    }
