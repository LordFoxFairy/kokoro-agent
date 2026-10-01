"""Native HITL evidence feasibility only: real saver, no Run recovery predicate.

Root supplies an owned PostgreSQL database. No Redis, provider, tool, or service
is called. The effect counter is a local marker, not an external-effect claim.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

from langchain_core.runnables import RunnableConfig
from langgraph._internal._constants import ERROR, INTERRUPT, NULL_TASK_ID, RESUME
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
import psycopg
import pytest
from typing_extensions import TypedDict

from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all


# Native overloads leave Command unbound in SDK stubs; keep that boundary local.
_native_graph: Any = StateGraph


class ProofState(TypedDict, total=False):
    answer: str
    second: str


@dataclass(frozen=True)
class WriteObservation:
    config: RunnableConfig
    task_id: str
    channels: tuple[str, ...]
    resume_values: tuple[Any, ...] | None
    input_resume: Any | None


class ObservedPostgresSaver(AsyncPostgresSaver):
    """Finite test observer; delegates all scheduling/serialization/SQL to native."""

    def __init__(self, conn: psycopg.AsyncConnection[Any]) -> None:
        super().__init__(conn)
        self.observations: list[WriteObservation] = []
        self.stages: list[
            tuple[str, RunnableConfig, str, tuple[str, ...], str | None]
        ] = []
        self.hold_task_resume = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        channels = tuple(channel for channel, _ in writes)
        configurable = config.get("configurable", {})
        locator: RunnableConfig = {
            "configurable": {
                key: value
                for key in ("thread_id", "checkpoint_ns", "checkpoint_id")
                if isinstance(value := configurable.get(key), str)
            }
        }
        # Finite fixture trace: no SDK runtime/callback/connection object traversal.
        self.stages.append(("received", locator, task_id, channels, None))
        try:
            received = next(
                (value for channel, value in writes if channel == RESUME), None
            )
            # These fixtures submit JSON values only; freeze their value before
            # any await, never confuse received arguments with committed facts.
            snapshot: Any = json.loads(json.dumps(received))
            observation = WriteObservation(
                locator,
                task_id,
                channels,
                tuple(cast(list[Any], snapshot))
                if task_id != NULL_TASK_ID and isinstance(snapshot, list)
                else None,
                cast(Any, snapshot) if task_id == NULL_TASK_ID else None,
            )
            if self.hold_task_resume and task_id != NULL_TASK_ID and RESUME in channels:
                self.entered.set()
                await self.release.wait()
            await super().aput_writes(config, writes, task_id, task_path)
        except BaseException as exc:
            self.stages.append(
                ("exception", locator, task_id, channels, type(exc).__name__)
            )
            raise
        self.stages.append(("delegated", locator, task_id, channels, None))
        self.observations.append(observation)


@asynccontextmanager
async def _saver(url: str, schema: str) -> AsyncGenerator[ObservedPostgresSaver, None]:
    async with connect_pg(url) as conn:
        await execute_sql(
            conn,
            f'SET search_path TO "{schema.replace(chr(34), chr(34) * 2)}"',
        )
        yield ObservedPostgresSaver(conn)


async def _writes(
    url: str, schema: str, config: RunnableConfig
) -> list[dict[str, Any]]:
    """A new autocommit connection, never the saver connection or its spy list."""
    locator = config.get("configurable", {})
    async with connect_pg(url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT task_id, channel FROM {qualified(schema, 'checkpoint_writes')} "
            "WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = %s "
            "ORDER BY task_id, idx",
            (
                locator["thread_id"],
                locator.get("checkpoint_ns", ""),
                locator["checkpoint_id"],
            ),
        )
        return [dict(row) for row in await fetch_all(cur)]


def _config() -> RunnableConfig:
    return {"configurable": {"thread_id": f"hitl-proof-{uuid4().hex}"}}


@pytest.mark.parametrize("boundary", ["healthy_node", "before_saver_commit"])
async def test_effect_marker_without_committed_resume_stays_unknown_without_redispatch(
    run_chat_database_url: str, run_chat_schema: str, boundary: str
) -> None:
    """Three absent reads are compatible with a live invocation, not safe replay."""
    entered, release = asyncio.Event(), asyncio.Event()
    effects: list[str] = []

    async def ask(state: ProofState) -> ProofState:
        answer = cast(str, interrupt({"request_id": "one"}))
        effects.append(answer)
        entered.set()
        if boundary == "healthy_node":
            await release.wait()
        return {"answer": answer}

    async with _saver(run_chat_database_url, run_chat_schema) as saver:
        graph = (
            _native_graph(ProofState)
            .add_node("ask", ask)
            .add_edge(START, "ask")
            .add_edge("ask", END)
            .compile(checkpointer=saver)
        )
        config = _config()
        await graph.ainvoke({}, config)
        paused = await graph.aget_state(config)
        task_id = paused.tasks[0].id
        saver.hold_task_resume = boundary == "before_saver_commit"
        running = asyncio.create_task(
            graph.ainvoke(Command(resume="one-effect"), config)
        )
        try:
            await asyncio.wait_for(entered.wait(), 5)
            if boundary == "before_saver_commit":
                await asyncio.wait_for(saver.entered.wait(), 5)
            for _ in range(3):
                rows = await _writes(
                    run_chat_database_url, run_chat_schema, paused.config
                )
                assert not running.done()
                assert effects == ["one-effect"]
                assert not any(
                    row["task_id"] == task_id and row["channel"] == RESUME
                    for row in rows
                )
                assert any(
                    row["task_id"] == NULL_TASK_ID and row["channel"] == RESUME
                    for row in rows
                )
            # Deliberately no second ainvoke: missing durable writes leave unknown.
        finally:
            release.set()
            saver.release.set()
            result = await asyncio.wait_for(running, 5)
        assert result == {"answer": "one-effect"}
        assert effects == ["one-effect"]
        rows = await _writes(run_chat_database_url, run_chat_schema, paused.config)
        assert any(
            row["task_id"] == task_id and row["channel"] == RESUME for row in rows
        )


@pytest.mark.parametrize("outcome", ["success", "error", "reask", "cancel"])
async def test_native_evidence_after_observation_loss_or_cancel_is_not_guessed(
    run_chat_database_url: str, run_chat_schema: str, outcome: str
) -> None:
    """Evidence reconstruction only; RESUME+ERROR/INTERRUPT is not active success."""
    entered, release = asyncio.Event(), asyncio.Event()
    effects: list[str] = []

    async def ask(state: ProofState) -> ProofState:
        answer = cast(str, interrupt({"request_id": "same-id"}))
        effects.append(answer)
        if outcome == "error":
            raise ValueError("native proof failure")
        if outcome == "reask":
            answer = cast(
                str,
                interrupt(
                    {"request_id": "same-id", "validation_error": "json_schema_invalid"}
                ),
            )
        if outcome == "cancel":
            entered.set()
            await release.wait()
        return {"answer": answer}

    async with _saver(run_chat_database_url, run_chat_schema) as saver:
        graph = (
            _native_graph(ProofState)
            .add_node("ask", ask)
            .add_edge(START, "ask")
            .add_edge("ask", END)
            .compile(checkpointer=saver)
        )
        config = _config()
        first = await graph.ainvoke({}, config)
        paused = await graph.aget_state(config)
        task_id = paused.tasks[0].id
        if outcome == "error":
            with pytest.raises(ValueError, match="native proof failure"):
                await graph.ainvoke(Command(resume="submitted"), config)
        elif outcome == "cancel":
            running = asyncio.create_task(
                graph.ainvoke(Command(resume="submitted"), config)
            )
            try:
                await asyncio.wait_for(entered.wait(), 5)
            finally:
                running.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await running
                release.set()
        else:
            result = await graph.ainvoke(Command(resume="submitted"), config)
            if outcome == "reask":
                assert result["__interrupt__"][0].value["request_id"] == "same-id"
                assert result["__interrupt__"][0].id == first["__interrupt__"][0].id
                assert (
                    result["__interrupt__"][0].value["validation_error"]
                    == "json_schema_invalid"
                )
            else:
                assert result == {"answer": "submitted"}
        assert effects == ["submitted"]
        if outcome != "cancel":
            assert any(
                o.task_id == task_id and RESUME in o.channels
                for o in saver.observations
            )
        saver.observations.clear()  # Bridge notification lost, native store untouched.
        rows = await _writes(run_chat_database_url, run_chat_schema, paused.config)
        task_channels = {row["channel"] for row in rows if row["task_id"] == task_id}
        if outcome == "cancel":
            assert task_channels == {INTERRUPT}
        else:
            assert RESUME in task_channels
        # Original INTERRUPT writes can coexist even on success. Only a causal
        # successor checkpoint and its task state disambiguate the latest result.
        async with _saver(run_chat_database_url, run_chat_schema) as reader:
            exact = await reader.aget_tuple(paused.config)
            assert exact is not None
            assert any(
                task == task_id and channel == RESUME
                for task, channel, _ in exact.pending_writes or ()
            ) == (outcome != "cancel")
            if outcome == "success":
                latest = await reader.aget_tuple(config)
                assert latest is not None and latest.parent_config == paused.config
                assert latest.checkpoint["channel_values"]["answer"] == "submitted"
            elif outcome == "error":
                assert ERROR in task_channels
            elif outcome == "cancel":
                latest = await reader.aget_tuple(config)
                assert latest is not None and latest.config == paused.config
                assert (await graph.aget_state(config)).tasks
            else:
                assert INTERRUPT in task_channels
        assert saver.observations == []
        assert effects == ["submitted"]  # No replay invocation to reconstruct facts.


async def test_multi_interrupt_map_preserves_child_namespace_in_postgres(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    def ask(state: ProofState) -> ProofState:
        return {"answer": cast(str, interrupt({"request_id": "nested"}))}

    def second(state: ProofState) -> ProofState:
        return {"second": cast(str, interrupt({"request_id": "second"}))}

    child = (
        _native_graph(ProofState)
        .add_node("ask", ask)
        .add_edge(START, "ask")
        .add_edge("ask", END)
        .compile()
    )
    async with _saver(run_chat_database_url, run_chat_schema) as saver:
        graph = (
            _native_graph(ProofState)
            .add_node("child", child)
            .add_node("second_node", second)
            .add_edge(START, "child")
            .add_edge(START, "second_node")
            .add_edge("child", END)
            .add_edge("second_node", END)
            .compile(checkpointer=saver)
        )
        config = _config()
        paused = await graph.ainvoke({}, config)
        assert len(paused["__interrupt__"]) == 2
        assert not [stage for stage in saver.stages if stage[0] == "exception"]
        expected_ids = {item.id for item in paused["__interrupt__"]}
        nested_ids = {
            item.id
            for item in paused["__interrupt__"]
            if item.value["request_id"] == "nested"
        }
        root_committed: set[str] = set()
        child_committed: set[str] = set()
        initial_interrupts = [o for o in saver.observations if INTERRUPT in o.channels]
        # A returned GraphInterrupt can mask a background saver failure. Prove
        # the exact root/child pause rows on independent connections BEFORE resume.
        for observed in initial_interrupts:
            async with _saver(run_chat_database_url, run_chat_schema) as reader:
                exact = await reader.aget_tuple(observed.config)
                assert exact is not None
                ids = {
                    item.id
                    for task, channel, value in exact.pending_writes or ()
                    if task == observed.task_id and channel == INTERRUPT
                    for item in value
                }
                assert ids and ids <= expected_ids
                namespace = observed.config.get("configurable", {})["checkpoint_ns"]
                if namespace == "":
                    root_committed.update(ids)
                else:
                    assert namespace.startswith("child:")
                    assert ids == nested_ids
                    child_committed.update(ids)
        assert root_committed == expected_ids
        assert child_committed == nested_ids and len(nested_ids) == 1
        saver.observations.clear()
        result = await graph.ainvoke(
            Command(
                resume={
                    item.id: item.value["request_id"]
                    for item in paused["__interrupt__"]
                }
            ),
            config,
        )
        assert not [stage for stage in saver.stages if stage[0] == "exception"]
        assert result == {"answer": "nested", "second": "second"}
        consumed = [o for o in saver.observations if RESUME in o.channels]
        assert consumed and all(o.task_id != NULL_TASK_ID for o in consumed)
        assert any(
            str(o.config.get("configurable", {})["checkpoint_ns"]).startswith("child:")
            for o in consumed
        )
        for observed in consumed:
            rows = await _writes(
                run_chat_database_url, run_chat_schema, observed.config
            )
            assert any(
                row["task_id"] == observed.task_id and row["channel"] == RESUME
                for row in rows
            )


async def test_repeated_validation_success_retains_prior_pg_resume_vector_after_observation_loss(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    """Exact PG characterization: mixed batch return is not an UPSERT guarantee.

    There is no Agent recovery predicate here. Incomplete consumed-vector proof
    remains unknown even when this isolated graph has a successful successor.
    """
    completions: list[str] = []

    def ask(state: ProofState) -> ProofState:
        value = cast(str, interrupt({"request_id": "repeat"}))
        while value != "valid":
            value = cast(
                str,
                interrupt(
                    {"request_id": "repeat", "validation_error": "json_schema_invalid"}
                ),
            )
        completions.append(value)
        return {"answer": value}

    async with _saver(run_chat_database_url, run_chat_schema) as saver:
        graph = (
            _native_graph(ProofState)
            .add_node("ask", ask)
            .add_edge(START, "ask")
            .add_edge("ask", END)
            .compile(checkpointer=saver)
        )
        config = _config()
        initial = await graph.ainvoke({}, config)
        paused = await graph.aget_state(config)
        task_id = paused.tasks[0].id
        interrupt_id = initial["__interrupt__"][0].id
        for count in (1, 2):
            result = await graph.ainvoke(Command(resume="invalid"), config)
            assert result["__interrupt__"][0].id == interrupt_id
            assert result["__interrupt__"][0].value == {
                "request_id": "repeat",
                "validation_error": "json_schema_invalid",
            }
            assert (await graph.aget_state(config)).config == paused.config
            async with _saver(run_chat_database_url, run_chat_schema) as reader:
                exact = await reader.aget_tuple(paused.config)
                assert exact is not None
                assert [
                    value
                    for task, channel, value in exact.pending_writes or ()
                    if task == task_id and channel == RESUME
                ] == [["invalid"] * count]
        saver.observations.clear()
        assert await graph.ainvoke(Command(resume="valid"), config) == {
            "answer": "valid"
        }
        # The delegate really received the full vector and returned successfully.
        submitted = [
            o
            for o in saver.observations
            if o.task_id == task_id and RESUME in o.channels
        ]
        assert len(submitted) == 1
        assert submitted[0].resume_values == ("invalid", "invalid", "valid")
        assert "answer" in submitted[0].channels
        inputs = [
            o
            for o in saver.observations
            if o.task_id == NULL_TASK_ID and RESUME in o.channels
        ]
        assert len(inputs) == 1
        assert inputs[0].input_resume == "valid"
        assert inputs[0].resume_values is None
        assert submitted[0].input_resume is None
        saver.observations.clear()
        async with _saver(run_chat_database_url, run_chat_schema) as reader:
            exact = await reader.aget_tuple(paused.config)
            latest = await reader.aget_tuple(config)
            assert exact is not None and latest is not None
            persisted = [
                value
                for task, channel, value in exact.pending_writes or ()
                if task == task_id and channel == RESUME
            ]
            assert persisted == [["invalid", "invalid"]]
            assert persisted != [["invalid", "invalid", "valid"]]
            assert latest.parent_config == paused.config
            assert latest.checkpoint["channel_values"]["answer"] == "valid"
            assert latest.config != paused.config
        assert (await graph.aget_state(config)).tasks == ()
        assert saver.observations == []
        assert completions == ["valid"]
        # No re-invocation to invent missing consumption evidence or repeat effects.
