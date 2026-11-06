"""Read a native final from a bounded, preflighted root lineage; never run a graph."""

from __future__ import annotations

import hashlib
import importlib.metadata
from pathlib import Path
import deepagents.graph
import json
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from kokoro_agent.domain.chat.projection import assistant_message_id
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.execution.runtime_profile import canonical_json
from kokoro_agent.infrastructure.postgres import qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one, fetch_all

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Required, get_args, get_origin, get_type_hints

from deepagents.graph import DeepAgentState
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langgraph.channels.base import BaseChannel
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.pregel import _checkpoint
from pydantic import TypeAdapter
from psycopg import AsyncConnection
from kokoro_agent.protocol import RunRequest

MAX_FINAL_LOCATORS = 128


@dataclass(frozen=True, slots=True)
class NativeFinal:
    message_id: str
    text: str


def final_lineage_tip(parents: Mapping[str, str | None]) -> str | None:
    """A missing/cross-Run parent is unknown, never permission to scan ancestors."""
    if not parents or len(parents) > MAX_FINAL_LOCATORS:
        return None
    tips = set(parents) - set(parents.values())
    if len(tips) != 1:
        return None
    tip = next(iter(tips))
    cursor: str | None = tip
    seen: set[str] = set()
    while cursor is not None:
        if cursor in seen or cursor not in parents:
            return None
        seen.add(cursor)
        cursor = parents[cursor]
    return tip if seen == set(parents) else None


class _LineageReader(BaseCheckpointSaver[str]):
    """Use the SDK's parent walk, not PostgresSaver's full-thread history fast path.

    The caller owns the read-only transaction and ALL tuple byte preflights.
    This adapter exposes only get; no checkpoint write or list operation exists.
    """

    def __init__(
        self,
        get_tuple: Callable[[RunnableConfig], Awaitable[CheckpointTuple | None]],
        thread_id: str,
        run_id: str,
        parents: Mapping[str, str | None],
    ) -> None:
        super().__init__()
        self._get = get_tuple
        self._thread = thread_id
        self._run = run_id
        self._parents = dict(parents)
        self._cache: dict[str, CheckpointTuple] = {}

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        values = config.get("configurable", {})
        key = values.get("checkpoint_id")
        if (
            values.get("thread_id") != self._thread
            or values.get("checkpoint_ns", "") != ""
            or not isinstance(key, str)
            or key not in self._parents
        ):
            raise ValueError("final checkpoint lookup outside approved lineage")
        if key in self._cache:
            return self._cache[key]
        saved = await self._get(config)
        if saved is None:
            raise ValueError("final checkpoint disappeared")
        parent = (
            saved.parent_config.get("configurable", {}) if saved.parent_config else None
        )
        actual = saved.config.get("configurable", {})
        if (
            saved.checkpoint["id"] != key
            or saved.metadata.get("kokoro_run_id") != self._run
            or actual.get("thread_id") != self._thread
            or actual.get("checkpoint_ns", "") != ""
            or actual.get("checkpoint_id") != key
            or (parent.get("checkpoint_id") if parent else None) != self._parents[key]
            or (
                parent is not None
                and (
                    parent.get("thread_id") != self._thread
                    or parent.get("checkpoint_ns", "") != ""
                )
            )
        ):
            raise ValueError("final checkpoint identity differs from preflight")
        self._cache[key] = saved
        return saved


async def restore_native_final(
    *,
    get_tuple: Callable[[RunnableConfig], Awaitable[CheckpointTuple | None]],
    thread_id: str,
    run_id: str,
    parents: Mapping[str, str | None],
    terminal_at: datetime,
) -> NativeFinal | None:
    """Native state proof only. The owner must also verify public identity/body.

    Invoke only after completed Run, recipe compatibility and byte preflight.
    Unknown terminal shape returns no proof. I/O/corrupt identity propagates.
    """
    tip = final_lineage_tip(parents)
    if tip is None:
        return None
    reader = _LineageReader(get_tuple, thread_id, run_id, parents)
    config: RunnableConfig = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
            "checkpoint_id": tip,
        }
    }
    saved = await reader.aget_tuple(config)
    if saved is None:
        raise ValueError("final checkpoint missing")
    values = saved.checkpoint["channel_values"]
    timestamp = datetime.fromisoformat(saved.checkpoint["ts"])
    if (
        terminal_at.tzinfo is None
        or timestamp.tzinfo is None
        or timestamp > terminal_at
        or values.get("__pregel_tasks") != []
        or any(key == "__start__" or key.startswith("branch:") for key in values)
        or saved.pending_writes
        or saved.checkpoint.get("updated_channels")
    ):
        return None
    annotation = get_type_hints(DeepAgentState, include_extras=True)["messages"]
    if get_origin(annotation) is Required:
        annotation = get_args(annotation)[0]
    channel = get_args(annotation)[1]
    if not isinstance(channel, BaseChannel):
        raise ValueError("native messages channel differs")
    # Upstream channel generics are erased in the installed SDK stubs.
    hydrate: Any = getattr(_checkpoint, "achannels_from_checkpoint")
    restored: Any = await hydrate(
        {"messages": channel},
        saved.checkpoint,
        saver=reader,
        config=config,
    )
    messages: object = restored[0]["messages"].get()
    if not isinstance(messages, list) or not messages:
        return None
    final: object = TypeAdapter(list[Any]).validate_python(messages)[-1]
    if (
        not isinstance(final, AIMessage)
        or not final.id
        or not final.id.strip()
        or final.tool_calls
        or final.invalid_tool_calls
        or not final.text.strip()
    ):
        return None
    return NativeFinal(message_id=final.id, text=final.text)


# The two graph files differ only in documentation; the messages declaration and
# reducer are byte-identical. This is a read compatibility boundary, not an
# execution recovery profile or permission to load code recorded in a recipe.
_FINAL_GRAPH_HASHES = {
    "0.6.6": "f342ed28dd9ed9768c8927848fd50bdb9e86145b33c2928120b4272cfdb2c6f7",
    "0.6.7": "3f68528ca70489fe9a8ee1ed14095aa09648af62059188006b714a42b6308bb3",
}
_FINAL_REDUCER_HASH = "4539763f8d47a4894fac9998d68b3fe0aa7eb369425f8dab08fc7cf9650aa681"
# ff367592 and the current assembly both use the default root state. Their
# middleware/display differences do not alter the messages checkpoint reducer.
_FINAL_FACTORY_HASHES = {
    "0742b5f34218d3b5fb77b14cefae391a3862d5059fdafa17fd4d37f757ddbe55",
    "1f0bee0e5d5c10e2e8954179826d6391cb0ecf3d47afdba2ef00a1b6543263df",
}
_FINAL_DEPENDENCIES = {
    "langchain": "1.3.2",
    "langchain-core": "1.4.0",
    "langgraph": "1.2.2",
    "langgraph-checkpoint": "4.1.1",
    "langgraph-checkpoint-postgres": "3.1.2",
    "ormsgpack": "1.12.2",
}


def supports_final_messages(recipe: Mapping[str, Any]) -> bool:
    """Accept only reviewed default-state construction and installed read semantics."""

    try:
        mapping = TypeAdapter(dict[str, Any])
        feature = mapping.validate_python(recipe.get("feature"), strict=True)
        peers = TypeAdapter(list[dict[str, Any]]).validate_python(
            feature.get("agents"), strict=True
        )
        implementation = mapping.validate_python(
            recipe.get("implementations"), strict=True
        )
        pairs = TypeAdapter(list[list[str]]).validate_python(
            implementation.get("dependencies"), strict=True
        )
        if len(peers) != 1 or any(len(pair) != 2 for pair in pairs):
            return False
        dependencies = {pair[0]: pair[1] for pair in pairs}
        if len(dependencies) != len(pairs):
            return False
        version = dependencies.get("deepagents", "")
        if version not in _FINAL_GRAPH_HASHES or any(
            dependencies.get(name) != value
            for name, value in _FINAL_DEPENDENCIES.items()
        ):
            return False
        sources = TypeAdapter(list[dict[str, Any]]).validate_python(
            implementation.get("sources"), strict=True
        )
        for source_id, package, path, expected in (
            ("assembly", "kokoro_agent", "agent_factory.py", _FINAL_FACTORY_HASHES),
            ("native", "deepagents", "graph.py", {_FINAL_GRAPH_HASHES[version]}),
            ("native", "deepagents", "_messages_reducer.py", {_FINAL_REDUCER_HASH}),
        ):
            selected = [
                source
                for source in sources
                if source.get("source_id") == source_id
                and source.get("package") == package
            ]
            if len(selected) != 1:
                return False
            files = TypeAdapter(list[dict[str, str]]).validate_python(
                selected[0].get("files"), strict=True
            )
            hashes = [item.get("sha256") for item in files if item.get("path") == path]
            if len(hashes) != 1 or hashes[0] not in expected:
                return False
        installed_version = importlib.metadata.version("deepagents")
        if installed_version not in _FINAL_GRAPH_HASHES or any(
            importlib.metadata.version(name) != value
            for name, value in _FINAL_DEPENDENCIES.items()
        ):
            return False
        graph_path = Path(deepagents.graph.__file__)
        return (
            hashlib.sha256(graph_path.read_bytes()).hexdigest()
            == _FINAL_GRAPH_HASHES[installed_version]
            and hashlib.sha256(
                graph_path.with_name("_messages_reducer.py").read_bytes()
            ).hexdigest()
            == _FINAL_REDUCER_HASH
        )
    except (ValueError, TypeError, OSError, importlib.metadata.PackageNotFoundError):
        return False


async def read_final_message_id(
    conn: AsyncConnection[Any],
    schema: str,
    request: RunRequest,
    reserve: Callable[[int], None],
) -> str | None:
    """Owner-only adjunct, inside the display reader's RR read-only transaction.

    No model/tool/graph construction, no event mutation, no arbitrary thread scan.
    The existing display request owns cancellation and the absolute deadline.
    """

    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    claims = qualified(schema, "kokoro_agent_run")
    events = qualified(schema, "kokoro_agent_chat_event")
    checkpoints = qualified(schema, "checkpoints")
    blobs = qualified(schema, "checkpoint_blobs")
    writes = qualified(schema, "checkpoint_writes")
    args = (tenant, scope.namespace, scope.session_id, request.run_id)
    async with conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT current_setting('transaction_read_only') AS ro, current_setting('transaction_isolation') AS isolation",
        )
        transaction = await fetch_one(cur)
        if transaction != {"ro": "on", "isolation": "repeatable read"}:
            raise ValueError("final read requires the display read-only snapshot")
        await execute_sql(
            cur,
            f"SELECT COUNT(*) AS count, COALESCE(SUM(octet_length(payload_json)),0) AS bytes FROM {events} WHERE tenant_id=%s AND namespace=%s AND session_id=%s AND run_id=%s AND event_type IN ('run.completed','run.failed')",
            args,
        )
        terminal_size = await fetch_one(cur)
        if terminal_size is None or terminal_size["count"] != 1:
            return None
        reserve(int(terminal_size["bytes"]))
        await execute_sql(
            cur,
            f"SELECT event_type,payload_json FROM {events} WHERE tenant_id=%s AND namespace=%s AND session_id=%s AND run_id=%s AND event_type IN ('run.completed','run.failed')",
            args,
        )
        terminal = await fetch_one(cur)
        if terminal is None or terminal["event_type"] != "run.completed":
            return None
        terminal_payload = json.loads(terminal["payload_json"])
        if terminal_payload.get("status") != "completed":
            return None
        # New Runs already have a durable binding. Never replace it with a
        # reconstructed adjunct, even if the checkpoint has since changed.
        if "final_message_id" in terminal_payload:
            return None
        await execute_sql(
            cur,
            f"SELECT terminal,terminal_at,octet_length(assembly_recipe_bytes)+COALESCE(octet_length(request_json),0) AS bytes FROM {claims} WHERE run_id=%s AND tenant_id=%s",
            (request.run_id, tenant),
        )
        size = await fetch_one(cur)
        if (
            size is None
            or not size["terminal"]
            or size["terminal_at"] is None
            or size["bytes"] is None
        ):
            return None
        reserve(int(size["bytes"]))
        terminal_at = size["terminal_at"]
        await execute_sql(
            cur,
            f"SELECT assembly_recipe_bytes,assembly_recipe_fingerprint,request_json FROM {claims} WHERE run_id=%s AND tenant_id=%s",
            (request.run_id, tenant),
        )
        claim = await fetch_one(cur)
        if (
            claim is None
            or RunRequest.model_validate_json(claim["request_json"]) != request
        ):
            raise ValueError("final frozen request differs")
        raw = bytes(claim["assembly_recipe_bytes"])
        recipe = TypeAdapter(dict[str, Any]).validate_json(raw, strict=True)
        if (
            hashlib.sha256(raw).hexdigest() != claim["assembly_recipe_fingerprint"]
            or canonical_json(recipe) != raw
            or recipe.get("domain_tag") != "kokoro-agent:assembly-recipe:1"
        ):
            raise ValueError("final frozen recipe differs")
        if not supports_final_messages(recipe):
            return None
        # Scalar IDs/parents/sizes only: no native value is loaded before reserve.
        await execute_sql(
            cur,
            f"""SELECT c.checkpoint_id,c.parent_checkpoint_id,c.checkpoint->>'v' AS format_version,
          octet_length(c.checkpoint::text)+octet_length(c.metadata::text)
          +octet_length(c.thread_id)+octet_length(c.checkpoint_ns)+octet_length(c.checkpoint_id)+COALESCE(octet_length(c.parent_checkpoint_id),0)
          +COALESCE((SELECT SUM(COALESCE(octet_length(b.blob),0)+octet_length(b.type)+octet_length(b.channel))
            FROM {blobs} b JOIN jsonb_each_text(c.checkpoint->'channel_versions') v ON b.channel=v.key AND b.version=v.value
            WHERE b.thread_id=c.thread_id AND b.checkpoint_ns=c.checkpoint_ns),0)
          +COALESCE((SELECT SUM(octet_length(w.blob)+COALESCE(octet_length(w.type),0)+octet_length(w.channel)+octet_length(w.task_id)+octet_length(w.task_path))
            FROM {writes} w WHERE w.thread_id=c.thread_id AND w.checkpoint_ns=c.checkpoint_ns AND w.checkpoint_id=c.checkpoint_id),0) AS bytes
          FROM {checkpoints} c WHERE c.thread_id=%s AND c.checkpoint_ns='' AND c.metadata->>'kokoro_run_id'=%s
          ORDER BY c.checkpoint_id LIMIT 129""",
            (scope.scoped_thread_id, request.run_id),
        )
        locators = await fetch_all(cur)
        if len(locators) > MAX_FINAL_LOCATORS:
            raise ValueError("final checkpoint locator budget exceeded")
        # Older saver formats issue additional pending-send reads outside the
        # root-namespace preflight. They are not part of this reviewed reader.
        if any(row["format_version"] != "4" for row in locators):
            return None
        parents = {
            str(row["checkpoint_id"]): row["parent_checkpoint_id"] for row in locators
        }
        if final_lineage_tip(parents) is None:
            return None
        reserve(sum(int(row["bytes"]) for row in locators))
        saver_constructor: Any = AsyncPostgresSaver
        saver: BaseCheckpointSaver[str] = saver_constructor(conn)
        native = await restore_native_final(
            get_tuple=saver.aget_tuple,
            thread_id=scope.scoped_thread_id,
            run_id=request.run_id,
            parents=parents,
            terminal_at=terminal_at,
        )
        if native is None:
            return None
        public_id = assistant_message_id(
            scope.namespace, request.run_id, native.message_id
        )
        await execute_sql(
            cur,
            f"SELECT COUNT(*) AS count,COALESCE(SUM(octet_length(payload_json)),0) AS bytes FROM {events} WHERE tenant_id=%s AND namespace=%s AND session_id=%s AND run_id=%s AND chat_message_id=%s AND event_type='assistant.completed'",
            (*args, public_id),
        )
        completed_size = await fetch_one(cur)
        if completed_size is None or completed_size["count"] != 1:
            return None
        reserve(int(completed_size["bytes"]))
        await execute_sql(
            cur,
            f"SELECT payload_json FROM {events} WHERE tenant_id=%s AND namespace=%s AND session_id=%s AND run_id=%s AND chat_message_id=%s AND event_type='assistant.completed'",
            (*args, public_id),
        )
        completed = await fetch_one(cur)
        if (
            completed is None
            or json.loads(completed["payload_json"]).get("content") != native.text
        ):
            return None
        return public_id
