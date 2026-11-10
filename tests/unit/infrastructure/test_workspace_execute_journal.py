"""The private script intent and delayed sandbox cleanup use existing owner rows."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from hashlib import sha256
import json

import pytest

from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.domain.run.workspace_output import (
    NativeWorkspaceWrite,
    WorkspaceExecuteIntent,
)
from kokoro_agent.infrastructure import postgres_run_effects, postgres_run_sandbox
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)


class Database:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []
        self.rows: deque[dict[str, object] | None] = deque()

    @asynccontextmanager
    async def connect(self, _: str) -> AsyncGenerator[Database]:
        yield self

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield

    @asynccontextmanager
    async def cursor(self) -> AsyncGenerator[Database]:
        yield self

    async def execute(self, query: str, params: object | None = None) -> None:
        self.calls.append((query, params))

    async def fetchone(self) -> dict[str, object] | None:
        return self.rows.popleft() if self.rows else None


async def test_execute_freezes_only_the_exact_started_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database()
    monkeypatch.setattr(postgres_run_effects, "connect_pg", database.connect)

    async def lease_current(
        _context: PostgresRunRepositoryContext,
        _cursor: object,
        run: str,
        lease: LeaseFence,
    ) -> bool:
        return run == "run" and lease.generation == 2

    monkeypatch.setattr(
        PostgresRunRepositoryContext, "lock_active_lease", lease_current
    )
    effects = postgres_run_effects.PostgresRunEffects(
        PostgresRunRepositoryContext(
            "postgresql://fixture", ttl_ms=1000, schema="fixture"
        )
    )
    lease = LeaseFence(owner="current", generation=2)
    path = "/outputs/report.txt"
    intent = WorkspaceExecuteIntent.bind(
        path=path,
        command="printf '报告'",
        timeout=7,
        run_id="run",
        tool_call_id="call",
        identity="owner-fence",
    )
    raw = intent.model_dump_json()
    assert raw == json.dumps(
        {
            "path": path,
            "command_sha256": sha256(rb"""["printf '\u62a5\u544a'",7]""").hexdigest(),
            "key": sha256(
                b'["docker-workspace-output:v1","run","call","owner-fence"]'
            ).hexdigest(),
        },
        separators=(",", ":"),
    )
    database.rows.append({"tool_call_id": "call"})
    assert await effects.journal_tool_started(
        "run", lease, "call", "execute", intent=raw
    )
    assert database.calls[-1][1] == ("run", "call", "execute", raw)
    for name, invalid in (
        ("edit_file", raw),
        ("execute", raw.replace(path, "/outputs/nested/report.txt")),
        ("execute", raw[:-1] + ',"path":"/outputs/duplicate.txt"}'),
    ):
        before = len(database.calls)
        assert not await effects.journal_tool_started(
            "run", lease, "call", name, intent=invalid
        )
        assert len(database.calls) == before

    def script_write(target: str) -> NativeWorkspaceWrite:
        return NativeWorkspaceWrite(path=target, content=b"original", script=True)

    async def finish(target: str, owner: LeaseFence = lease) -> bool:
        write = script_write(target)
        return await effects.journal_tool_finished(
            "run", owner, "call", write.acknowledgement, False, workspace_write=write
        )

    assert not await finish(path, LeaseFence(owner="old", generation=1))
    for started in ("", raw):
        database.rows.append({"result": started, "has_capacity": True})
        before = len(database.calls)
        assert not await finish("/outputs/wrong.txt")
        assert len(database.calls) == before + 1  # Only the locked intent was read.

    database.rows.append({"result": raw, "has_capacity": False})
    before = len(database.calls)
    assert not await finish(path)
    assert len(database.calls) == before + 1  # No success UPDATE or version INSERT.
    assert database.calls[-1][1] == ("run", 128, "run", "call")

    database.rows.extend(
        [{"result": raw, "has_capacity": True}, {"tool_call_id": "call"}]
    )
    assert await finish(path)
    assert (
        script_write(path).acknowledgement
        == "Captured workspace output /outputs/report.txt"
    )
    version_id = (
        "wsv_"
        + sha256(
            b'["native-workspace-script:v1","run","call","/outputs/report.txt"]'
        ).hexdigest()
    )
    assert database.calls[-1][1] == (
        "run",
        version_id,
        2,
        "call",
        path,
        8,
        sha256(b"original").hexdigest(),
        b"original",
        "run",
        128,
    )


async def test_cleanup_defer_changes_only_first_claim_due_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database()
    monkeypatch.setattr(postgres_run_sandbox, "connect_pg", database.connect)
    store = postgres_run_sandbox.PostgresRunSandbox(
        PostgresRunRepositoryContext(
            "postgresql://fixture",
            ttl_ms=1000,
            schema="fixture",
            clock=lambda: 2_000,
        )
    )
    for delay in (50, 0):
        database.rows.append(
            {
                "cleanup_id": f"cleanup-{delay}",
                "run_id": "run",
                "lease_generation": 1,
                "backend_kind": "docker",
                "sandbox_id": f"sandbox-{delay}",
                "teardown_ref": "docker:v1",
                "attempt_count": 0,
                "next_attempt_at": datetime.fromtimestamp((2000 + delay) / 1000, UTC),
            }
        )
        intent = await store.register_sandbox_cleanup(
            run_id="run",
            lease_generation=1,
            backend_kind="docker",
            sandbox_id=f"sandbox-{delay}",
            teardown_ref="docker:v1",
            **({"defer_ms": delay} if delay else {}),
        )
        params = database.calls[-2][1]
        assert isinstance(params, tuple)
        assert params[-3:] == (2000 + delay, 2000, 2000)
        assert intent.next_attempt_at == 2000 + delay
