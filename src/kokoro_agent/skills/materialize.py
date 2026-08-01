"""Pre-graph materialization for DeepAgents' native Skill middleware.

The Hub resolves an immutable run assembly. GA writes every exact package to a
content-addressed directory before ``create_deep_agent`` is called and passes
those directories through DeepAgents' native ``skills=`` parameter. No graph
middleware, checkpoint ledger, best-effort fallback, or stale directory scan is
part of this path.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass

from deepagents.backends.composite import CompositeBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.backends.state import StateBackend
from deepagents.backends.store import StoreBackend
from langgraph.store.memory import InMemoryStore

from kokoro_agent.contract import SkillGrant
from kokoro_agent.skills.hub import SkillHub, content_hash_of
from kokoro_agent.skills.supply import SKILLS_ROOT


@dataclass(frozen=True, slots=True)
class NativeSkillAssembly:
    """Exact native Skill inputs and the backend that owns their paths."""

    backend: BackendProtocol
    sources: tuple[str, ...]
    package_digest: str


async def materialize_native_skills(
    *,
    grants: Sequence[SkillGrant],
    hub: SkillHub,
    backend: BackendProtocol,
    namespace: str,
    run_id: str,
) -> NativeSkillAssembly:
    """Materialize the frozen Hub packages before graph construction.

    State mode cannot be mutated outside a running LangGraph node, so its
    workspace remains a native ``StateBackend`` while ``/.skills/`` is routed
    to a native ``StoreBackend`` backed by a graph-local in-memory store.
    Sandbox modes place packages in the sandbox itself so referenced scripts
    remain executable. Content-addressed source directories make stale files
    unreachable without destructive GC.
    """

    target = _backend_with_pregraph_skill_route(backend, namespace, run_id)
    digest_rows: list[dict[str, str]] = []
    packages: list[tuple[SkillGrant, dict[str, str]]] = []
    for grant in grants:
        files = await hub.load_package(grant.scope, grant.name, grant.content_hash)
        if content_hash_of(files) != grant.content_hash:
            raise RuntimeError(f"NATIVE_SKILL_CONTENT_LOCK_INVALID:{grant.name}")
        packages.append((grant, files))
        digest_rows.append(
            {
                "scope": grant.scope,
                "name": grant.name,
                "content_hash": grant.content_hash,
            }
        )
    package_digest = hashlib.sha256(
        json.dumps(
            digest_rows,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    source = f"{SKILLS_ROOT}assemblies/{package_digest}/"
    for grant, files in packages:
        payload = [
            (
                f"{source}{grant.name}/{relative_path}",
                files[relative_path].encode("utf-8"),
            )
            for relative_path in sorted(files)
        ]
        responses = await target.aupload_files(payload)
        if len(responses) != len(payload) or any(response.error for response in responses):
            raise RuntimeError(f"NATIVE_SKILL_MATERIALIZATION_FAILED:{grant.name}")
        roundtrip = await target.adownload_files([path for path, _content in payload])
        if len(roundtrip) != len(payload) or any(
            downloaded.error is not None or downloaded.content != expected
            for downloaded, (_path, expected) in zip(roundtrip, payload, strict=True)
        ):
            raise RuntimeError(f"NATIVE_SKILL_WRITE_INTEGRITY_FAILED:{grant.name}")
    return NativeSkillAssembly(
        backend=target,
        sources=(source,) if packages else (),
        package_digest=package_digest,
    )


def _backend_with_pregraph_skill_route(
    backend: BackendProtocol, namespace: str, run_id: str
) -> BackendProtocol:
    if not isinstance(backend, StateBackend):
        return backend
    store = InMemoryStore()
    skill_backend = StoreBackend(
        store=store,
        namespace=lambda _runtime: ("kokoro", "agent", namespace, run_id, "skills"),
    )
    return CompositeBackend(
        default=backend,
        routes={SKILLS_ROOT: skill_backend},
        artifacts_root="/",
    )
