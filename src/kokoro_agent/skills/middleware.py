"""Refresh native Skill metadata at graph entry, not at session creation."""

from __future__ import annotations

from collections.abc import Sequence

from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.skills import (
    SkillsMiddleware,
    SkillsState,
    SkillsStateUpdate,
)
from langchain_core.runnables import RunnableConfig
from langgraph.runtime import Runtime

from kokoro_agent.clients.skills import SkillClientError
from kokoro_agent.execution.events import (
    ProgressPersistenceError,
    confirm_skill_progress,
)
from kokoro_agent.protocol import SkillPhase, SkillProgressSink


class RunSkillsMiddleware(SkillsMiddleware):
    """Keep the SDK parser/prompt while binding discovery to the current Run.

    A new graph entry must not inherit another Run's cached Skill selection.
    Native Command resume continues its checkpointed node without re-entering
    this hook, preserving same-Run HITL semantics and the existing guards.
    """

    _expected_skill_paths: frozenset[str] | None = None
    _progress: SkillProgressSink | None = None

    @classmethod
    def for_selected(
        cls,
        *,
        backend: BackendProtocol,
        sources: Sequence[str],
        expected_skill_paths: Sequence[str],
        progress: SkillProgressSink,
    ) -> RunSkillsMiddleware:
        middleware = cls(backend=backend, sources=sources)
        middleware._expected_skill_paths = frozenset(expected_skill_paths)
        middleware._progress = progress
        return middleware

    @staticmethod
    def _unloaded(state: SkillsState) -> SkillsState:
        current = state.copy()
        current.pop("skills_metadata", None)
        current.pop("skills_load_errors", None)
        return current

    def _loaded(self, update: SkillsStateUpdate | None) -> SkillsStateUpdate:
        if update is None:
            raise RuntimeError("native Skill loader returned no state update")
        if self._expected_skill_paths is None:
            raise RuntimeError("native Skill selection was not bound")
        metadata = update.get("skills_metadata", [])
        if (
            len(metadata) != len(self._expected_skill_paths)
            or frozenset(item["path"] for item in metadata)
            != self._expected_skill_paths
        ):
            raise SkillClientError("SKILL_NATIVE_LOAD_INCOMPLETE")
        update.setdefault("skills_load_errors", [])
        return update

    def before_agent(
        self, state: SkillsState, runtime: Runtime, config: RunnableConfig
    ) -> SkillsStateUpdate:
        """SDK-only sync hook; product Run progress uses async v3 below."""
        return self._loaded(
            super().before_agent(self._unloaded(state), runtime, config)
        )

    async def abefore_agent(
        self, state: SkillsState, runtime: Runtime, config: RunnableConfig
    ) -> SkillsStateUpdate:
        try:
            return self._loaded(
                await super().abefore_agent(self._unloaded(state), runtime, config)
            )
        except ProgressPersistenceError:
            raise
        except Exception:
            if self._expected_skill_paths:
                if self._progress is None:
                    raise RuntimeError("native Skill progress was not bound") from None
                await confirm_skill_progress(
                    self._progress,
                    SkillPhase(phase="failed", error_code="skill_load_failed"),
                )
            raise
