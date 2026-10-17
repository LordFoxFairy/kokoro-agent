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
    _discoveries: tuple[SkillsMiddleware, ...] = ()

    @classmethod
    def for_selected(
        cls,
        *,
        backend: BackendProtocol,
        sources: Sequence[str],
        discovery_backends: Sequence[tuple[str, BackendProtocol]],
        progress: SkillProgressSink,
    ) -> RunSkillsMiddleware:
        middleware = cls(backend=backend, sources=sources)
        middleware._expected_skill_paths = frozenset(
            path for path, _ in discovery_backends
        )
        if len(middleware._expected_skill_paths) != len(discovery_backends):
            raise SkillClientError("SKILL_SOURCE_INVALID")
        middleware._discoveries = tuple(
            SkillsMiddleware(backend=discovery, sources=sources)
            for _, discovery in discovery_backends
        )
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

    @staticmethod
    def _merge(updates: Sequence[SkillsStateUpdate | None]) -> SkillsStateUpdate:
        """Names are display metadata, not precedence keys for exact selections."""
        result = SkillsStateUpdate(skills_metadata=[], skills_load_errors=[])
        for update in updates:
            if update is None:
                raise RuntimeError("native Skill loader returned no state update")
            result["skills_metadata"].extend(update.get("skills_metadata", []))
            result.setdefault("skills_load_errors", []).extend(
                update.get("skills_load_errors", [])
            )
        return result

    def before_agent(
        self, state: SkillsState, runtime: Runtime, config: RunnableConfig
    ) -> SkillsStateUpdate:
        """SDK-only sync hook; product Run progress uses async v3 below."""
        if self._discoveries:
            return self._loaded(
                self._merge(
                    [
                        discovery.before_agent(self._unloaded(state), runtime, config)
                        for discovery in self._discoveries
                    ]
                )
            )
        return self._loaded(
            super().before_agent(self._unloaded(state), runtime, config)
        )

    async def abefore_agent(
        self, state: SkillsState, runtime: Runtime, config: RunnableConfig
    ) -> SkillsStateUpdate:
        try:
            if self._discoveries:
                return self._loaded(
                    self._merge(
                        [
                            await discovery.abefore_agent(
                                self._unloaded(state), runtime, config
                            )
                            for discovery in self._discoveries
                        ]
                    )
                )
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
