"""Refresh native Skill metadata at graph entry, not at session creation."""

from __future__ import annotations

from deepagents.middleware.skills import (
    SkillsMiddleware,
    SkillsState,
    SkillsStateUpdate,
)
from langchain_core.runnables import RunnableConfig
from langgraph.runtime import Runtime


class RunSkillsMiddleware(SkillsMiddleware):
    """Keep the SDK parser/prompt while binding discovery to the current Run.

    A new graph entry must not inherit another Run's cached Skill selection.
    Native Command resume continues its checkpointed node without re-entering
    this hook, preserving same-Run HITL semantics and the existing guards.
    """

    @staticmethod
    def _unloaded(state: SkillsState) -> SkillsState:
        current = state.copy()
        current.pop("skills_metadata", None)
        current.pop("skills_load_errors", None)
        return current

    @staticmethod
    def _loaded(update: SkillsStateUpdate | None) -> SkillsStateUpdate:
        if update is None:
            raise RuntimeError("native Skill loader returned no state update")
        update.setdefault("skills_load_errors", [])
        return update

    def before_agent(
        self, state: SkillsState, runtime: Runtime, config: RunnableConfig
    ) -> SkillsStateUpdate:
        return self._loaded(
            super().before_agent(self._unloaded(state), runtime, config)
        )

    async def abefore_agent(
        self, state: SkillsState, runtime: Runtime, config: RunnableConfig
    ) -> SkillsStateUpdate:
        return self._loaded(
            await super().abefore_agent(self._unloaded(state), runtime, config)
        )
