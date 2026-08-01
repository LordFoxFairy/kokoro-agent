"""Narrow public boundary for dormant internal AG-UI event candidates."""

from kokoro_agent.presentation.adapter import (
    CandidateProtocolError,
    build_agui_candidate,
    map_agent_event_candidates,
)
from kokoro_agent.presentation.candidate import (
    AgentAguiCandidateRoute,
    AgentAguiCandidateSource,
    AgentAguiEventCandidate,
)
from kokoro_agent.presentation.profile import (
    AGUI_CANDIDATE_PROFILE_REVISION,
    AGUI_UPSTREAM_COMMIT,
    AGUI_UPSTREAM_PYTHON_VERSION,
)

__all__ = [
    "AGUI_CANDIDATE_PROFILE_REVISION",
    "AGUI_UPSTREAM_COMMIT",
    "AGUI_UPSTREAM_PYTHON_VERSION",
    "AgentAguiCandidateRoute",
    "AgentAguiCandidateSource",
    "AgentAguiEventCandidate",
    "CandidateProtocolError",
    "build_agui_candidate",
    "map_agent_event_candidates",
]
