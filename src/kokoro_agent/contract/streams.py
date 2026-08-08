# GENERATED — DO NOT EDIT. Kokoro Root authority: contract/spec/streams.yaml
# Root materialization (run from Kokoro Root): uv run --locked python contract/generate.py --output-root OUTPUT_ROOT
# Consumer regeneration (run from Kokoro Root): node contract/generate.mjs --consumer CONSUMER --source-root ROOT --output-repository CONSUMER_REPOSITORY
from __future__ import annotations

REQUESTS_STREAM = "kokoro:runs:requests"
CONSUMER_GROUP = "kokoro-agent"
REQUESTS_MAXLEN = 10000
RUN_EVENTS_MAXLEN = 10000
RUN_CONTROL_MAXLEN = 10000
LIVE_MAXLEN = 512
BLOCK_MS = 1000


def run_events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


def run_control_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:control"


def live_stream(session_id: str) -> str:
    return f"kokoro:session:{session_id}:live"


def event_id(run_id: str, index: int) -> str:
    return f"{run_id}:{index}"


def lease_key(run_id: str) -> str:
    return f"kokoro:agent:lease:{run_id}"
