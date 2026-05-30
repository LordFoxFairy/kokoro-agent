"""A1 SPIKE probe — observe real astream_events(v2) event shapes from DeepAgents.

Run: uv run python scripts/probe_deepagents.py

This is a throwaway diagnostic — it will be deleted in A5 once the findings
are recorded as comments in run_agent.py.
"""
from __future__ import annotations

import asyncio
import logging
import sys

sys.path.insert(0, "src")   # make kokoro_agent importable without install
sys.path.insert(0, ".")    # make tests/ importable

logging.basicConfig(level=logging.WARNING)  # silence DeepAgents INFO noise

from deepagents import create_deep_agent, GeneralPurposeSubagentProfile, HarnessProfile, register_harness_profile  # noqa: E402
from kokoro_agent.tools import TOOL_OBJECTS  # noqa: E402
from tests._fake_chat_model import scripted_planning_model  # noqa: E402


async def main() -> None:
    fake = scripted_planning_model()

    # Register a harness profile for our fake model to:
    # 1. Disable the general-purpose subagent (removes "task" tool)
    # 2. Exclude filesystem/execute tools from the tool set
    register_harness_profile(
        "deepagents-fake-chat-model",
        HarnessProfile(
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
            excluded_tools=frozenset({"ls", "read_file", "write_file", "edit_file",
                                      "glob", "grep", "execute", "task"}),
        ),
    )

    agent = create_deep_agent(
        model=fake,
        tools=TOOL_OBJECTS,
        system_prompt="probe: plan and search",
    )

    print("=== astream_events(v2) event shapes ===")
    event_count = 0
    async for event in agent.astream_events(
        {"messages": [("user", "plan and search for kokoro")]},
        version="v2",
    ):
        event_count += 1
        evt_type = event["event"]
        name = event.get("name", "")
        data = event.get("data", {})
        data_keys = list(data.keys()) if isinstance(data, dict) else str(data)

        print(f"[{event_count:03d}] event={evt_type!r:40s} name={name!r:30s} data_keys={data_keys}")

        # For tool start/end, print the actual input/output and run_id for pairing
        if evt_type == "on_tool_start":
            run_id = event.get("run_id", "N/A")
            print(f"         tool input: {data.get('input')!r}  run_id={str(run_id)[:16]}")
        elif evt_type == "on_tool_end":
            output = data.get("output")
            run_id = event.get("run_id", "N/A")
            print(f"         tool output: {str(output)[:80]!r}  run_id={str(run_id)[:16]}")
        elif evt_type == "on_chat_model_stream":
            chunk = data.get("chunk")
            if chunk is not None:
                content = getattr(chunk, "content", "")
                tool_chunks = getattr(chunk, "tool_call_chunks", [])
                print(f"         chunk.content={content!r} tool_call_chunks={tool_chunks!r}")
        elif evt_type == "on_chat_model_end":
            output = data.get("output")
            if output is not None:
                msgs = getattr(output, "generations", [])
                print(f"         output.generations count={len(msgs)}")

    print(f"\n=== Total events: {event_count} ===")


if __name__ == "__main__":
    asyncio.run(main())
