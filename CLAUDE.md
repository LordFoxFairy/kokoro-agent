# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Follow `/Users/yuri/WebstormProjects/Kokoro/CLAUDE.md` first; it remains the global governance source of truth. This repo-local file is only a thin overlay for `kokoro-agent`.

## Repo purpose
- `kokoro-agent` is the Python worker and raw execution-event producer.
- It consumes `run.request` items, runs the model, and emits raw execution events for downstream normalization.
- Browser-facing or session-facing contract shaping does not belong here.

## Critical boundaries
- `src/kokoro_agent/worker.py` owns lifecycle and loop concerns: request consumption, per-`run_id` idempotence, and publishing each run's event stream.
- `src/kokoro_agent/run_agent.py` owns execution and event sequencing: `run.started` -> `text.delta`* -> `text.completed` -> `run.completed | run.failed`.
- Use explicit Pydantic boundary models (`RunRequest`, `AgentEvent`) instead of letting request/event `dict` shapes drift.
- Browser-facing contracts, normalized cursors/timestamps/owners, and parser behavior belong to `kokoro-session`, not `kokoro-agent`.

## Where code belongs
- `src/kokoro_agent/events.py`: boundary schemas and allowed raw event kinds.
- `src/kokoro_agent/infrastructure/model.py`: model selection/bootstrap from env.
- `src/kokoro_agent/infrastructure/stream_port.py`: transport abstractions plus memory/redis implementations.
- `src/kokoro_agent/worker.py`: worker loop and event publication.
- `src/kokoro_agent/run_agent.py`: chunk extraction, monotonic `seq`, and terminal-event behavior.
- `tests/`: intent-focused boundary tests for the touched module.

## Verification checklist
- Run `pytest`.
- Run `ruff check`.
- Run `pyright`.
- Run the most relevant targeted test file or node for the behavior you changed, for example `pytest tests/test_run_agent.py -q`.
- If raw event kinds, payload shapes, or ordering change, also check the matching `kokoro-session` parser/tests before calling the change complete.

## Local pitfalls
- Do not move session or browser concerns into this repo; it emits raw execution events only.
- Preserve monotonic `seq`, event order, and fail-loud `run.failed` behavior when changing execution flow.
- Keep `MemoryStreamPort` and `RedisStreamPort` behavior aligned; transport differences should not leak into the contract.
- When a boundary shape changes, update the Pydantic models first and then align the downstream `kokoro-session` parser/tests.
