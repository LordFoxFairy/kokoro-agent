---
architectureIndex: 1
rootId: agent.compat
owners:
  - "@LordFoxFairy"
---

# compatibility commands

Root compatibility gates invoke these child-owned commands as independent processes. They may use
only Agent public production boundaries and emit closed, bounded JSON receipts or inputs; Root never
imports Agent source code.

`agui_candidate_provider.py` accepts one bounded fixture document, constructs official AG-UI Python
events through `kokoro_agent.presentation.build_agui_candidate`, and emits the exact Session-owned
`kokoro-session-agui-compatibility-input.v1` bundle. It is compatibility evidence only: it does not
activate presentation transport or mutate graph, checkpoint, control, handoff, or runtime state.

New commands must keep stable error codes, single-line JSON stdout, no secret/environment reads, and
regular-file no-follow/TOCTOU protection for file input.
