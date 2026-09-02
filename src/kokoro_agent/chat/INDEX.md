# chat

GA-owned Chat execution facts. This package is independent of DeepAgents/LangGraph native
state and never reads checkpoint tables. The Web-facing Chat API is owned by
`kokoro-bff/modules/chat`.

- `models.py`: strict user-visible `chat_messages` / `chat_events` record shapes and GA ID derivation.
- `projection.py`: allowlisted projection from execution payloads; raw thinking, tool arguments,
  tool results, subagent content and internal errors are dropped.
- Repository adapter: `../infrastructure/postgres_chat_repository.py`; the `chat/` package does not own SQL.
- `(run_id, source_index)` is idempotent, while
  `(namespace, session_id, seq)` is the isolated ordered replay cursor.
- Application service: `../services/chat_service.py` provides identity-scoped history/replay. It derives `RuntimeNamespace` from trusted
  `ExecutionIdentity`; caller input and query output never contain namespace.

Every chat row includes GA's derived namespace; session ID alone is never an authorization or
isolation key. GA does not write this internal safe projection into BFF Chat's browser-live stream: that stream
has a different generated envelope and sequence owner. BFF Chat queries/replays these facts through
the Root Chat contract once its GA query client is connected; native state is never chat history.
