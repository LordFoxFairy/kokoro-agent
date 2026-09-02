# infrastructure — Agent 外部技术适配器

- `postgres.py`：PostgreSQL 连接、schema qualification 与连接生命周期。
- `schema.py`：Agent execution 的 canonical PostgreSQL schema。
- `postgres_run_repository.py`：RunRepository 的 PostgreSQL 实现。
- `postgres_chat_repository.py`：ChatRepository 的 PostgreSQL 实现。
- `checkpoints.py`：DeepAgents/LangGraph 官方 PostgreSQL checkpointer 装配。
- `memory_store.py`：DeepAgents Store 的 PostgreSQL 装配。

这里不定义 Agent 运行用例，也不承载业务 owner 数据；它只实现 `repositories/` 的 port 和外部技术 adapter。
