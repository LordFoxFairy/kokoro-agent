# infrastructure — Agent 外部技术适配器

- `postgres.py`：PostgreSQL 连接、schema qualification 与连接生命周期。
- `checkpoints.py`：DeepAgents/LangGraph 官方 PostgreSQL checkpointer 装配。
- `memory_store.py`：DeepAgents Store 的 PostgreSQL 装配。

这里不定义 Agent 运行用例，也不承载业务 owner 数据；运行事实的 repository 接口和 PostgreSQL 实现位于相邻的 `repositories/`。
