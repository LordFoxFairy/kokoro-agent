# storage — GA 运行事实与适配器（PostgreSQL）

- `ledger.py`：Run claim、lease、control、terminal、effect journal 等 GA 运行事实。
- `memory_store.py`：DeepAgents Store 的进程装配入口。
- `checkpoints.py`：只创建 LangGraph/DeepAgents 官方 PostgreSQL checkpointer，不改变其表结构或 state。
产物交付只通过 `clients.storage.DeliveryClient`，Artifact owner 不属于 GA。
Skill package 的本地 fixture adapter 留在 `skills/local_reader.py`（历史 fixture），不从 storage 域重导出。

本目录不定义 Agent、Feature 或自有 Graph/State，也不把 LangChain checkpoint 当作产品聊天历史。
用户可见历史位于相邻 `chat/` 包的 `chat_messages` / `chat_events` adapter；它与 RunLedger、
checkpoint 是三条独立数据面。
