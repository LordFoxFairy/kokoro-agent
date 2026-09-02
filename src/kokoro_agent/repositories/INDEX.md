# repositories — Agent repository ports

- `run_repository.py`：RunRepository port 与 transport-neutral 结果模型；Run claim、lease、control command、terminal、effect journal 的 PostgreSQL 实现位于 `infrastructure/postgres_run_repository.py`。
- `chat_repository.py`：ChatRepository port；用户可见消息与事件的 PostgreSQL 实现位于 `infrastructure/postgres_chat_repository.py`。

数据库 schema 和技术 adapter 不属于 repository port，统一位于 `infrastructure/`。
产物交付只通过 `clients.storage.DeliveryClient`，Artifact owner 不属于 GA。
Skill package 的本地 fixture adapter 留在 `skills/local_reader.py`（历史 fixture），不从 repositories 域重导出。

本目录不定义 Agent、Feature 或自有 Graph/State，也不把 LangChain checkpoint 当作产品聊天历史。
用户可见历史由 `ChatRepository` 管理 `chat_messages` / `chat_events`；它与 RunRepository、
checkpoint 是三条独立数据面。

## Control 的边界

`run_control_commands` 是 Agent 内部唯一的 control command ledger，不是 Chat 业务表：

- 它在 HTTP control 发布到 Redis 前记录 admission/idempotency，保证同一个 `run_id + command_id`
  重试时得到同一 receipt，并拒绝 request digest 漂移。
- worker 收到 Redis control 后，把同一行推进到 `persisted → applied/superseded`，保证 ACK 与 apply
  的崩溃恢复顺序；HTTP receipt 只是这条记录的 transport projection，不落第二张表。
- `chat_events` / `chat_messages`：只保存用户可见的产品事件和消息，不写入 control command ledger。

BFF 不读上述 PostgreSQL 表。BFF 只调用 Agent HTTP control 接口取得 admission receipt，并通过 Agent
的 chat query/event ingress 获取需要展示的产品投影。
