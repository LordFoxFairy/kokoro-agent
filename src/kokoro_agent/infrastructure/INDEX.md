# infrastructure — Agent 外部技术适配器

- `postgres.py`：PostgreSQL 连接、schema qualification 与连接生命周期。
- `schema.py`：Agent execution 的 canonical PostgreSQL schema。
- `postgres_run_repository.py`：RunRepository 的稳定公开 façade；只负责装配和转发，不收纳整套 SQL。
- `postgres_run_context.py`：连接配置、租约 fencing、Row 映射所需的共享事务原语；不作为业务 port 暴露。
- `postgres_run_admission.py`：dispatch/control admission 与 delivery capability。
- `postgres_run_dispatch.py`：dispatch claim、pending 查询和 DLQ capability。
- `postgres_run_events.py`：durable event、outbox、receipt reconcile capability。
- `postgres_run_leases.py`：lease、lifecycle、usage 和 terminal capability。
- `postgres_run_effects.py`：execution effect、steer、tool result/journal capability。
- `postgres_run_sandbox.py`：sandbox binding 与 durable cleanup capability。
- `postgres_chat_repository.py`：ChatRepository 的 PostgreSQL 实现。
- `checkpoints.py`：DeepAgents/LangGraph 官方 PostgreSQL checkpointer 装配。
- `memory_store.py`：DeepAgents Store 的 PostgreSQL 装配。

这里不定义 Agent 运行用例，也不承载业务 owner 数据；它实现 `domain/<context>/` 的 repository port、
application port 的具体技术 adapter，以及 canonical schema。`application/schema.py` 是 schema apply 的稳定
operator boundary，负责配置映射和调用这里的 DDL 实现。
