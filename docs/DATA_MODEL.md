# kokoro-agent 数据模型

## Canonical authority

唯一 DDL 是 [`../database/schema.sql`](../database/schema.sql)，仅支持空数据库 fresh install；不保留
migration chain、外键或 `REFERENCES`。所有表名/列名小写 snake_case，时间事实使用 `TIMESTAMPTZ(3)`。

## 表按事实分组

| 事实组 | 表 |
|---|---|
| Run admission/lifecycle | `kokoro_agent_run`、`kokoro_agent_run_dispatch`、`kokoro_agent_run_dlq` |
| Event durability | `kokoro_agent_run_outbox`、`kokoro_agent_run_receipt`、`kokoro_agent_run_receipt_manifest` |
| Control/usage | `kokoro_agent_run_control_command`、`kokoro_agent_run_steer`、`kokoro_agent_run_usage_segment` |
| Sandbox/tool | `kokoro_agent_sandbox_cleanup_intent`、`kokoro_agent_tool_result`、`kokoro_agent_tool_journal` |
| Chat projection | `kokoro_agent_chat_session`、`kokoro_agent_chat_event`、`kokoro_agent_chat_message`、`kokoro_agent_chat_sequence` |
| Long-term memory | `kokoro_agent_memory` |
| Native checkpoint | `checkpoints`、`checkpoint_blobs`、`checkpoint_writes` |

Run/dispatch 是 tenant-scoped；namespace 是 Agent 从 trusted identity 派生的隔离键。Run ingress scoped
lookup 必须同时 predicate `tenant_id` 与派生 namespace，且 tenant-owned JOIN 必须把 tenant 条件写在
`ON` 与 `WHERE` 中。没有独立 `tenant_id` 列的 chat scope 只能接受由同一 trusted identity 派生的 namespace，
不得把 caller-provided namespace 当作权限。子表通过受保护的 application/repository 查询和固定事务路径关联，
不依赖数据库外键。跨 owner 的 tenant/resource ref 只保存 opaque reference，不做数据库 JOIN。

## 字段与约束原则

- `id`/业务 identity 只在真正幂等或唯一不变量时建主键/唯一索引。
- 状态字段使用有限集合 `CHECK`；不使用互相矛盾的 boolean 代替状态机。
- append-only event/receipt/usage 不机械添加 `updated_at`。
- JSON/JSONB 只承载结构化 payload；可查询的 tenant、状态、排序字段使用普通列。
- 每个索引必须对应 tenant filter、due scan、稳定排序、唯一性或并发路径。
- 软删除、version、audit actor 只在有真实语义时增加。

## 时间与金额

数据库默认值为 `CURRENT_TIMESTAMP(3)`；应用内部可在 adapter 使用 epoch milliseconds 参与兼容的
checkpoint API，但不得把 Unix 秒写入数据库。金额如未来进入 Agent 只用最小货币单位整数和显式 currency。

## System 路由消费切片

2026-09-08 G6-Agent 不改变 canonical schema、Run输入或持久化写边界。模型目录、版本、provider、租户路由表
只属于System；Agent不复制、不JOIN。每次模型构造的解析revision/digest/generation记结构化日志；这不是持久化模型快照。
当前 schema/contract 验证命令保持不变；目标失败路径与依赖 pin 见 TECHNICAL_DESIGN §6、API_CONTRACT。
