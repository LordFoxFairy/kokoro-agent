# kokoro-agent 技术设计

## 1. Owner 与依赖

```text
BFF / trusted service
        |
        v
interfaces/http -> application use case -> domain rules
        |                  |
        +-------------> infrastructure ports/adapters
worker bootstrap ---------+
```

`protocol` 是跨进程 wire 模型，不依赖数据库和传输实现。`domain` 不依赖 HTTP、Redis、PostgreSQL、
DeepAgents 或 provider SDK。`application` 决定用例、授权入口、事务和幂等；`infrastructure` 实现 SQL、
锁、外部 client、checkpoint 和 stream；`interfaces` 只做解析、映射和错误转换。

当前历史目录 `execution`、`services`、`repositories`、`http` 仍承载已验证实现；新代码按上述层落地，
每次切片完成真实迁移后删除旧入口，禁止同义 alias 并行。

## 2. Run 生命周期

```text
HTTP/Redis request
  -> validate trusted identity and feature key
  -> persist dispatch intent + immutable request fence
  -> publish Redis notification
  -> worker reads canonical request from PostgreSQL
  -> atomic claim creates lease generation
  -> build Feature/Agent and invoke native DeepAgents
  -> persist fenced evidence/chat/outbox
  -> claim one terminal transition
  -> publish/replay durable event and cleanup sandbox
```

同一 `run_id` 的请求 body 变化返回冲突；旧 worker 的 generation 不得写入新 owner 的 run。Redis 丢帧时由
PostgreSQL pending intent/outbox 扫描恢复。控制命令使用 durable command ledger，重复 identity 重放已有
receipt，digest 不同则拒绝。

## 3. Agent 装配

`Agent` 是静态能力声明，`Feature` 是产品入口和 peer handoff 声明，`AgentFactory` 直接调用
`deepagents.create_deep_agent`；多个 peer 只使用官方 `langgraph-swarm`。请求不携带 graph、tool、Skill、
MCP 或 namespace 配方。native state、checkpoint 和 loop 归上游框架所有。

## 4. 事务与一致性

- PostgreSQL 是 Agent durable facts 的 owner；Redis 只作通知和短期传输。
- 关系写入先校验 trusted namespace/状态，再以固定顺序加锁，在一个事务内写事实、receipt/outbox 和
  sequence。
- 没有数据库外键；跨 owner 关系由应用校验、事务、锁、状态检查和 reconciliation 维护。
- 事件 projection 使用 `(run_id, durable_seq)`/业务 idempotent id，重复投递不产生重复事实。

## 5. 故障恢复

worker 启动依次 republish pending dispatch、outbox、未应用 control 和 cleanup intent；心跳续租失败时旧
任务停止副作用。SIGTERM 停止新消费，drain 超时后交给 lease TTL 恢复。异常单 run 收口为 `run.failed`，
不杀死长驻调度循环。
