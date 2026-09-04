# kokoro-agent 文档索引

本目录只描述 Agent owner 的当前实现和运行边界。字段级协议事实以
[`../contract/openapi/v1/openapi.json`](../contract/openapi/v1/openapi.json)、
[`../src/kokoro_agent/protocol/`](../src/kokoro_agent/protocol/) 和
[`../database/schema.sql`](../database/schema.sql) 为准。

## 推荐阅读顺序

1. [`../README.md`](../README.md)：五分钟启动和入口。
2. [`CURRENT.md`](CURRENT.md)：当前实现、证据和已知缺口。
3. [`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md)：分层、状态机、事务和恢复。
4. [`API_CONTRACT.md`](API_CONTRACT.md)：HTTP、Redis command/event 和 BFF 接入边界。
5. [`DATA_MODEL.md`](DATA_MODEL.md)：表 owner、不变量、索引和 retention。
6. [`SECURITY.md`](SECURITY.md)：身份、tenant、secret、egress 和日志脱敏。
7. [`RELIABILITY.md`](RELIABILITY.md)：幂等、lease、outbox、重放和降级。
8. [`SLO.md`](SLO.md)：目标 SLI/SLO 和告警阈值（目标不等于实测）。
9. [`RUNBOOK.md`](RUNBOOK.md)：本地启动、诊断、恢复和回滚。
10. [`ACCEPTANCE.md`](ACCEPTANCE.md)：可执行验收矩阵。
11. [`ADR/README.md`](ADR/README.md)：仍有效的架构决策。

## 专题文档

- [`agent/architecture.md`](agent/architecture.md)：DeepAgents/Feature/Swarm 边界。
- [`agent/current-boundary.md`](agent/current-boundary.md)：身份与 namespace。
- [`agent/agui-boundary.md`](agent/agui-boundary.md)：BFF AG-UI 投影边界。
- [`agent/bff-integration.md`](agent/bff-integration.md)：BFF 消费摘要。
