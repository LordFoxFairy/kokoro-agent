# kokoro-agent ADR 索引

| ADR                                                  | 决策                                                                                         |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [ADR-001](ADR-001-owner-and-runtime.md)              | Agent 以 DeepAgents 为唯一 runtime，按 owner 隔离事实                                        |
| [ADR-002](ADR-002-canonical-schema.md)               | V1 使用唯一 canonical schema 与应用层关系完整性                                              |
| [ADR-003](ADR-003-durable-run-boundary.md)           | PostgreSQL durable ledger + Redis replayable notification                                    |
| [ADR-004](ADR-004-agent-execution-proof-and-jwks.md) | Agent owns canonical execution proof signing and public JWKS; IAM owns current authorization |

新增 ADR 必须写明状态、上下文、决策、替代方案、影响和验证证据；已废弃决策保留历史但不能重新成为实现依据。
