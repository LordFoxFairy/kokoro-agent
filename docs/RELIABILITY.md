# kokoro-agent 可靠性设计

## 关键保证

- launch：先持久化 dispatch intent，再发布 Redis notification；丢通知可由启动扫描恢复。
- claim：PostgreSQL CAS + lease generation；旧 owner 不能续写或发终态。
- control：durable command receipt + request digest；重复请求安全重放。
- event：outbox/receipt manifest 记录 durable sequence；发布失败可重试，终态只允许一次认领。
- sandbox：cleanup intent 带 lease、attempt 和 next_attempt_at；失败不会阻塞其他 Run。
- shutdown：停止新消费，有限时间 drain；未完成 Run 由 TTL lease 重新认领。

## 超时和重试

所有外部 client 显式设置 connect/read/overall timeout，并传播取消。只对幂等或带稳定 identity 的瞬时
失败使用指数退避+jitter；不对未知副作用盲目重试。Redis 是通知层，PostgreSQL 是 durable source。

## 降级

Capability Skill、MCP、Storage 是可选旁路；其不可用时记录明确观测并按能力粒度关闭，不创建伪成功 receipt，
基础 Agent loop 仍保持可诊断状态。PostgreSQL 或 Redis readiness 失败时不接受业务流量。

## 观测

日志字段至少包括 `service`、`operation`、`request_id`、`run_id`、结果和耗时；指标关注 admission、claim、
lease loss、outbox age、control latency、terminal success、replay gap、cleanup backlog。目标阈值见 `SLO.md`。

## System 路由消费故障

一个进程一个HTTPX client；连接/读写/pool及总体deadline受KOKORO_SYSTEM_TIMEOUT_S控制（默认5秒，上限60秒），
响应最大64KiB，不跟随重定向，不自动重试，任务取消直接传播。错误不回显上游原文或服务凭据。
System路由失败在分配sandbox前终止当前Run；恢复构造重新解析当前策略，revision/digest/generation只记日志，
不宣称已有durable route snapshot或跨重启固定模型。模型服务不参与独立HTTP ingress的admission事务。
