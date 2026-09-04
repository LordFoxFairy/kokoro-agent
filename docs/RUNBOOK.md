# kokoro-agent 运行手册

## 启动

应用进程从源码启动；本地只复用一个 PostgreSQL 和一个 Redis。先安装依赖并安装当前 schema：

```bash
uv sync --frozen
KOKORO_AGENT_DATABASE_URL=TARGET uv run kokoro-agent-db-apply-schema
KOKORO_AGENT_DATABASE_URL=TARGET KOKORO_REDIS_URL=TARGET uv run kokoro-agent-worker
KOKORO_AGENT_DATABASE_URL=TARGET KOKORO_REDIS_URL=TARGET KOKORO_INTERNAL_SECRET_AGENT=TOKEN uv run kokoro-agent-http
```

每个 owner 使用独立 database/schema 和 Redis logical DB；Agent 默认 Redis DB 为 9。不要在已有依赖时
再次 `docker run`。

## 诊断顺序

1. `GET /healthz`：只确认进程存活。
2. `GET /readyz`：确认 bearer、PostgreSQL schema 和 Redis 可达。
3. 检查 `run_dispatch` pending、lease expiry、outbox oldest、control receipt 和 cleanup backlog。
4. 用同一 `run_id`/`Idempotency-Key` 重放，确认 digest 不变；不要手工重复写终态。
5. 核对 `request_id`、`trace_id`、run evidence 和 worker generation；日志中不得复制 secret/payload。

## 恢复

- Redis 丢帧：保持 PostgreSQL intent，重启/heartbeat republish；不直接伪造成功事件。
- worker 崩溃：等待 lease TTL，由新 generation claim；确认旧 generation 无新增 durable event。
- outbox 堵塞：确认下游可达、按 oldest 顺序重试；保留 receipt 和原始 event identity。
- cleanup 失败：检查 backend teardown 权限和 cleanup intent，按退避策略处理；必要时由 backend owner 手动
  释放资源并记录 cleanup id。
- schema drift：停止服务、备份并在空的受管 database 安装 canonical schema；V1 不运行历史迁移 runner。

## 回滚

回滚到上一不可变镜像 digest，并保留当前 PostgreSQL/Redis durable facts；先验证 contract/schema 版本一致，再
恢复流量。禁止用旧代码写入新 schema 未定义的字段，也禁止删除生产数据来“修复”版本不一致。
