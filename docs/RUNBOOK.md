# kokoro-agent 运行手册

## 启动

应用进程从源码启动；本地只复用一个 PostgreSQL 和一个 Redis。先安装依赖并安装当前 schema：

```bash
uv sync --frozen
export KOKORO_AGENT_DATABASE_URL=postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent
export KOKORO_AGENT_DATABASE_SCHEMA=kokoro_agent
export KOKORO_REDIS_URL=redis://127.0.0.1:56380/9

uv run kokoro-agent-db-apply-schema
KOKORO_SYSTEM_BASE_URL=http://127.0.0.1:4240 \
KOKORO_INTERNAL_SECRET_AGENT=TOKEN \
KOKORO_LITELLM_ENABLED=1 KOKORO_LITELLM_BASE_URL=http://127.0.0.1:4000/v1 \
KOKORO_LITELLM_API_KEY=TOKEN uv run kokoro-agent-worker
KOKORO_INTERNAL_SECRET_AGENT=TOKEN uv run kokoro-agent-http
```

本地先探测 `127.0.0.1:55433` 与 `127.0.0.1:56380`，复用已运行的共享实例。Agent 只使用独立 database
`kokoro_worker_agent`、应用 schema `kokoro_agent` 和 Redis logical DB `9`；不要在已有依赖时再次
`docker run`。CI 使用 workflow 显式注入的 service 地址，不使用这组本地默认值。

## 诊断顺序

1. `GET /healthz`：只确认进程存活。
2. `GET /readyz`：确认 bearer、public ring、PostgreSQL schema 和 Redis 可达。
3. 检查 `run_dispatch` pending、lease expiry、outbox oldest、control receipt 和 cleanup backlog。
4. 用同一 `run_id`/`Idempotency-Key` 重放，确认 digest 不变；不要手工重复写终态。
5. 核对 `request_id`、`trace_id`、run evidence 和 worker generation；日志中不得复制 secret/payload。

HTTP entrypoint 显式处理 SIGINT/SIGTERM：signal handler 立即把 server 标为 draining 并设置停止事件；独立主线程停止新 accept。每个实际 HTTP request 通过 locked gate 线性化，pre-existing TCP connection 不预留 admission；service bearer preflight 优先于 drain gate，未带或错误 bearer 仍返回 401，合法 bearer 的新业务/readiness request 返回 `503 agent_draining`，且 gate 位于 body/identity/dependency/dispatch 之前。`/healthz` 仍可用于进程存活检查。active handler 在固定 2 秒 deadline 内 drain，能够在期限内完成的已声明响应保持完整，然后执行 `server_close` 与 bounded join。超过 deadline 时有界退出；Python 线程不可强制取消，也不冒称超时 handler 已完成。部署侧仍应先摘流再发终止信号。

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

## System 模型解析诊断

解析调用 /v1/system/model-catalog/resolve，专用服务身份kokoro-agent；确认System已为可信tenant/feature配置可见默认policy。
ROUTE_NOT_FOUND/POLICY_DENIED为配置/授权失败，MODEL_UNAVAILABLE为健康路由不可用；客户端不自行重试或直连provider。
检查模型解析日志的revision_id/digest/generation/request_id，不粘贴secret或owner原始错误。System客户端随worker context关闭。

### Execution-proof key/JWKS operations (A2b)

Mount the worker PKCS#8 file under a trusted secret directory, owned by the worker euid and mode `0400` or `0600`; configure the worker issuer/kid/thumbprint env values only in the future production-composition slice. The A2c standalone supplier exists, but the worker private loader 仍未装配. Mount the HTTP public ring on a root/euid-owned non-group/world-writable parent chain with an allowed read-only file mode, then set only the three HTTP descriptor env variables. Never place private bytes in env or YAML and never log paths/descriptors.

A missing or invalid public ring intentionally leaves `/healthz` at 200 while `/readyz` and JWKS return 503. Correct the mount/descriptor and restart to build a fresh immutable snapshot. Rotation follows the five-stage old→old+new→new fleet gate in ADR-004; `kid` is a header identifier while the thumbprint is the cryptographic key binding.
