# worker — 进程入口与长驻调度

worker 只负责传输、Run 生命周期和共享服务装配。Skill/MCP/Storage 的 owner 通过 public contract
提供，worker 只注入已解析的 reader/client，不执行外部域的 CRUD 或初始化写入。

## 职责

kokoro-agent 的进程域：env 一次解析 → 共享件装配 → RunSupervisor 长驻消费请求流。
持有 run 生命周期的调度真相（去重认领、TTL 租约心跳、过期重拾、优雅停机、终态清理）。

## 公开 API

- `main.py`：`main()` 标准进程入口；`serve(config, clients)` 是部署装配入口。`AppConfig.from_env`
  单点读 env → 创建 GA 自有 Redis stream + PostgreSQL checkpointer/run_repository/memory/chat → 注入可选 public clients →
  `AgentFactory` → `RunSupervisor.serve`。标准 CLI 的 owner clients 为空能力，不直读外部私库。
  SIGTERM 优雅停机：停消费新请求，`drain` 限时等活跃 run 收尾，超时交 TTL 租约重拾。
- `dependencies.py`：`WorkerClients` 是部署期可选 owner-client 集；`WorkerDependencies` 集中保存 worker
  warm 时创建一次的模型、checkpoint、run_repository、store、sandbox 和窄 clients。两者都不是 caller
  input、Service Locator 或 Feature 配方。`WorkerClients.delivery` 是可选 Storage Artifact
  facade；缺席时只不装配 deliver tool。
- `supervisor.py`：`RunSupervisor`（注入式装配；RunRepository 持有去重/租约/原 request/终态认领
  四类真相）。
  - `serve(bus)`：consumer group 消费 REQUESTS_STREAM；RunRequest 走 CAS claim→durable claim
    后 ACK（R1）；用户消息 durable 写入先于 dispatch claim；启动即跑
    `_republish_outbox`（R4 critical outbox queued 行按 seq 序幂等补发）与
    `_reapply_pending_control`（R2 control command 续办）。per-message 隔离，单条失败收口 run.failed。
  - `dispatch(bus, msg)`：RunRequest→认领起跑 / RunResume→帧对齐续跑 / RunSteer→信箱入账
    （keep-first；注入由 SteeringMiddleware 下一模型轮消费）/ RunCancel→原子认领终态补发 cancelled。
  - `heartbeat_once`：活跃 run 续租；续租失败即 fencing（让渡本地执行，终态权归新属主）；
    重拾他处过期 run；收养暂停 run 的 control 监听；R4 回执对账（`_reconcile_run_receipts`：
    推进 consumed/GC 已确认行、rejected NACK 按 contract_incompatible 终局、receipt_state_lost 告警、
    published 无回执超宽限期 `outbox_republish_ms` 复用固定身份重发）；retention 清扫终态 run。
  - `drain(timeout_s)`：优雅停机（暂停 run 不算活跃，不阻塞退出）。
- `messages.py`：`parse_inbound(raw) → InboundMessage | None`（contract 校验，坏帧警告丢弃）。

## 关键协作者

- 下游依赖：`execution/`（invoke_once/RunEmitter/approvals 全套）、`repositories/run_repository`（RunRepository）、
  `streams/`（StreamProtocol）、`features/` + `agent_factory.py`（Feature 装配）、`skills/`、`sandbox/`、
  `mcp/config`、`contract`。
- 上游：kokoro-bff 内部 Chat 模块经 Redis Streams 投递 RunRequest（REQUESTS_STREAM）与
  per-run control 流（resume/cancel/steer）。
- `metrics`（OBS-1）：claim 胜负/inbox 相位/outbox 相位/租约 gauge 埋点（fail-open，只观测）；
  `main` 在 `KOKORO_AGENT_METRICS_PORT` 配置时起 prometheus_client 端点（缺省关）。

## 运行时约束

- resume 路径的多重护栏：is_terminal 闸（stale resume 不续跑）→ has_pending_interrupt 幂等闸
  → adopt 交接租约 → 再查 is_terminal（build 长窗内他处 cancel）→ spawn 前 entry gate。
- per-run control 流是 consumer group：多 worker 收养天然去重；终态清理删流先于 cancel 监听
  任务（监听可能是当前任务），删流后 NOGROUP 属干净收束。
- Semaphore（默认 8）仅限活跃 invoke：暂停态不持有额度，resume 重新竞争。
- 终态统一漏斗 `_teardown_control`：沙箱回收 → 事件流 TTL → 删 control 流 → 收监听。
- emitter 缓存 per-run；miss 时 `RunEmitter.attach` 同时读取 Redis live history 与 durable
  `chat_events.source_index`，Redis 被清空后仍不会回卷 GA event identity。

## 扩展规则

- 新入站消息 kind：contract 扩 InboundMessage → dispatch 加分支；不得绕 parse_inbound。
- 调度依赖一律构造注入（agent_builder/trace_factory/sandbox_teardown 模式），supervisor 不读 env。

## 当前陷阱

- steer 持久化失败只记日志不判死 run（插话可由用户重发）。
- task done-callback 按任务身份弹出：resume 覆盖同 run_id 新句柄时旧回调不误删。
