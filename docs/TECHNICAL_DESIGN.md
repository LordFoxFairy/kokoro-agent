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

worker 长驻调度由 `supervisor.py` façade、`supervisor_control.py`、`supervisor_execution.py`、
`supervisor_recovery.py` 和显式 `supervisor_context.py` 协作契约组成；PostgreSQL RunRepository 也按
admission、dispatch、events、leases、effects、sandbox capability 拆分。旧入口不保留同义 alias。

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

同一 `run_id` 的请求 body 变化返回冲突；旧 worker 的 generation 不得写入新 owner 的 run。Run ingress scoped
读取同时校验 trusted tenant 与派生 namespace，SQL JOIN 也按 tenant 连接，避免只依赖
hash namespace；chat scope 的 namespace 仍只能由同一 trusted identity 派生。Redis 丢帧时由 PostgreSQL
pending intent/outbox 扫描恢复。控制命令使用 durable command ledger，重复
identity 重放已有 receipt，digest 不同则拒绝。

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

## 6. System 模型路由接线（2026-09-08，已接线、live smoke待验）

本节是 ADR-031 的窄消费者切片，不把上文当前四层目录当作新增代码模板。Root 为本仓唯一 writer；
基线 `70a38138f42f29e8a482fde7890fe0e2d0c27e34`，工作树干净。跨仓任务表是 System 的 IMPLEMENTATION_PLAN G6-Agent。

| 放置项 | 决定 |
|---|---|
| Owner | System 拥有 label/policy/availability→route；Agent 拥有执行、模型实例及进程凭据 |
| 当前事实 | `agent_factory.py` 创建 DeepAgents 时调用 `select_model_label`；没有 System client，默认硬编码 anthropic/claude |
| 目标职责 | 创建实际模型前以 trusted tenant、feature、可选 label 调用 System；失败关闭，不绕过回本地名称 |
| 目录比较 | 采用已有 `clients/system.py`，与现有 owner clients 邻接；拒绝新 `infrastructure/system` 或第二 runtime 层 |
| 粒度 | 一个单一 HTTP 边界模块含窄 Protocol/内部路由结果；`model/factory.py` 负责路由到模型实例映射 |
| 依赖 | worker 入口创建进程级 HTTPX client 并关闭，Factory 依赖窄 ModelResolver；不跨仓 import/SQL，不导出 HTTPX Response |
| 数据/API | 本仓 schema/Run wire 不变；System 契约 pin 见 API_CONTRACT；解析不处于数据库事务内 |
| 删除 | 删除 `select_model_label` 与硬编码模型 fallback；显式 Agent model 若与路由冲突则拒绝，不静默覆盖 |
| 验证 | HTTP client、真实Factory调用、缺配置/404/403/503/坏响应/超时/取消/限额；Ruff/Pyright/pytest/contract/build和live smoke |

System 当前只返回 `litellm` 路由，`gateway_model_name` 映射为 Agent `ModelConfig.name`；
provider endpoint/secret 永不来自解析响应。Agent 声明的 effort/thinking 仍属于执行设置。
CLI 启动必须配置 System URL/服务凭据及 LiteLLM 网关；嵌入部署可显式注入同一窄 resolver，fake 只在 tests。
HTTP 客户端设置总体 deadline、各阶段 timeout、响应上限、不跟随重定向、不自动重试；取消直接传播。
模型解析在创建 sandbox/附属能力前执行，失败不先制造外部资源。每次 build（含恢复构造）重新受信解析并记录
revision/digest/generation 的结构化日志；本切片不承诺持久化 run-level 模型快照，也不复制模型表。
