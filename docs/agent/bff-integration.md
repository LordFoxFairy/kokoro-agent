# kokoro-bff ↔ kokoro-agent 集成边界

状态：当前集成约束，2026-09-01。

本文是 `kokoro-bff` 与 `kokoro-agent` 的边界说明，不是新的 wire schema，也不替代根仓
`contract/` 的 API/AIP 定义。所有跨仓字段、版本和兼容规则必须以已发布的版本化契约为准。

## 1. 三仓职责

```text
浏览器
  -> kokoro Web /api/session/*
  -> kokoro-bff/modules/chat /v1/sessions/*
  -> Agent business adapter
  -> kokoro-agent HTTP ingress
  -> kokoro-agent Redis worker
  -> PostgreSQL durable facts + Redis transport
```

- `kokoro-bff` 是 Web-facing 业务层：负责 Chat 会话/消息/标题/分享投影、鉴权、幂等、错误归一、SSE 连接和业务编排。
- `kokoro-agent` 是执行层：负责 Agent loop、Run/control/HITL、恢复、执行安全事件和持久化执行事实。
- `kokoro-bff` 不直接访问 Agent 的 Redis、PostgreSQL、checkpoint、RunLedger、`chat_messages` 或 `chat_events`。
- Agent 由独立的 HTTP ingress 与 Redis worker 进程组成；HTTP ingress 先写 durable dispatch admission，再投递 worker。阶段 1 仍用 BFF 内置确定性 mock adapter 完成 Web→BFF→Agent 业务边界联调，真实 adapter 在 BFF 仓库显式接入。
- `session_id` 仍是 Chat 的稳定业务标识，但不意味着存在独立的 `kokoro-session` 仓库或服务。

## 2. 阶段 1 适配策略

阶段 1 的 `kokoro-bff` 使用本仓内置的确定性 Agent business adapter。默认配置为：

```text
KOKORO_BFF_MODE=mock
KOKORO_AGENT_ADAPTER=mock
```

mock adapter 只返回版本化的受理、控制和终态 fixture：

1. 不启动 Agent worker；
2. 不读取 Agent Redis；
3. 不访问 Agent PostgreSQL、checkpoint、RunLedger 或内部 Python 模块；
4. 不复制 Redis envelope 或 Agent 内部 DTO；
5. 只验证 BFF 的业务编排、鉴权、幂等、错误映射、SSE 和 Web 兼容路由。

这样可以先把 Web、BFF、Agent 的 API 契约闭环跑通，后续只替换 adapter，不改变 BFF 的外部
Chat v1 路由和业务语义。

## 3. 未来真实 Agent 适配

真实适配必须调用 Agent 本仓的独立版本化 HTTP business port，不能把 Redis worker 的内部
envelope 直接暴露给 BFF。

至少满足：

- 定义独立契约版本，包含请求、响应、错误、幂等、超时和兼容/弃用规则；
- 由 ingress/transport owner 完成身份校验以及业务请求到 Redis worker 的映射；
- BFF 只通过 `AgentBusinessAdapter` 调用，不依赖 Redis stream/key、consumer group、checkpoint 或 Python 类型；
- 旧版本在弃用窗口内保持可验证行为，BFF 显式选择版本，不通过探测或静默降级；
- 真实适配上线前必须通过 BFF contract fixture 和 Agent consumer/worker 的兼容性门禁。

建议环境选择面：

```text
KOKORO_AGENT_ADAPTER=mock
KOKORO_AGENT_HTTP_BASE_URL=<future-agent-business-port>
KOKORO_AGENT_HTTP_CONTRACT_VERSION=<future-http-version>
```

未配置真实 endpoint 时必须返回明确的 `503 upstream_not_configured`，
不能连接任意 Redis，也不能静默回退到旧 Session 服务。

## 4. 存储边界：PostgreSQL + Redis

阶段 1 只保留两个基础设施：

| 组件 | Owner | 用途 |
|---|---|---|
| PostgreSQL | Agent/各业务服务各自的 adapter | Durable truth：执行事实、Chat 事实索引及各业务服务自己的领域数据 |
| Redis | transport/worker owner | stream、队列、lease、heartbeat、wakeup、短缓存和限流 |

- Web 不连接 PostgreSQL 或 Redis。
- BFF 不连接 Agent Redis 或 Agent PostgreSQL；BFF 自己的持久化接入也必须通过其业务 adapter。
- Redis 丢失时从 PostgreSQL 重建；PostgreSQL 不可用时不能由 Redis 假装成功。
- 不新增 MySQL、MongoDB 或旧的独立 Session 存储依赖。

## 5. Chat / SSE 边界

`kokoro-bff/modules/chat` 是 Web-facing Chat owner：

- `/v1/sessions`、消息提交、标题、删除、分享和公开 snapshot；
- 鉴权、namespace/project scope、请求幂等和错误 envelope；
- `Last-Event-ID` replay、SSE keep-alive 和浏览器连接生命周期；
- 将 Agent 执行结果投影成对外 Chat/ProductEvent。

`kokoro-agent` 只拥有执行侧 `chat_messages`、`chat_events` 等安全事实和 Run 状态；BFF 通过
版本化 business adapter 或未来的查询端口消费，不直接读表。Agent 不向浏览器发布事件，也不
创建第二套 Chat API、SSE stream 或独立事件序列。

## 6. 跨仓允许的连接面

| 连接面 | 用途 | 约束 |
|---|---|---|
| BFF Chat v1 HTTP | Web → BFF | 根仓契约生成、BFF 自己的鉴权/幂等/错误 envelope |
| Agent business port | BFF → Agent 业务层 | 独立版本、adapter、兼容性测试；阶段 1 为 mock |
| Redis worker contract | transport owner → Agent | 只消费已定义 internal envelope；BFF 不直连 |
| 环境变量/secret | 选择 adapter、endpoint、数据库和 Redis | 不把地址、凭据、key 写入源码或业务 payload |
| 独立 CI | 各仓自证实现和兼容性 | 不跨仓 import 私有模块，不共享数据库来代替契约 |

不得通过共享源码路径、未发布内部包、手工复制 DTO 或共享运行时数据库建立隐式依赖。
每个子仓只闭环自己的代码、测试、Docker 和发布门禁；跨仓只交换版本化 contract fixture、
兼容性结果和发布元数据。

## 7. 阶段 1 验收

- [ ] Web 所有 Chat 请求只经 BFF `/v1/sessions/*`，没有独立 Session/Gateway 路径。
- [ ] BFF Chat mock contract 覆盖 list/detail/message/SSE/control/title/delete/share。
- [ ] BFF 默认 mock 不访问 Agent Redis 或 Agent PostgreSQL。
- [ ] Agent 通过 PostgreSQL + Redis 完成执行事实、Run/control/HITL、outbox/recovery worker 门禁。
- [ ] 三仓各自通过 lint、typecheck、unit/contract、build；Web→BFF→Agent mock smoke 通过。
