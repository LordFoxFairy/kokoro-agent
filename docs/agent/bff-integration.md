# kokoro-bff ↔ kokoro-agent 集成边界

状态：当前集成约束，2026-09-01。

本文是 `kokoro-bff` 与 `kokoro-agent` 的边界说明，不是新的 wire schema，也不替代根仓
`contract/` 的 API/AIP 定义。所有跨仓字段、版本和兼容规则必须以已发布的版本化契约为准。

## 1. 当前 Agent 形态

`kokoro-agent` 当前是 Redis worker，不是 HTTP 服务：

```text
Root LaunchRunRequest
  -> Redis internal envelope adapter
  -> kokoro-agent worker
  -> Feature / AgentFactory / DeepAgents
  -> GA chat facts and run receipts
  -> kokoro-session Chat query / replay / AG-UI / SSE
```

- 当前没有 Agent HTTP ingress，也没有可供 BFF 直接调用的 Agent HTTP endpoint。
- Redis envelope 是 worker 当前的内部传输适配层；其中 `input` 对 Root 请求的顶层
  `message_id/content` 的映射，不构成第二套业务契约。
- 浏览器、Chat API、AG-UI 和 SSE 都不属于 Agent worker 的入口面。
- Agent 的执行、Run 生命周期以及 GA 自己的 `chat_messages`/`chat_events` 事实仍按
  `current-boundary.md` 和 Agent API 摘录执行；这些内部事实不等于浏览器 SSE stream。

## 2. 第一阶段：只接 mock Agent adapter

第一阶段的 `kokoro-bff` 只能依赖 mock Agent adapter。BFF 通过一个窄的 adapter 接口调用
本地确定性 fixture，例如提交一次 launch、发送 control，以及读取 adapter 返回的受理/终态
结果；mock 的行为由 BFF 测试 fixture 定义。

第一阶段的硬约束：

1. `KOKORO_AGENT_ADAPTER=mock` 是唯一启用的 Agent 适配模式。
2. mock adapter 不启动 Agent worker，不读取 Agent Redis，不访问 Agent 数据库、checkpoint、
   RunLedger 或内部模块。
3. BFF 只测试自身的路由编排、请求映射、错误处理和对 Session 的交接，不在 BFF 重写
   DeepAgents loop、AgentFactory、Redis consumer 或 Chat/SSE replay。
4. 未配置 HTTP ingress 时，不得以“临时 endpoint”“调试路由”或环境变量 fallback 连接真实
   Agent。

mock adapter 的返回值应被视为契约 fixture，而不是对当前 Agent Redis envelope 的复制。将来
   切换真实实现时，只替换 adapter，不改变 BFF 的业务层对内部 Agent 实现的依赖关系。

## 3. 未来 HTTP ingress 必须独立版本化

如果未来为 Agent 增加 HTTP ingress，它必须作为独立发布、独立演进的 HTTP 契约和适配器版本
加入；不能把当前 Redis worker 直接“包一层 HTTP”，也不能把 Redis internal envelope 自动当作
HTTP API。

至少需要满足以下条件：

- 为 HTTP 定义明确的契约名和版本（例如 `v1`），包含请求、响应、错误、幂等、超时和
  兼容/弃用规则。
- HTTP ingress 自己完成版本校验、身份边界和 Redis/worker 映射；BFF 不依赖 Agent 的
  Redis key、stream name、consumer group、checkpoint 或 Python 内部类型。
- `kokoro-bff` 通过独立的 HTTP Agent adapter 消费该版本；adapter 的契约版本必须显式配置，
  不以“探测服务行为”推断版本，也不静默降级到另一种 wire shape。
- HTTP 契约、实现和发布节奏独立于当前 Redis worker。新版本必须先通过兼容性测试，再由
  BFF 选择性切换；旧版本在约定的弃用窗口内保持可验证的行为。
- HTTP ingress 的增加不改变 `kokoro-session` 对 Chat/SSE 的 owner 边界，也不把 Agent
  变成浏览器事件服务。

## 4. BFF 不得直接访问 Agent Redis

`kokoro-bff` 永远不直接连接 `kokoro-agent` 使用的 Redis。具体禁止：

- 在 BFF 中创建面向 Agent Redis 的 client，读取或写入 Agent stream/key；
- 在 BFF 中配置 Agent Redis URL、密码、consumer group 或内部队列名称；
- 通过 `XREAD`、`XADD`、ack、claim、lease、recovery 等 worker 机制模拟 Agent ingress；
- 直接读取 Agent 的 RunLedger、checkpoint、`chat_messages`、`chat_events` 或其他存储；
- 因为 mock adapter 缺席而回退到任意 Redis 连接。

Redis 可以作为跨仓传输的一种契约载体，但其 producer/consumer、stream、ack 和 envelope
必须由明确的 transport owner 管理并以版本化契约发布。BFF 只调用自己持有的 adapter 或
版本化 HTTP 边界；“能连上 Redis”不等于获得 Agent 集成权限。

## 5. Chat / SSE 事实面仍由 kokoro-session 负责

对外 Chat/SSE 事实面仍由 `kokoro-session` 负责：

- Chat API 的浏览器可见查询和会话语义；
- 历史查询、replay 游标、生成的浏览器事件 envelope 和 `seq`；
- AG-UI/SSE live transport 及其连接生命周期；
- 对 Agent GA 事件事实到公开 ProductEvent 的投影。

`kokoro-agent` 可以按既有 GA 边界写入自己的安全聊天事实和 replay 记录，但 BFF 必须把它们
当作 Agent/Session 契约背后的事实，不创建平行的 Chat history、事件 outbox、SSE stream 或
第二个事件序列。BFF 不直接向浏览器发 Agent 事件，也不绕过 `kokoro-session` 查询或投影
Chat/SSE 事实。

## 6. 跨仓库允许的连接面

跨仓库只通过以下四类可审计连接面：

| 连接面 | 用途 | 约束 |
|---|---|---|
| 版本化 HTTP 契约 | 未来 BFF ↔ Agent HTTP ingress | 独立版本、adapter 和兼容性测试；当前阶段不启用 |
| 版本化 Redis 契约 | Root/transport owner ↔ 当前 Agent worker | 只消费已定义 envelope；BFF 不直连 Agent Redis |
| 环境变量/secret 注入 | 选择 adapter、endpoint 和契约版本 | 不把地址、凭据或内部 key 写入源码或业务 payload |
| 独立 CI | 各仓验证自身实现和跨仓 fixture 兼容性 | 不用共享工作区导入对方私有模块来代替契约测试 |

建议的环境选择面如下：

```text
KOKORO_AGENT_ADAPTER=mock
KOKORO_AGENT_HTTP_BASE_URL=<future-http-endpoint>
KOKORO_AGENT_HTTP_CONTRACT_VERSION=<future-http-version>
```

第一阶段只使用 `KOKORO_AGENT_ADAPTER=mock`；HTTP 地址和 HTTP 版本未配置、未启用。任何
真实 endpoint、secret handle 或部署差异都由环境注入，不能硬编码到 BFF 或 Agent 源码。

各仓独立运行自己的 lint、type、unit/contract、构建和发布门禁；跨仓验证只交换版本化
contract fixture、兼容性结果和发布元数据。不得通过共享数据库、共享源码路径、未发布内部
Python/TypeScript import 或手工复制 DTO 建立隐式依赖。

## 7. 验收检查表

- [ ] Agent 仍被记录为 Redis worker，且没有 HTTP ingress 的当前假设。
- [ ] BFF 第一阶段 adapter 固定为 mock，且没有 Agent Redis fallback。
- [ ] 未来 HTTP ingress 有独立的契约版本、发布和兼容性门禁。
- [ ] BFF 没有 Agent Redis URL、key、consumer group 或 worker recovery 依赖。
- [ ] Chat/SSE 查询、replay、ProductEvent 投影和浏览器 live transport 仍经过
      `kokoro-session`。
- [ ] 跨仓连接只使用版本化 HTTP/Redis 契约、环境变量/secret 和独立 CI。
