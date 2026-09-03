# Agent 与 AG-UI 边界

## 结论

`kokoro-agent` 不直接依赖浏览器 UI SDK，也不把 AG-UI 事件重复落入 Agent 数据库。Agent 的职责是运行 DeepAgents、保存自己的执行事实，并通过严格的 Agent Chat query/replay HTTP 契约提供安全产品投影。

AG-UI 的公共事件名和运行时 schema 由 `@ag-ui/core` 提供，产品映射由 BFF 本仓 API 文档定义；Agent 只维护自己的内部 Chat/Run contract，不复制一份 AG-UI Python 类型。这样 `@ag-ui/core` 只在 Web/BFF 的浏览器边界
出现，Agent 的生产依赖保持最小、可选启动，并且不会把 Web transport 反向耦合进执行层。

`kokoro-bff` 是浏览器入口和业务层，负责：

1. 校验 Web 身份、tenant 和请求幂等语义；
2. 调用 Agent 的版本化 query/replay/control API；
3. 把 Agent 内部事件一次性转换成 AG-UI canonical events；
4. 通过 SSE 将 AG-UI 事件交给 `kokoro` Web；
5. 在 `metadata.kokoro` 中保留 `event_id`、`seq`、`session_id`、`run_id` 和源时间，用于断线恢复。

这样 Agent 不需要同时维护一套 Python AG-UI encoder 和一套浏览器业务投影，避免 Web、BFF、Agent 各自实现一份 session/chat 协议。AG-UI 的 `RUN_*`、`TEXT_MESSAGE_*`、`TOOL_CALL_*` 和 `CUSTOM` 只在 BFF/Web 的公开交互边界出现；Agent 内部的 `assistant.delta`、`activity` 等事件仍是 Agent-owned execution facts。

## 为什么不把 AG-UI SDK 放进 Agent

AG-UI 是 Agent 与用户界面的交互协议，BFF 承接了鉴权、业务投影和浏览器连接生命周期；Agent 本身是可选执行服务，可能没有启动。将 AG-UI 传输绑定到 Agent 会让可选 Agent 反向决定 Web API，并把 BFF 的业务职责泄漏到执行仓库。当前结构保持：

```text
Web -> BFF Chat/AG-UI -> Agent query/replay/control -> Agent execution facts
```

本地没有 Agent 时，BFF mock 使用同一个 AG-UI 输出契约，因此 Web 不需要切换协议或增加第二套 reducer。
