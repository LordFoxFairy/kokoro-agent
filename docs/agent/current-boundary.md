# kokoro-agent 当前边界

状态：2026-07-07
范围：只约束 `kokoro-agent`

## Owns

- LangChain/LangGraph/DeepAgents 执行。
- tools、skills、MCP client、subagents、HITL、sandbox。
- checkpoint、memory、run state、raw execution events。
- official AG-UI adapter 到 Root R1 `PresentationSubmission` 的直接构造、durable source
  commit / `DeliveryRecord` 与 Agent→Session pull/ack/quarantine application port。
- run 级别的 capability 装配和执行生命周期。

## Does Not Own

- 浏览器会话事件契约的业务投影。
- public presentation identity、Session admission/binding、cursor/snapshot/SSE。
- session messages、snapshot、SSE replay。
- 用户、团队、site、workspace 的身份主数据。
- credit/payment/model pricing。
- capability hub 的写入控制面。

## Namespace

GA 侧只认 `RunScope.namespace`。

允许：

```text
checkpoint scope = namespace + thread_id
memory scope = namespace
skill/capability resolve = namespace
sandbox/workspace archive prefix = namespace
```

禁止：

```text
user:<ownerId>
ownerId/userId/workspaceId in GA contract
agent 查询用户主数据来判断隔离
```

session/platform 负责把上游身份解析成 namespace。agent 只消费最终 namespace。

## 文档来源

跨仓稳定规则以根仓 `docs/kokoro-handbook/` 为准。子仓文档只补本仓实现细节。
