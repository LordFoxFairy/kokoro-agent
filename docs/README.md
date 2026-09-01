# kokoro-agent 文档

跨仓 API/AIP 与 owner 方案同步规则见根仓 [51-跨子仓契约同步](../../docs/kokoro-handbook/technical/51-cross-repository-contract-sync.md)。

这个目录只放 `kokoro-agent` 子仓自己的长期文档。

## 放这里

- agent 执行链路、sandbox、skills 供给、MCP client、memory、checkpoint、HITL 的本仓实现细节。
- 本仓调试、测试、验证和运行说明。
- 对根仓方案的 agent 侧摘录。

## 不放这里

- 跨仓权威规则、产品决策、platform/user/session/web 的主权文档。
- 临时调研材料、外部参考来源路径、截图和探索草稿。
- 已被根仓 handbook 接管的历史 spec 全文。

这些内容分别属于根仓 `../docs/`、对应子仓的 `docs/`，或本仓被忽略的 `tmp/`。

## 当前文档

- [最终 Agent 架构](./agent/architecture.md)
- [当前边界与 namespace 规则](./agent/current-boundary.md)
- [API/AIP 契约摘录](./agent/api-contract.md)
- [GA 技术方案](./agent/technical-plan.md)
