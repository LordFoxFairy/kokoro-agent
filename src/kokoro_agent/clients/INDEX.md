# clients — 外部 public contract 客户端

本目录只定义 GA 需要的窄协议和连接适配边界，不复制 Capability、Storage、Studio、Billing 或
Model owner 的领域模型。

## 当前协议

- `skills.py`：`SkillClient` 负责把 Agent/Feature 的 Skill 名称解析为本次装配的读取 grant；
  `SkillReader` 只负责按 grant 读取包体。`NoSkillsClient` 是未接 Capability 部署的空能力实现。
  Grant 是 client 返回的临时结果，不进入 Agent/Feature/Session。查询、可见性、CRUD 与
  logical path 由 Capability public contract 负责。
- `mcp.py`：`McpClient`，接收名称、`ExecutionIdentity` 与 GA 派生 namespace，只暴露本次运行
  需要的 MCP 配置读取面；注册、启停、凭据和路径由 Capability public contract 负责。MCP grant
  细节留在 client 内部。适配器用 `McpClientError` 表达 Capability 读取不可用；GA 保留部署定义并
  将仅存在于 Capability 的名称标成 unavailable，不中断 Agent 基础循环。
- `storage.py`：`DeliveryClient.publish()` 是 GA 发布产物的唯一 Storage Artifact
  facade；其中的 `PackageStore` 仅供 `skills/local_reader.py` fixture 内部使用，不从
  `clients` 包导出。GA 不组装 bucket key，
  不持有 upload/asset/artifact 生命周期。

后续新增 client 时，按 owner contract 拆文件；Agent/Feature 只能依赖协议，具体 HTTP、Connect
或 SDK 实现由部署通过 `WorkerClients` 注入。标准 CLI 不直读 owner 私库；本地内存实现仅是测试
fixture。
