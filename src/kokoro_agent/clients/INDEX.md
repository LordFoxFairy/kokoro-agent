# clients — 外部 public contract 客户端

本目录只定义 GA 需要的窄协议和连接适配边界，不复制 Capability、Storage、Studio、Billing 或
Model owner 的领域模型。

## 当前协议

- `skills.py`：`PlatformSkillClient`按已冻结exact ref调用Run-bound Resolve/Approved，每次读取重新授权并校验完整包，无名称grant与bytes授权缓存。
- `skill_package_transport.py`：固定ObjectStore origin的GET transport，空owner headers、无cookie/bearer、期限/32MiB/hash验证，不复用Artifact PUT凭据。
- `mcp.py`：`McpClient`，接收名称、`ExecutionIdentity` 与 GA 派生 namespace，只暴露本次运行
  需要的 MCP 配置读取面；注册、启停、凭据和路径由 Capability public contract 负责。MCP grant
  细节留在 client 内部。适配器用 `McpClientError` 表达 Capability 读取不可用；已声明 MCP 直接失败，不回退部署定义；无声明时不调用 client。
- `storage.py`：`DeliveryClient.publish()` 是 GA 发布产物的唯一 Storage Artifact
  facade。GA 不组装 bucket key，不持有 upload/asset/artifact 生命周期。

后续新增 client 时，按 owner contract 拆文件；Agent/Feature 只能依赖协议，具体 HTTP、Connect
或 SDK 实现由部署通过 `WorkerClients` 注入。标准 CLI 不直读 owner 私库；本地内存实现仅是测试
fixture。

- `platform_credentials.py`：worker-only exact owner-file snapshot、tenant/generation 高水位与安全读取。
- `platform_tokens.py`：IAM Basic/form token exchange、generation cache/singleflight/rotation/cancel 生命周期。
- `platform_transport.py`：六个首批 generated Connect RPC、不可变 run sender、逐 call binding/proof/Bearer、deadline/error boundary。
  Skill typed reader已接入；MCP后续切片、Platform v4激活与安装产品链仍待验。
