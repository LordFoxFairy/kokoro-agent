# clients — 外部 public contract 客户端

本目录只定义 GA 需要的窄协议和连接适配边界，不复制 Capability、Storage、Studio、Billing 或
Model owner 的领域模型。

## 当前协议

- `skills.py`：`PlatformSkillClient`按已冻结exact ref调用Run-bound Resolve/Approved，每次读取重新授权并校验完整包，无名称grant与bytes授权缓存。
- `package_transport.py`：共享固定ObjectStore origin的有界原始GET，禁止cookie、redirect和自动重试；具体owner策略由包装客户端裁决。
- `mcp_package_transport.py`：MCP短时精确大小/受控headers下载，独立于Skill授权，不接收内部凭据头。
- `skill_package_transport.py`：固定ObjectStore origin的GET transport，空owner headers、无cookie/bearer、期限/32MiB/hash验证，不复用Artifact PUT凭据。
- `mcp.py`：明确Connection引用与当前工具port；不接受名称/URL/command或外部grant。
- `mcp_runtime.py`：LeasedRun装配的当前Connection/Connector/逐页Server读取、发现/调用材料分离及逐次原参数授权；无长期cache、自动重试或旧YAML fallback。
- `storage.py`：`DeliveryClient.publish()` 是 GA 发布产物的唯一 Storage Artifact
  facade。`storage_transport.py` 固定正式 Storage v3 并只开放七个 delivery RPC；signed PUT
  返回原始 object version。`storage_delivery.py` 先以 tool journal v2 keep-first 持久
  upload/version/Complete identity，再 Complete、scan 与 finalize。GA 不组装 bucket key，
  不持有 Storage 的 upload/asset/artifact 生命周期。

后续新增 client 时，按 owner contract 拆文件；Agent/Feature 只能依赖协议，具体 HTTP、Connect
或 SDK 实现由部署通过 `WorkerClients` 注入。标准 CLI 不直读 owner 私库；本地内存实现仅是测试
fixture。

- `platform_credentials.py`：worker-only exact owner-file snapshot、tenant/generation 高水位与安全读取。
- `platform_tokens.py`：IAM Basic/form token exchange、generation cache/singleflight/rotation/cancel 生命周期。
- `platform_transport.py`：九个 exact generated Connect RPC、不可变 run sender、逐 call binding/proof/Bearer、deadline/error boundary。
  Skill typed reader与两项Runtime传输已接入；MCP当前resolver/worker已装配，正常组合与approval整链仍待验、Platform v4激活与安装产品链仍待验。
