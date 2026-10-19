# mcp

MCP 连接的 GA 运行时实现，不是 Capability 的资源管理面。

## 目录边界

- `config.py`：部署侧 MCP 连接定义；标准 Streamable HTTP 与本地 stdio process 的 discriminated 配置及环境变量引用解析。
- `package.py`：当前受管包独立严格 manifest、ZIP/逐文件 SHA/portable path 与入口校验，不授予执行权限。
- `managed_session.py`：当前材料到标准 SDK 的单次 HTTP/stdio session，临时目录/执行位及取消/暂停清理；生产 resolver/worker 接线仍待后继。
- `tools.py`：模型看到的稳定 `mcp_list_tools` / `mcp_describe_tool` / `mcp_call` 工具面。
- `servers.py`：标准 adapter connection 装配；仅 HTTP 注入 egress 策略，stdio 由上游 adapter 按需启动和回收。
- `tests/support/local_mcp.py`：仅供测试/本地 fixture 使用的读取实现；生产接线由
  `clients/mcp.py` 的 `McpClient` public contract 注入，不把 Capability 数据库带进 GA。
- `secret_client.py`：Capability public contract 的 secret handle 读取客户端。

Agent/Feature 只通过 Factory 得到 MCP 工具，不直接导入本目录的存储实现。
