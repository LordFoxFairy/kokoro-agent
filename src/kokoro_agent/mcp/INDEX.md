# mcp

MCP 连接的 GA 运行时实现，不是 Capability 的资源管理面。

## 目录边界

- `config.py`：部署侧 MCP 连接定义和环境变量引用解析。
- `tools.py`：模型看到的稳定 `mcp_list_tools` / `mcp_describe_tool` / `mcp_call` 工具面。
- `servers.py`：连接建立、egress 策略和不可用状态处理。
- `local_registry.py`：仅供本地 fixture 使用的内存读取实现；生产接线应由
  `clients/mcp.py` 的 `McpClient` public contract 注入，不把 Capability 数据库带进 GA。
- `secret_client.py`：Capability public contract 的 secret handle 读取客户端。

Agent/Feature 只通过 Factory 得到 MCP 工具，不直接导入本目录的存储实现。
