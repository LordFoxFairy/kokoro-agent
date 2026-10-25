# mcp

Agent受管工具执行，不拥有Platform控制面数据。

- `tools.py`：恒定list/describe/call三工具，Connection ID声明、journal调用上下文和HITL桥；不持metadata/credential缓存。
- `schema.py`：owner发现schema的有限JS Number/UTF-16排序摘要，不改变proof数值域。
- `package.py`：独立mcp.json、ZIP/逐文件SHA/portable路径/执行位与入口验证，不授予执行权限。
- `managed_session.py`：当前材料到一次标准SDK HTTP/stdio/SSE会话；`sse_transport.py`仅SSE的有界endpoint/流/ACK、identity响应、上下文handler日志及本会话失败latch，保取消/暂停清理。
- `egress.py`：strict默认DNS pin/禁止redirect/不读环境代理；off仅为显式部署选择，测试loopback独立设置。

`clients/mcp.py`是窄调用port和明确Connection值，`clients/mcp_runtime.py`经当前owner API逐次解析和授权，worker/platform只负责Run-bound装配。旧名称/YAML registry、任意command配置和shared-secret secret-resolver均已删除，无fallback。正式服务配对/整链及页面验收另记CURRENT。
