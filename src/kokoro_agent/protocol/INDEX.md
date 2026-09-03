# Agent protocol

本目录只拥有 Agent 自己的跨进程协议：

- `control.py`：Run launch/resume/cancel/steer 的严格内部命令；
- `events.py`：Agent 执行证据的严格事件模型；安全 Chat projection 由 `chat/` 单独产生；
- `streams.py`：Agent Redis transport 的 stream/key 命名与上限；
- `__init__.py`：上述本仓协议的明确导出。

协议类型不依赖 Repository、HTTP handler、数据库、Redis client 或其他 owner 的实现。
本目录不是共享契约仓，不保存 Skill/MCP/Storage 数据库文档，不依赖 Root 生成器。
字段变化在本仓 API 文档和 contract tests 中同步验证。
