"""基础设施层 leaf：tools 与 stream_events 共享的工具名身份（零依赖，单一真理源）。"""

from __future__ import annotations

TODO_TOOL_NAME = "write_todos"  # deepagents 内置 TODO 工具
SUBAGENT_TOOL_NAME = "task"  # kokoro 路由名：子智能体
RUNTIME_SUBAGENT_TOOL_NAME = "agent"  # kokoro 路由名：运行时自定义子智能体
