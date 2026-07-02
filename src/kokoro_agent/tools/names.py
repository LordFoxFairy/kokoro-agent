"""领域层：DeepAgents 与事件路由共用的工具名。"""

from __future__ import annotations

TODO_TOOL_NAME = "write_todos"  # deepagents 内置 TODO 工具
SUBAGENT_TOOL_NAME = "task"  # kokoro 路由名：子智能体
EXECUTE_TOOL_NAME = "execute"  # deepagents 内置 shell 工具
ASK_USER_TOOL_NAME = "ask_user"  # Kokoro 默认人机问答工具
