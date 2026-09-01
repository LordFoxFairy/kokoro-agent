# agents

可复用的完整 DeepAgents 能力声明。

## 公开入口

- `definition.py`：`Agent` 声明模型。
- `general.py`：`GENERAL_AGENT`。
- `music.py`：`MUSIC_AGENT`。
- `subagent_catalog.py`：worker 启动时加载的可用 native subagent 目录。
- `subagents.py`：只将当前 Agent 明确声明且部署可用的目录项转换为 DeepAgents 原生
  `SubAgent` 参数；不是 peer Agent 或 Swarm。
- `__init__.py`：只导出上述声明；不负责构造、运行或 Redis。

## 约束

Agent 文件只描述 prompt、固定工具、默认 Skill/MCP、是否支持产物交付与 native
subagent 需求。`delivery=True` 只是能力声明；Storage public client 缺席时不挂载空壳
deliver tool，也不影响 Agent 基础循环。Session、Run、
checkpoint、worker service 和外部 client 由 `agent_factory.py`/worker 装配；native subagent 仅是
DeepAgents 的后台隔离能力，不提升为 Feature peer。不要在此增加角色
目录或第二套 Agent loop。部署启用目录项不等于给所有 Agent 自动挂载；`Agent.subagents` 是唯一
选择面，未知名称在构造时 fail-loud。
