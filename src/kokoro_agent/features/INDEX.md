# features

对外产品能力的静态组装声明。

## 公开入口

- `definition.py`：`Feature`，选择一个或多个 Agent、`entry_agent` 与可选 peer handoff。
- `chat.py`：`CHAT_FEATURE`。
- `music.py`：`MUSIC_FEATURE`。
- `music_chat.py`：`MUSIC_CHAT_FEATURE`，只声明通用 Agent 与 Music Agent 的 peer handoff。
- `catalog.py` / `__init__.py`：`FeatureCatalog`、默认 `FEATURE_CATALOG` 和 `get_feature`；
  目录只做受信 Feature key 查找，不编译或执行图。
- `../inspect.py`：只读遍历同一个 catalog，输出不含 prompt、identity 或 runtime state 的诊断描述。

## 约束

Feature 是唯一业务组装层；请求只引用受信 Feature key，不提交 Agent、工具、Skill、MCP 或
graph 配方。单 Agent 直接走 DeepAgents；需要同一会话 peer 接手时才由 Factory 使用官方
Swarm。后台隔离工作属于 Agent 的 DeepAgents native subagent。
多 Agent Feature 的每个成员必须能从 `entry_agent` 沿声明的 handoff 边到达，避免装配出
永远接收不到控制权的 Swarm 成员。
