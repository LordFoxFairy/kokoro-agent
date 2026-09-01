# tools

GA 提供给 DeepAgents 的固定工具、运行期工具集合与工具调用 middleware。

## 公开入口

- `registry.py`：固定工具名称注册与解析。
- `toolset.py`：合流 Agent 固定工具、worker toolbox、MCP 与 deliver 工具。
- `deliver.py`：通过 DeepAgents native backend 读取工作区字节，只调用 Storage
  `DeliveryClient.publish()`；不计算对象 key，不直连 S3/MinIO。
- `guards.py`：主 Agent/native subagent 共用的终态、预算、审批、steering 与副作用 journal 链。
- `middleware.py`：具体 LangChain middleware 实现。
- 其余文件：单个工具或窄策略实现。

## 约束

本目录不选择 Feature、不创建 Agent、不读取环境变量，也不实现 Skill 工具。Skill 元数据和读取由
DeepAgents 原生 SkillsMiddleware / `read_file` 负责。`agent_factory.py` 是唯一调用方和唯一
DeepAgents 构造点；工具授权默认 fail-closed。所有 GA 工具经 `Toolset.from_tools()` 合流，同名
工具（包括 Feature 注入的 handoff 工具）在绑定模型前直接拒绝。MCP client 查询失败只降级对应
能力：基础 Agent 仍可构造，已声明但当前无法解析的名称保留为 unavailable 结果。
