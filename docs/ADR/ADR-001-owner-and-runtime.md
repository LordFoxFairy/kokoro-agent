# ADR-001：Owner 边界与唯一 Agent runtime

- 状态：Accepted
- 日期：2026-09-04

## 上下文

Agent 执行需要 checkpoint、HITL、subagent 和 peer handoff；同时不能把 BFF、Capability、Storage 的业务
模型复制进执行仓。

## 决策

DeepAgents/LangGraph 原生对象是唯一执行 runtime。`Agent` 是静态能力声明，`Feature` 是可信装配声明，
`AgentFactory` 是唯一构造入口。Agent 只拥有 Run/evidence/chat execution facts；跨仓通过版本化 contract。

## 影响

不能从请求动态提交 graph/tool/Skill/MCP 配方；需要新能力时修改 owner 声明、contract 和测试。旧目录迁移必须
删除重复入口，不能保留兼容 alias。
