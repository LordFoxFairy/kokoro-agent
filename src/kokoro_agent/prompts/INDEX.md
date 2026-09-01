# prompts

Agent 自带的静态 prompt 资产。

## 公开入口

- `__init__.py`：读取受管 Markdown prompt。

## 约束

prompt 由 Agent 定义选择；caller、Session 和 RunRequest 不传 prompt preset 或任意内联 prompt。
Skill 清单不在这里渲染，由 DeepAgents 原生 SkillsMiddleware 注入。本目录不访问网络、数据库、
checkpoint 或 worker transport。
