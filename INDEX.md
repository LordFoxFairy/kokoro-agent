# kokoro-agent 仓库索引

## 运行入口

- `src/kokoro_agent/worker/main.py`：worker/http 启动装配和环境读取唯一入口。
- `src/kokoro_agent/http/`：Agent-owned HTTP ingress；不执行浏览器 SSE/AG-UI。
- `src/kokoro_agent/worker/supervisor.py`：Redis dispatch、lease fencing、control 和终态收口。
- `src/kokoro_agent/agent_factory.py`：唯一 DeepAgents native runnable 构造入口。

## 代码地图

```text
src/kokoro_agent/
  protocol/       Agent 自有 command/event/stream wire 模型
  domain/         领域实体、值对象、规则（逐步收敛中）
  application/    用例编排与窄 port（逐步收敛中）
  infrastructure/ PostgreSQL、Redis、checkpoint、外部 adapter
  interfaces/     HTTP/RPC/event 传输映射（逐步收敛中）
  agents/         DeepAgents 能力声明
  features/       Feature -> Agent 装配声明
  execution/      run、approval、event projection
  repositories/   持久化 port 和 transport-neutral record
  services/       既有应用服务；新代码迁入 application
  clients/        Capability/Storage public client port
  worker/         Redis worker、recovery、graceful drain
  http/           版本化业务 ingress
  chat/           Agent-owned chat fact model/projection
  sandbox/        workspace/backend adapter
  model/          provider/model adapter
  tools/          工具、middleware、权限 guard
```

## 事实源与验证

- [机器 HTTP contract](contract/openapi/v1/openapi.json)
- [canonical PostgreSQL schema](database/schema.sql)
- [文档索引](docs/INDEX.md)
- [技术设计](docs/TECHNICAL_DESIGN.md)
- [当前实现](docs/CURRENT.md)
