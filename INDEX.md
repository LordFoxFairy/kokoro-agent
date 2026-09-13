# kokoro-agent 仓库索引

## 运行入口

- `src/kokoro_agent/worker/main.py`：worker 启动装配入口；只读取 worker 环境。
- `src/kokoro_agent/interfaces/http/main.py`：HTTP-only 启动装配入口；只读取 HTTP 业务配置与 public JWKS descriptor。
- `src/kokoro_agent/interfaces/http/`：Agent-owned HTTP ingress 与 immutable public-ring projection；不持有 private key，不执行浏览器 SSE/AG-UI。
- `src/kokoro_agent/application/schema.py`：canonical schema operator use case；CLI 与 worker 共同导入。
- `src/kokoro_agent/worker/supervisor.py`：Redis dispatch、lease fencing、control 和终态收口。
- `src/kokoro_agent/agent_factory.py`：唯一 DeepAgents native runnable 构造入口。

## 代码地图

```text
src/kokoro_agent/
  protocol/       Agent 自有 command/event/stream wire 模型
  domain/         领域实体、值对象、规则与按 context 放置的 repository port
  application/    用例编排、DTO 与 schema operator boundary
  infrastructure/ PostgreSQL、Redis、checkpoint、外部 adapter
  interfaces/     HTTP/RPC/event 传输映射
  agents/         DeepAgents 能力声明
  features/       Feature -> Agent 装配声明
  execution/      run、approval、event projection
  clients/        Capability/Storage public client port
  worker/         Redis worker、recovery、graceful drain
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

本地 profile 复用共享 PostgreSQL `127.0.0.1:55433` 中的独立 database `kokoro_worker_agent`，以及共享
Redis `127.0.0.1:56380` 的 logical DB `9`；运行入口不负责创建重复的基础设施容器。CI 使用 workflow
显式配置的 service 地址，不依赖本地默认值。
