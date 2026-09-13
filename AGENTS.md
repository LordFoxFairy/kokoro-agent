# kokoro-agent 子仓工程规范

本文件是 `kokoro-agent` 的本地执行补充。先读取根仓库
[`../AGENTS.md`](../AGENTS.md)；根手册定义全局 owner、依赖方向、UTC、SQL、契约、测试和提交规则，本文件只
补充 Agent 的实际边界。规则已经明确时直接执行，不重复询问。

## Owner

本仓只拥有 Agent 执行事实：Run、dispatch admission、lease/checkpoint、control receipt、tool journal、
安全执行事件、Chat history/replay、HITL 和 evidence。BFF 拥有 Conversation/Message/AG-UI 公共投影；
Capability 拥有 Skill/MCP control plane；Storage 拥有对象和 Artifact；IAM 拥有身份与授权。跨仓只调用
owner contract，不读别人的数据库或导入别人的 DTO。

## 依赖方向

```text
interfaces -> application -> domain
infrastructure -> domain/application ports
bootstrap -> application + infrastructure
protocol -> 只依赖标准库/Pydantic，不依赖业务层
worker -> 启动装配与 Redis ingress，不成为 Domain 的依赖
```

目前历史包目录仍在逐步收敛；新增代码必须按上述边界落点。禁止在 `src/` 增加 InMemory/Fake/Fixture、
万能 `common/utils`、第二套 runtime、隐式环境读取或跨 owner SQL。测试替身只放 `tests/support`、
`test/fixtures` 或 `test/doubles`。

## 当前事实源

- HTTP：`contract/openapi/v1/openapi.json`
- Redis command/event：`src/kokoro_agent/protocol/`
- PostgreSQL：`database/schema.sql`，只有 fresh install，没有历史迁移链
- 配置：`worker/main.py`、`interfaces/http/main.py` 与 `application/schema.py` 分别只解析自身进程/操作边界；HTTP root 不加载 worker/private 配置
- 真实依赖：共享 PostgreSQL 与 Redis；不重复创建已有容器

## 工作协议

1. 开始先检查本仓 `git status`、当前分支、`docs/CURRENT.md`、`docs/CODEBASE_MAP.md`（若由根仓提供）和
   相关 contract/schema。
2. 先写/更新 contract、领域不变量和测试，再按 application、infrastructure、interfaces 的顺序实现。
3. 每个逻辑切片单独 commit；移动、行为、格式化和生成物不要混成不可审查的大提交。
4. 完成前运行：

```bash
uv run ruff check src tests
uv run pyright
uv run pytest -q
uv run kokoro-agent-contract-check
KOKORO_AGENT_DATABASE_URL=TARGET KOKORO_REDIS_URL=TARGET uv run pytest -q -o addopts='' -m 'integration or acceptance'
uv build --wheel --sdist
```

报告必须列出真实命令、结果、失败项和未完成风险；Agent 自报不替代主工作区验证。
