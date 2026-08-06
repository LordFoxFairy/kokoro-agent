# kokoro-agent

Kokoro 的**执行层**：DeepAgents + LangChain worker。以 consumer-group 消费 run 请求流，
跑 agent 循环，产出契约事件（`run.* / message.* / thinking.* / tool.* / todo.* / plan.* / subagent.*`，
共 21 kind），写入 per-run 事件流。**不面向浏览器**，只供 `kokoro-session` 消费。

> 全局架构与起栈见 [根 README](../README.md)；技术法律见根仓 `docs/kokoro-handbook/`；
> 协议单源见根仓 `contract/`（本仓 `src/kokoro_agent/contract/` 是生成镜像，勿手改）。
> 本仓正式文档入口见 [docs/README.md](docs/README.md)；历史本地草稿仍按 `.gitignore`
> 留在未跟踪区，不作为权威依据。

## 目录（按执行链路组织）

```
src/kokoro_agent/
├── contract/         ⚙ 生成物（DO NOT EDIT）：事件/控制/流名的唯一协议词汇
├── config.py         AppConfig：环境变量唯一解析点，仅 worker/main.py 消费
├── agents/           【成品层】封装好的对外 agent 定义（general 成品：人格+身份）
├── orchestration/    【编排层】assemble=每请求主配方（RunRequest+RuntimeConfig→InvokableAgent，
│                     只收领域设置不收 AppConfig）；context=模型可见面唯一拼装点
│                     （人格+条件工具指引+skills）
├── worker/           【调度域】main=env→deps→serve；supervisor=长驻调度（请求流消费、
│                     per-run control 流、租约心跳/过期重拾、暂停 run 收养、SIGTERM drain）
├── execution/        【运行域】build_agent（DeepAgents 装配收窄为 InvokableAgent 端口）、
│                     run_agent（invoke/终态认领/recursion 熔断）、events（RunEmitter：index 单点
│                     递增、wire 截断、review 抑制）、approvals（HITL/审核帧构造与 resume 对齐）
├── presentation/     【Agent presentation authority】RunEmitter owner fact → official AG-UI model →
│                     Mongo append-only candidate log；独立 mTLS Connect provider，不接浏览器
├── run/state.py      RunScope（run 身份）+ KokoroAgentState（DeepAgentState 扩展）：身份乘
│                     State 轴随 input 进图、落 checkpoint、resume 不重供；图节点不得改写
├── model/            chat model 工厂（openai/anthropic/DeepSeek 包装抽 reasoning）+ LocalFake
├── tools/            底层工具与治理：ask_user_question、propose_plan、
│                     web_fetch（SSRF 防御）、web_search（协议+provider 注册表同文件）、
│                     registry（名字治理）、permissions（interrupt_on 构造）、
│                     middleware（工具授权 fail-closed / 委派执法 / 结果审核）
├── hub/              Platform Hub mTLS RPC：精确 run assembly、Skill 流式校验与内容寻址缓存
├── skills/           run-scoped immutable Skill 包读取、SKILL.md 校验与附件物化
├── subagents/        目录（内建=空，原则：只收带真实工具的真能力；预设走 namespace wire）
├── mcp/              Hub 下发的精确 MCP 定义接入：白名单过滤 + mcp__{server}__{tool} 命名
├── sandbox/          执行 backend 工厂（state / local_shell；e2b 待落地 fail-loud）
├── streams/          StreamProtocol（cursor 不透明）+ redis（XADD maxlen、XREADGROUP/XACK、
│                     XAUTOCLAIM 死信收养）
├── storage/          RunStateStore（TTL 租约/暂停哨兵/终态原子认领/审核结果 keep-first，
│                     mongo）+ checkpointer 工厂；旧 memory store 只保留为非生产实验件
└── observability.py  Langfuse trace config（三 env 齐备才开，缺任一静默关闭）
```

## 运行

```bash
uv sync
# 本地假模型（凭据无关，离线可跑）：
KOKORO_REDIS_URL=redis://127.0.0.1:6379/10 \
  KOKORO_MONGO_URL='mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true' KOKORO_MONGO_DB=kokoro \
  KOKORO_HUB_RPC_URL=https://hub.internal:9443 \
  KOKORO_HUB_RPC_SERVER_NAME=hub.internal \
  KOKORO_HUB_RPC_CA_FILE=/run/secrets/hub-ca.pem \
  KOKORO_HUB_RPC_CERT_FILE=/run/secrets/agent.pem \
  KOKORO_HUB_RPC_KEY_FILE=/run/secrets/agent-key.pem \
  KOKORO_LOCAL_FAKE_MODEL=1 uv run kokoro-agent-worker
```

接真实模型：去掉 `KOKORO_LOCAL_FAKE_MODEL`，配置 Platform Model Gateway 的 mTLS client
材料（见 `.env.example`；GA 不持 provider 凭据；该文件也列出 reasoning 抽取、web 工具、
Langfuse 和熔断开关）。**模型档位、sandbox backend、
skills/MCP/子代理预设、权限**全部由 kokoro-session 的 namespace profile 决定并经 wire 下发
——agent 只消费 RuntimeConfig，不自造政策。
GA 侧只认 opaque `namespace`；不要在 agent 契约里新增 `userId` / `ownerId` /
`workspaceId` 作为隔离辅助字段，也不要拼 `user:<id>` 这类业务前缀。

## 生产发布事实

[`deployables.yaml`](deployables.yaml) 是 Agent child-owned 的唯一进程库存，
[`deployables.schema.json`](deployables.schema.json) 以 JSON Schema 2020-12 关闭未知字段。它只枚举
三个真实 console entrypoint：`kokoro-agent-worker`、`kokoro-agent-evidence` 和
`kokoro-agent-presentation`；不存在兼容 CLI、历史 shim 或隐式第四进程。Root release orchestrator
读取该库存作为 policy input，Agent 运行时本身不读取也不修改 activation 字段。

当前三个进程均为 `activationAuthorized: false`、`runtimeTraffic: false`、
`launchReadiness: blocked`、零副本。Presentation 与当前 Evidence V2 Root boundary 仍为
`contract-only`，而 Evidence 入口仍实际提供 V1，另有明确版本错配；Worker/Evidence 还受
execution-owner lease epoch 与 terminal/outbox/evidence 原子性约束；全部进程都缺依赖感知
readiness 实现。进程存活不等于依赖就绪，故镜像显式 `HEALTHCHECK NONE`，发布系统不得用通用
TCP/PID 探针把这些阻断伪装成 ready。

生产 Dockerfile 使用 pinned Python base digest 和 build-only pinned uv，将 non-editable package
复制进无 uv/pip/cache 的 runtime stage，以 `10001:10001` 运行并支持只读根文件系统；仅 `/tmp`
允许由编排器挂 tmpfs。库存同时要求禁止提权、drop `ALL` capabilities、RuntimeDefault seccomp 与
禁用 service-account token。默认入口是 `kokoro-agent-worker`，另两个进程只允许通过库存里的精确
command 覆盖。库存解除阻断并获得跨仓发布证据前，构建成功不构成 activation 授权。

## 能力面（全部经 单测 → 跨栈 e2e → 真模型 验证）

- **HITL 双拦截**：工具前审批（approve/edit/reject）+ 工具后结果审核（approve/respond/reject，
  keep-first 缓存防双跑）；`ask_user_question` 恒为 respond 暂停点；`propose_plan` 恒为主 agent
  独占 tool-call 帧的 approve/reject 暂停点，产出 immutable durable `plan.proposed` owner。
- **Product Memory M0 hard-cut**：旧 Mongo `save_memory`/`search_memory` 已从所有生产工具箱、
  Runtime catalog 与 worker storage composition 移除；历史 `KOKORO_AGENT_MEMORY` 非空即拒绝启动，
  stale wire catalog 在任何 Hub 调用或沙箱分配前 fail-closed。Product Memory 的权威属于 Platform，
  M2 只能通过 Root 版本化的窄 `MemoryPort` 接回 GA。旧模块仅供明确的非生产实验直接导入，且不迁移旧数据。
- **web 双件**：`web_fetch` 恒挂载（SSRF：DNS 解析后拒非公网/逐跳复检/15s/1MB/24k）；
  `web_search` 配置即挂载（tavily/searxng/zhipu 注册表，无 provider 不挂空壳）。
- **skills / MCP**：每次 run 在工具装配前经 Platform Hub mTLS RPC 解析一次精确 assembly；
  `agent_catalog_ref`、grant revision/hash、assembly digest、Skill artifact hash 全链锁定，失配或撤销
  fail-closed，不回退本地配置或共享数据库。
- **子代理**：wire 预设（tools 按名解析实例、model 工厂化，未知名 fail-loud）；
  委派三档 `subagent_create=deny|ask|allow`（deny 只放行声明集）。
- **thinking**：openai 兼容端点带 `reasoning_content` 时 `KOKORO_OPENAI_REASONING=1`
  切 DeepSeek 包装抽取，thinking.delta 全链上 wire。
- **韧性**：TTL 租约重拾、PEL 死信收养、暂停 run control 监听收养（worker 崩溃后 HITL
  不卡死）、`KOKORO_RECURSION_LIMIT` 失控熔断（默认 100）。

## 门禁与验证

```bash
uv run ruff check . && uv run pyright && uv run pytest   # 本仓三件套
uv run pytest tests/repository/test_deployment_inventory.py -q
docker build --target runtime --tag kokoro-agent:verification .
python3 ../scripts/e2e-v21-gate.py        # 跨栈确定性门禁（LocalFake，30 项）
python3 ../scripts/chaos-verify.py        # 崩溃混沌：worker 收养 + session 恢复（11 项）
python3 ../scripts/trace-verify.py        # Langfuse HITL trace 连续性（7 项）
python3 ../scripts/real-model-verify.py   # 真模型五场景（thinking/subagent/search/skills/execute）
```

## 关键不变量

- wire 词汇 = `contract/` 生成物，一套 kind 从 agent 到像素同名同拼写；per-run 单调 `index`
  由 `execution/events.py` 的 RunEmitter 单点递增（event_id 幂等链根基）；
  契约 optional 字段缺席=省略（exclude_none），null 永不上 wire。
- 请求流 XREADGROUP 消费、parse 后即 XACK；崩溃恢复权在 RunStateStore TTL 租约，
  HITL 暂停置哨兵永不被重拾重跑，其 control 监听由存活 worker 心跳收养。
- claim-before-emit：cancel/自然完成/异常三路共用同一原子认领键，恰好一个终态事件。
- HITL 帧构造唯一在 `execution/approvals.py`：resume 按 tool_id fail-loud 对齐，
  `tool.awaiting_approval` 携带 `pending_tool_ids`（同帧凑齐才提交的契约依据）。
- `plan.proposed.owner_ref` 逐字等于主 agent 的真实 tool_call_id；同 owner 的 durable semantic marker
  保留到 run retention，outbox receipt GC 后重拾也不得换 event identity 或覆盖 proposal。
- 第三方类型豁免锁死于 `tests/test_boundary_pragmas.py` allowlist（现仅 2 处），
  行内 `type: ignore` 全仓为零（同测执法）。
- 异常 → `run.failed` 终态 fail-loud，worker 存活（单消息隔离，不崩调度循环）。

> 依赖变更必须用 `uv lock --no-config --default-index https://pypi.org/simple`，避免本机 uv
> 配置把镜像 URL 噪音写进官方源 lock；验证用 `uv lock --check --no-config`。
