---
architectureIndex: 1
rootId: agent.skills
owners:
  - "@LordFoxFairy"
---

# skills — 技能一等域（包契约、hub 池读写面、物化对账）

## Responsibilities

技能的存储与供给：SKILL.md 包契约校验、Skills Hub（Mongo 元数据+正文快照 ×
S3/local 内容寻址 zip 包体权威源）、装配期把有附件的包物化进 run 的 backend。

## Non-responsibilities

本域不拥有技能市场、运营审核、Site entitlement、Session grant 选择或浏览器管理界面。

## Public boundary

公开面 = `__init__.py` 的 `__all__`（15 个符号），按定义模块列：

- `hub.py`
  - `SkillHub`：写面 `upsert`（校验清单强制、hash 未变幂等不写、文档级 revision CAS）、
    `mark_deleted`、`set_official_flags`（enabled=上架 / required=恒注入拒关）、
    `set_enabled`（per-namespace 启停偏好，独立表）；读面只按会话快照卡 (scope,name,hash)
    直读：`read_body`（正文双路：当前版 Mongo 快读 / 旧 hash 走 zip 取回）、`load_package`、
    `load_package_if_assets`（纯文档包返 None 不白走包体存储）。
    池查询/管理面权威在 kokoro-hub（TS）；本仓不提供池枚举/跨 scope 解析面。
  - `make_skill_hub(settings)` 异步上下文装配；`SkillHubSettings`；`seed_official`
    （部署目录只是 seed 输入，真源是库+包体）；`validate_package`（名称/保留名/文件数/
    体积/路径穿越/尖括号注入，fail-loud）；`content_hash_of`；`SkillHubError`
    （校验失败/CAS 冲突/包体缺失的统一失败类型，消费侧按它兜底转工具错误）；`OFFICIAL_SCOPE`。
- `materialize.py`：`SkillMaterializerMiddleware`（before_agent 一次对账）、
  `reconcile_skill_assets`（graph state 账本 {name: content_hash} 驱动增量：目录缺失自愈
  全量重写、残留 GC、单包失败不阻断只不落账本）。
- `package.py`：`parse_frontmatter`（YAML 头 fail-loud：name 与目录同名、description 非空）、
  `SkillFrontmatter`、`SkillAssetError`。
- `supply.py`：`SKILLS_ROOT`（"/.skills/"，点前缀=能力供给不进用户文件清单）、
  `MaterializeBackend`（物化 reconcile 所需的 backend 能力面：upload 写包体 + als 探目录）。

包内私有（不在 `__all__`，外部只能深导入）：`hub.py` 的 `PackageStore` /
`make_package_store` / `LocalPackageStore` / `S3PackageStore`，`supply.py` 的
`ExecCapableBackend`（GC 删除按 isinstance 探测降级，仅 materialize.py 内用）。
`PackageStore` / `make_package_store` 当前被 `worker/main.py`、`agents/deps.py`、
`tools/deliver.py` 深导入复用为 deliveries 存储——这是未收口的边界缺口，扩大跨包用法前
必须先把它们提升进 `__init__.py` 并在此登记。

## Callers and dependencies

- 消费面：`agents/`（清单直接渲染会话快照 grants + 挂 SkillMaterializerMiddleware）、
  `tools/skills.py`（正文双路读取）、`worker/main.py`（seed_official/deliveries 存储）。
- 跨仓：池查询/管理面权威在 kokoro-hub（TS，同一 Mongo skills 集合）；session 建会话时
  从 kokoro-hub 取池做快照，run 装配按快照卡 (scope,name,hash) 直读本仓读面。
- 下游：pymongo async、boto3（经 to_thread）、`contract/storage`（SkillCard/SkillDoc）。

## Data ownership and events

本域读取快照卡并拥有内容寻址包体/物化账本；Hub 拥有池管理写面，Session 拥有会话 grant snapshot。

## Runtime and security

- 内容锁：zip 按 content_hash 寻址不可变，官方升级不影响已快照旧 hash 的进行中会话。
- 写入即生效（下一会话可见）；(scope,name) 唯一；并发写靠 revision CAS fail-loud。
- 包体先落、元数据后写：put 失败则 Mongo 不动，不出现悬空 package_ref。
- s3 凭据 env-only（复用 workspace 对，ADR-010）；缺凭据配 s3 即抛错。

## Idempotency, failure, and recovery

content hash、revision CAS 与物化账本保证重复 upsert/重启/目录丢失可恢复；单包物化失败不错误落账。

## Extension rules and forbidden dependencies

- 新校验项加进 `validate_package`（upsert 强制执行点），不散落在调用方。
- 物化布局只在 supply.py 定义；GC 目标名必须过 `_SAFE_SKILL_NAME` 二次确认（防相对 rm 越界）。

## Current gotchas

当前 Agent 仍直读 Hub 共用存储的快照版本；不得把每次 run 改成同步 Hub RPC 热路径。

## Verification

运行 `uv run pytest tests/test_skill_hub.py tests/test_assembly.py -q`、`uv run pyright` 与 `uv run ruff check src tests`。
