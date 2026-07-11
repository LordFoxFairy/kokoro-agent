# skills — 技能一等域（包契约、hub 池读写面、物化对账）

## 职责

技能的存储与供给：SKILL.md 包契约校验、Skills Hub（Mongo 元数据+正文快照 ×
S3/local 内容寻址 zip 包体权威源）、装配期把有附件的包物化进 run 的 backend。

## 公开 API（`__init__.py` re-export 即公开面）

- `hub.py`
  - `SkillHub`：写面 `upsert`（校验清单强制、hash 未变幂等不写、文档级 revision CAS）、
    `mark_deleted`、`set_official_flags`（enabled=上架 / required=恒注入拒关）、
    `set_enabled`（per-namespace 启停偏好，独立表）；读面 `list_pool`（namespace 覆盖
    official；session 建会话查此面做快照）、`resolve_cards`（保 names 序，prompt 字节稳定）、
    `read_body`（正文双路：当前版 Mongo 快读 / 旧 hash 走 zip 取回）、`load_package`、
    `load_package_if_assets`（纯文档包返 None 不白走包体存储）。
  - `make_skill_hub(settings)` 异步上下文装配；`SkillHubSettings`；`seed_official`
    （部署目录只是 seed 输入，真源是库+包体）；`validate_package`（名称/保留名/文件数/
    体积/路径穿越/尖括号注入，fail-loud）；`content_hash_of`；`PackageStore` 协议与
    `make_package_store`（local/s3；worker/main 借它做 deliveries 存储）；`OFFICIAL_SCOPE`。
- `materialize.py`：`SkillMaterializerMiddleware`（before_agent 一次对账）、
  `reconcile_skill_assets`（graph state 账本 {name: content_hash} 驱动增量：目录缺失自愈
  全量重写、残留 GC、单包失败不阻断只不落账本）。
- `package.py`：`parse_frontmatter`（YAML 头 fail-loud：name 与目录同名、description 非空）、
  `SkillFrontmatter`、`SkillAssetError`。
- `supply.py`：`SKILLS_ROOT`（"/.skills/"，点前缀=能力供给不进用户文件清单）、
  `MaterializeBackend`/`ExecCapableBackend`（物化所需 backend 能力面；GC 删除按
  isinstance 探测降级）。

## 关键协作者

- 消费面：`agents/`（装配清单 resolve_cards + 挂 SkillMaterializerMiddleware）、
  `tools/skills.py`（正文双路读取）、`worker/main.py`（seed_official/deliveries 存储）。
- 跨仓：kokoro-session 直读同一 Mongo skills 集合做授权池（skills/pool）。
- 下游：pymongo async、boto3（经 to_thread）、`contract/storage`（SkillCard/SkillDoc）。

## 运行时约束

- 内容锁：zip 按 content_hash 寻址不可变，官方升级不影响已快照旧 hash 的进行中会话。
- 写入即生效（下一会话可见）；(scope,name) 唯一；并发写靠 revision CAS fail-loud。
- 包体先落、元数据后写：put 失败则 Mongo 不动，不出现悬空 package_ref。
- s3 凭据 env-only（复用 workspace 对，ADR-010）；缺凭据配 s3 即抛错。

## 扩展规则

- 新校验项加进 `validate_package`（upsert 强制执行点），不散落在调用方。
- 物化布局只在 supply.py 定义；GC 目标名必须过 `_SAFE_SKILL_NAME` 二次确认（防相对 rm 越界）。
