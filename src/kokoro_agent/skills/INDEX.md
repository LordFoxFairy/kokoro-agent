# skills — DeepAgents 原生 Skill 接线

本目录不实现第二套 Skill runtime。Agent 只声明 Skill 名称；Factory 通过 Capability
`SkillClient.resolve(...)` 得到当前 Run 可见引用，再把 `CapabilitySkillBackend` 作为
`/.skills/` 的只读逻辑 route 交给 `create_deep_agent(skills=["/.skills/"])`。Skill 元数据注入、
`SKILL.md` 渐进读取和附件访问均由 DeepAgents 原生 `SkillsMiddleware` / `read_file` 完成。

## 公开 API

- `backend.py`：`CapabilitySkillBackend` 与 `SKILLS_ROOT`；按获准引用懒读包体，拒绝所有写入。
- `package.py`：本地 fixture 包的 frontmatter 校验。
- `local_reader.py`：历史本地 fixture reader；生产 worker 不使用其 CRUD/seed 写面。
- `__init__.py`：只公开导出 DeepAgents runtime 所需的 backend/root；fixture helper
  不是包级 public API。

## 边界与约束

- Capability 拥有 Skill CRUD、可见性、用户/项目/session path 与名称解析。
- Storage 拥有包体 bytes 和对象生命周期；MinIO、AWS S3、R2 等只替换 Storage adapter。
- GA 不创建 `find_skills`、`load_skill`、mount ledger、物化/GC 或自定义 Skill prompt manifest。
- `/.skills/` 是 `CompositeBackend` 的逻辑路由，不是 GA 向 sandbox 复制文件的物理目录。
- Capability 不可用时声明 Skill 解析为空，基础 DeepAgents 对话循环继续。
