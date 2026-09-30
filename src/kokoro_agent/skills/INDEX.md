# skills — DeepAgents 原生 Skill 接线

本目录不实现第二套runtime；Run冻结的exact refs由Platform current authorization裁决。
- `package.py`：固定v4 ZIP32/profile/manifest的纯内存校验，返回原始文件bytes；不写宿主/不执行脚本。
- `backend.py`：`TypedSkillBackend`、`SKILLS_ROOT`；路径为exact SkillId无填充base64url，只读、无别名。
- `__init__.py`：空导入边界，避免client/package/backend循环依赖。

每次ls/read/glob/grep/download均通过run-bound client重新取得当前批准并校验GET/ZIP；任何授权/包失败向上抛出，
不把已声明Skill转为空列表继续。无bytes缓存，glob/grep逐包处理，批下载128MiB/128路径，grep1MiB/1000条明确上限。
`SKILL.md`元数据与渐进披露仍由原生DeepAgents消费，二进制download保持原字节，非UTF8 read显式报错。
所有write/edit/upload拒绝；`/.skills/`仅虚拟内存路由，无物化路径或跨owner SQL。
