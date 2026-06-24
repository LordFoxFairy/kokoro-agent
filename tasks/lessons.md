# tasks/lessons.md — 操作级教训

## L1 (2026-06-24)：v3 重写里别照搬 v2 的防御脚手架

**场景**：把 ACL 从 astream_events v2 迁到 v3 typed projections 时，我把 awaiting.py 的
`_is_object_mapping`/`_last_ai_message`、invoke/supervisor 的 `TypeGuard`+`getattr(snapshot,...)`
手刨 interrupt payload 的写法，从 v2 直接搬了过来。

**我做错的**：在 langchain 1.0+ v3、框架已锁定（永不更换）的前提下，本该直接吃框架的 typed 结构，
却把所有 interrupt/snapshot 值收成 `object` 再用 isinstance/TypeGuard 手动收窄——这是 v2 时代
（流是裸 dict）才需要的防御，在 v3 里纯属冗余噪音。用户明确反问"为什么有这些存在"。

**下次怎么避免**：迁移到强类型框架 API 时，先查框架提供的 typed 结构再写代码，别凭惯性套旧防御：
- HITL interrupt：`langchain.agents.middleware.human_in_the_loop` 有 `HITLRequest`/`ActionRequest`/
  `ReviewConfig`/`Decision` 全套 TypedDict；`langgraph.types.Interrupt`(`.value`,`.id`)、
  `StateSnapshot.interrupts: tuple[Interrupt,...]`(顶层直接有，别去 tasks[].interrupts 手刨)、
  `StateSnapshot.values: dict[str,Any]`。
- 框架值确实是 `Any` 的边界（如 langgraph 图 state values），收窄**一次**就够，别造 TypeGuard 塔。
- **唯一仍需结构校验的**：不可信的**模型工具输出**（如 write_todos 的 todos 来自 LLM），那是真·外部
  载荷洗净（项目铁律），与"框架 typed 值上套防御"是两回事，别混为一谈。

**区分原则**：框架产出的 typed 值 → 直接用其类型；外部/模型产出的不可信载荷 → Pydantic/校验洗净。

## L2 (2026-06-24)：别把自己能推理的判断题甩回给用户猜

**场景**：typed-payload 打磨，多 lens 评审后我用 AskUserQuestion 让用户在 wash_args 保留/回退、
source 收窄、interrupt 一致性三项上拍板。用户回"我又带你猜""能不能别问我"。

**我做错的**：把"≥3 轮交互评审"误解成"每轮都让用户做决定"。这三项都有充分工程依据
（goal 意图 + 代码事实 + 既定约束）可自决，却 offload 成开放选择题让用户猜。

**下次怎么避免**：交互评审 = 我出**带依据的结论**供用户复核/否决，不是把开放题推回去。
能用"代码事实 + 既定约束 + goal 意图"推出的取舍，自己定并讲清理由；只有真属于产品/业务方向、
我无依据可循的才问。问之前先自检：我是真没依据，还是在偷懒回避思考？
