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

## L3 (2026-06-24)：设计文档放 docs/superpowers/，不放 tasks/、不 commit

**场景**：写分通道 spec，先往 `docs/superpowers/specs/` 写（被 gitignore），又自作主张移到 `tasks/`
并 commit。用户怒："你又不是看不到"——`docs/superpowers/specs/` 里**本就躺着 v3 那批 spec/plan**。

**我做错的**：(1) 没先 `ls docs/superpowers/specs/` 看兄弟文件就乱放；(2) 看到 docs/ 被 ignore，
就擅自改去 tasks/ 还 commit——既破坏约定（tasks/ 只放 todo.md/lessons.md），又把本该本地的
设计文档塞进版本库。

**下次怎么避免**：本仓设计产物的家 = **`docs/superpowers/specs/`（spec）、`docs/superpowers/plans/`（plan）**，
gitignore、**本地工作文档、不 commit**。写前先 `ls` 看既有兄弟、就地放进去。`tasks/` 仅
`todo.md`/`lessons.md`。看到目录被 ignore，是"别 commit"的信号，不是"换地方"的信号。绝不为了让它
被 track 而搬家。

## L4 (2026-06-24)：AgentEvent 信封是唯一 JSON 边界，别在它前面再撒校验

**场景**：tool_call_start 的 args 我加了 `wash_args`/`as_json_args` 在 transformer 里逐键/整盘
validate JSON。用户连呼"为什么要丢弃""多此一举""毫无意义"。

**我做错的**：`AgentEvent` 信封本身就是 `model_config(strict=True)` + `data: dict[str, JsonValue]`，
`model_validate` 时**已经把整个 data（含 args）校验过一遍 JSON 安全**。我在它前面又 validate 一次
args，纯属重复；而且"逐键 try/except 丢弃"还会静默吞掉键，把本来能跑的 args 搞得不一致。这是
[[L1]] 同一个病的又一次发作——在框架/模型产出的结构化数据上撒手搓校验。

**下次怎么避免**：**单一 strict 边界原则**——对外只有 `AgentEvent.model_validate` 这一道 JSON 关；
它前面的投影层只管把框架/模型给的数据**原样透传**，类型如实标（langchain 给 `dict[str, object]`
就标 `dict[str, object]`，别强转 `JsonValue` 逼出"转换/洗净"代码）。模型生成的 tool args 必是 JSON，
真出现非 JSON 让信封那一关报错即可，不在前面静默丢。

**补（2026-06-24，用户进一步纠正）**：连 `custom_event` 的 `_wash` 也删了——同理，custom 载荷也被
信封校验，wash 多余；`get_stream_writer` 业务遥测本就该是 JSON，非 JSON 让信封报错即可。`_ev` 这种
偷懒缩写也改成标准名 `_make_event`。**原则贯彻到底：投影层零 wash，全部原样透传，类型如实。**

## L5 (2026-06-24)：wire 是 canonical 就该自洽对称，别把 consumer 细节倒灌进 wire

**场景**：分通道设计里我让 reasoning 通道"只发 delta 不发 final"，而 text 通道发 delta+final。
理由是"web 现有 thinking reducer 是纯续写"。用户质疑："langchain 本身这么干吗？不一致后续排查困难吗？"

**我做错的**：把**消费端（web P4）的当前实现细节**倒灌进了**对外 canonical wire** 的设计，造成两通道
不对称。langchain 的 `.text`/`.reasoning` projection 本就对称；wire 是单一真理源，就该自洽。不对称 =
后续"为何 text 有终态帧 reasoning 没有"翻半天。

**下次怎么避免**：wire/契约设计只对**自身一致性 + 上游（框架）语义**负责，**不为下游消费者的当前实现
打折**。下游不支持就 P4 改下游（reasoning final 当 replace，与 text 一致），而非把 wire 改残。
对称的东西就对称建模。
