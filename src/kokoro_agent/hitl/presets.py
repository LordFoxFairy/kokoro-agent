"""三预设的决策词汇：approval/question/review 各自 allowed_decisions 的单一事实源。

现状三场景是 request_human 四 kind 的预设形态。approval/question 的暂停仍由 langchain
interrupt_on 机制承载（声明面不变），review 与任意执行点原语走 request_human 载荷；三者的
决策集在此集中定义，供 permissions（interrupt_on 声明）与 approvals（wire 投影）共用。
"""

from __future__ import annotations

from kokoro_agent.contract import AllowedDecision

# kind=approval：放行 / 改参放行 / 拒绝（工具边界调用前审批）。
APPROVAL_DECISIONS: tuple[AllowedDecision, ...] = ("approve", "edit", "reject")
# kind=question：仅人工作答（ask_user_question 语义暂停点，不参与 approve/edit/reject）。
QUESTION_DECISIONS: tuple[AllowedDecision, ...] = ("respond",)
# kind=review：采纳 / 人工替换 / 废弃（工具后结果审核；edit 对已执行结果无意义）。
REVIEW_DECISIONS: tuple[AllowedDecision, ...] = ("approve", "respond", "reject")
