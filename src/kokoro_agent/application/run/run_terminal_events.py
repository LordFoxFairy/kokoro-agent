"""应用层：worker 在 run 流之外注入的终态事件工厂（统一 seq 契约与载荷形状）。"""

from __future__ import annotations

from kokoro_agent.application.events.agent_event import AgentEvent


def run_failed_event(run_id: str, error_kind: str, message: str) -> AgentEvent:
    """模型解析/准入失败时的终态：发生在任何 run 事件之前，故 seq=1 即流首条。"""
    return AgentEvent(
        kind="run.failed",
        run_id=run_id,
        seq=1,
        payload={"error_kind": error_kind, "message": message},
    )


def run_cancelled_event(run_id: str) -> AgentEvent:
    """用户 cancel 后 worker 补发的终态：run 流已被 cancel 截断（驱动 seq≥1 不再续发），
    此条为流外补码，seq=0 作哨兵——session normalizer 对终态豁免去重，event_id 由
    (run_id, seq, kind) 派生，0 与驱动 seq 不冲突。"""
    return AgentEvent(
        kind="run.completed",
        run_id=run_id,
        seq=0,
        payload={"status": "cancelled"},
    )
