"""OBS-1 worker 指标规格：默认注册表渲染含 kokoro_agent_ 前缀关键指标名；record_* 幂等增量、
采集 fail-open（异常内吞不冒泡）。断言指标名存在与增量，不断言精确值（避开 flake）。"""

from __future__ import annotations

from prometheus_client import REGISTRY, generate_latest

from kokoro_agent import metrics


def _value(name: str, **labels: str) -> float:
    # 公共内省口（documented test API）：缺样本返 0（未被 record 过的 label 组合尚无样本）。
    sample = REGISTRY.get_sample_value(name, labels or None)
    return 0.0 if sample is None else sample


def test_registry_exposes_kokoro_agent_metric_names() -> None:
    body = generate_latest().decode()
    for name in (
        "kokoro_agent_dispatch_claim_total",
        "kokoro_agent_control_inbox_total",
        "kokoro_agent_outbox_total",
        "kokoro_agent_tool_unknown_outcome_total",
        "kokoro_agent_mcp_unavailable_total",
        "kokoro_agent_egress_blocked_total",
        "kokoro_agent_active_runs",
        "kokoro_agent_lease_held",
        "kokoro_agent_durable_output_retained_records",
        "kokoro_agent_execution_evidence_retained_records",
        "kokoro_agent_durable_replay_retention_seconds",
    ):
        assert name in body


def test_dispatch_claim_counts_won_and_lost() -> None:
    before_won = _value("kokoro_agent_dispatch_claim_total", outcome="won")
    before_lost = _value("kokoro_agent_dispatch_claim_total", outcome="lost")
    metrics.record_dispatch_claim(won=True)
    metrics.record_dispatch_claim(won=False)
    assert _value("kokoro_agent_dispatch_claim_total", outcome="won") == before_won + 1
    assert _value("kokoro_agent_dispatch_claim_total", outcome="lost") == before_lost + 1


def test_outbox_transitions_increment_by_state() -> None:
    before = _value("kokoro_agent_outbox_total", state="republished")
    metrics.record_outbox("republished")
    metrics.record_outbox("queued", count=0)  # count<=0 不增量
    assert _value("kokoro_agent_outbox_total", state="republished") == before + 1


def test_egress_blocked_counts_on_exception_construction() -> None:
    from kokoro_agent.mcp.egress import EgressBlocked

    before = _value("kokoro_agent_egress_blocked_total")
    EgressBlocked("目标地址落在禁止网段")
    assert _value("kokoro_agent_egress_blocked_total") == before + 1


def test_lease_gauges_set_current_value() -> None:
    metrics.set_lease_gauges(active_runs=3, lease_held=5)
    assert _value("kokoro_agent_active_runs") == 3
    assert _value("kokoro_agent_lease_held") == 5


def test_durable_retention_gauges_set_current_value() -> None:
    metrics.set_durable_retention_gauges(
        output_records=7, evidence_records=3, retention_seconds=3600
    )
    assert _value("kokoro_agent_durable_output_retained_records") == 7
    assert _value("kokoro_agent_execution_evidence_retained_records") == 3
    assert _value("kokoro_agent_durable_replay_retention_seconds") == 3600
