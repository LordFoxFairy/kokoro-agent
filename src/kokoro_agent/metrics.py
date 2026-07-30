"""worker 可观测指标（OBS-1 V1）：prometheus_client 默认注册表 + 最小 http 端点。

采集恒 fail-open——任何 record_* 内部异常都吞掉，绝不冒泡进主链路（指标故障不得杀 run）。
端点由 KOKORO_AGENT_METRICS_PORT 显式开启，缺省关（worker 无常驻 HTTP 面）。
"""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, start_http_server

LOGGER = logging.getLogger(__name__)

# dispatch CAS 认领（D5）：won=本 worker 抢到 pending→claimed；lost=迟到/重复投递被丢弃。
DISPATCH_CLAIM = Counter(
    "kokoro_agent_dispatch_claim_total",
    "dispatch CAS claim outcomes",
    ["outcome"],
)

# R2 control inbox 相位：persisted=首次落库；applied=续办 apply；superseded=stale 不 apply。
CONTROL_INBOX = Counter(
    "kokoro_agent_control_inbox_total",
    "control inbox transitions",
    ["state"],
)

# R4 critical outbox 相位：queued=分配 durable_seq 落行；published=发布确认；
# republished=宽限期无回执/崩溃后补发；receipt_state_lost=manifest 缺失告警。
OUTBOX = Counter(
    "kokoro_agent_outbox_total",
    "critical outbox transitions",
    ["state"],
)

# Non-critical live frames whose user-visible payload is already retained by the independent
# durable-output authority. These are delivery failures, not critical outbox transitions.
DURABLE_OUTPUT_DELIVERY = Counter(
    "kokoro_agent_durable_output_delivery_total",
    "non-critical durable output live delivery outcomes",
    ["state"],
)

# R3 tool journal：started 行重放（上次崩在执行中，outcome 未知）→交模型/HITL 决策。
TOOL_UNKNOWN_OUTCOME = Counter(
    "kokoro_agent_tool_unknown_outcome_total",
    "tool journal entries replayed with unknown outcome",
)

# MCP server 装配期不可用（注册表禁用遮蔽 / 凭据句柄解析失败）：占名不可用，不炸 run。
MCP_UNAVAILABLE = Counter(
    "kokoro_agent_mcp_unavailable_total",
    "mcp servers skipped as unavailable at assembly",
)

# egress 守门拒绝：目标落在禁止网段 / 主机名无法解析（SSRF 防护）。
EGRESS_BLOCKED = Counter(
    "kokoro_agent_egress_blocked_total",
    "outbound requests blocked by egress guard",
)

# 本 worker 当前活跃 run 数（心跳每拍刷新）。
ACTIVE_RUNS = Gauge(
    "kokoro_agent_active_runs",
    "active runs owned by this worker",
)

# 本 worker 当前持有租约数（活跃 run + 收养的 control 监听近似）。
LEASE_HELD = Gauge(
    "kokoro_agent_lease_held",
    "leases currently held by this worker",
)

# Agent-owned durable read collections. These gauges are refreshed from Mongo on each
# heartbeat so retention leaks remain visible across worker restarts.
DURABLE_OUTPUT_RETAINED = Gauge(
    "kokoro_agent_durable_output_retained_records",
    "estimated durable output records retained in Agent storage",
)
EXECUTION_EVIDENCE_RETAINED = Gauge(
    "kokoro_agent_execution_evidence_retained_records",
    "estimated durable execution evidence records retained in Agent storage",
)
DURABLE_REPLAY_RETENTION_SECONDS = Gauge(
    "kokoro_agent_durable_replay_retention_seconds",
    "configured minimum terminal replay window; zero means time purge is disabled",
)


def record_dispatch_claim(*, won: bool) -> None:
    try:
        DISPATCH_CLAIM.labels(outcome="won" if won else "lost").inc()
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics record_dispatch_claim failed", exc_info=True)


def record_control_inbox(state: str) -> None:
    try:
        CONTROL_INBOX.labels(state=state).inc()
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics record_control_inbox failed", exc_info=True)


def record_outbox(state: str, count: int = 1) -> None:
    try:
        if count > 0:
            OUTBOX.labels(state=state).inc(count)
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics record_outbox failed", exc_info=True)


def record_durable_output_delivery(state: str, count: int = 1) -> None:
    try:
        if count > 0:
            DURABLE_OUTPUT_DELIVERY.labels(state=state).inc(count)
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics record_durable_output_delivery failed", exc_info=True)


def record_tool_unknown_outcome() -> None:
    try:
        TOOL_UNKNOWN_OUTCOME.inc()
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics record_tool_unknown_outcome failed", exc_info=True)


def record_mcp_unavailable() -> None:
    try:
        MCP_UNAVAILABLE.inc()
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics record_mcp_unavailable failed", exc_info=True)


def record_egress_blocked() -> None:
    try:
        EGRESS_BLOCKED.inc()
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics record_egress_blocked failed", exc_info=True)


def set_lease_gauges(*, active_runs: int, lease_held: int) -> None:
    try:
        ACTIVE_RUNS.set(active_runs)
        LEASE_HELD.set(lease_held)
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics set_lease_gauges failed", exc_info=True)


def set_durable_retention_gauges(
    *, output_records: int, evidence_records: int, retention_seconds: int
) -> None:
    try:
        DURABLE_OUTPUT_RETAINED.set(output_records)
        EXECUTION_EVIDENCE_RETAINED.set(evidence_records)
        DURABLE_REPLAY_RETENTION_SECONDS.set(retention_seconds)
    except Exception:  # noqa: BLE001 — 指标采集绝不影响主链路
        LOGGER.debug("metrics set_durable_retention_gauges failed", exc_info=True)


def start_metrics_server(port: int) -> None:
    """起最小 metrics http 端点（prometheus_client 自带 WSGI server，独立线程）。
    仅 KOKORO_AGENT_METRICS_PORT 显式配置时由 main 调用；失败只告警不阻断 worker 启动。"""
    try:
        start_http_server(port)
        LOGGER.info("kokoro-agent metrics endpoint on :%d", port)
    except Exception:  # noqa: BLE001 — 端点起不来不阻断 worker 主职
        LOGGER.exception("metrics endpoint failed to bind :%d", port)
