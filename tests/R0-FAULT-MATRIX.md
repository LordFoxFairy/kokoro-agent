# R0 故障注入护栏矩阵 — kokoro-agent

Wave 2 R0：为总设计稿 §2.3 已证实缺陷写「先红后绿」注入钉，不改 src。
钉以 `pytest.mark.xfail(strict=True)` 标注：当前 assertion 必然失败→xfail（套件保持绿）；
缺陷修复后转 XPASS，strict fail-loud，提醒回来收口本钉并去标。

测试文件：`tests/test_r0_fault_matrix.py`（用 `tests/fakes.py` 内 FakeBus/FakeLedger，纯进程内，不需真后端）。

| 钉 | 归属 | 注入点 | 缺陷（当前语义） | 断言（期望/修复后语义） |
| --- | --- | --- | --- | --- |
| `test_request_not_acked_before_durable_claim_persists` | R1 | `worker/supervisor.py:118-120` `serve`：parse 后即 `bus.ack`，早于 dispatch→`_on_request`→`store.try_claim`（durable claim，`:198`）。注入 `try_claim` 于持久化前崩溃。 | ACK 先于 durable claim。崩溃于 parse 后、claim 落库前时，消息已 ACK 即从 PEL 消失，无重投（恢复权本应在租约，但租约随 claim 才建立）。 | durable claim 未落地 → 请求消息**不得 ACK**（`"req-1" not in bus.acked`），留 PEL 可重投。当前 ACK 已发生→红。 |
| `test_terminal_frame_not_silently_dropped_on_publish_failure` | R4 | `execution/run_agent.py:62-76` `invoke_once`：`claim_terminal()`（`:62`）先消耗终态权，`emitter.emit(RunCompletedPayload)`（`:69`）后发布；publish 抛错被 `:73` 顶层 except 吞掉（再 claim 得 False→什么都不发）。注入终态帧首次 publish 抛错 + 一次性终态权。 | 终态权在发布前消耗；关键状态帧 publish 失败被静默吞掉、无 durable outbox/补发，终态帧永失。 | 终态 publish 瞬时故障**不得静默丢弃**——经补发投递到事件流，或上抛触发重投（`raised or terminal≥1`）。当前既不补发也不上抛→红。 |

## 纲领依据

- §2.3：「request/control 在 durable claim/inbox 前 ACK」「agent terminal 在事件发布前消耗终态权；关键状态帧发布失败可直接丢弃」。
- §7：「critical publish 失败 → agent outbox pending 自动补发；固定 event_id/durable_seq」。
- §8.3：「request 读出后 claim 前、claim 后 ACK 前」「terminal intent 后 publish 前」。

## 收口提示（给 R1 / R4 实现者）

- 钉 1（R1）自动翻绿：把 `bus.ack` 后置到 `try_claim` 成功之后即 XPASS。清晰的干净钉。
- 钉 2（R4）依赖尚未落地的 agent critical/terminal outbox 基础设施：断言取「补发到事件流 **或** 上抛」的并集以覆盖多种修复形态；若 R4 采用「静默落 outbox、既不上抛也不即时补发到 live 流」的形态，需在实现时把本钉断言改指向 outbox pending 观测面。无论哪种，strict xfail 都会在终态帧不再静默丢弃时提醒收口。
