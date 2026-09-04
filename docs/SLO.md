# kokoro-agent SLO（目标）

以下是发布前需要由监控实测的目标，不是历史成绩或保证值。

| SLI | 目标 | 窗口 | 告警 |
|---|---:|---:|---|
| HTTP health success | 99.99% | 30 天 | 5 分钟低于 99.9% |
| HTTP readiness success | 99.9% | 30 天 | 连续 5 分钟低于 99% |
| launch admission p95 | < 500 ms | 1 小时 | 连续 15 分钟超 1 s |
| control admission p95 | < 500 ms | 1 小时 | 连续 15 分钟超 1 s |
| durable event publish lag p95 | < 5 s | 1 小时 | backlog oldest > 30 s |
| lease recovery completion | < 2 × lease TTL | 1 小时 | 任一恢复超时 |
| unhandled process crash | 0 | 24 小时 | 任一即告警 |

错误预算由服务 owner 与发布流程共同管理。每次目标偏离必须关联 request/trace/run evidence 和 runbook，
不能用重启掩盖 durable 数据问题。
