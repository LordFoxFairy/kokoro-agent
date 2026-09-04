# ADR-003：Durable Run 与 Redis 通知边界

- 状态：Accepted
- 日期：2026-09-04

## 决策

PostgreSQL 保存不可变 request fence、dispatch intent、lease generation、receipt、outbox 和 chat facts；
Redis 只保存可丢失/可重放通知。所有副作用由 generation fence 保护，终态通过原子 claim 保证一次收口。

## 验证

集成测试必须覆盖落库后发布前崩溃、重复投递、同 owner 重启、旧 generation 写入、outbox republish、control
reapply 和 sandbox cleanup retry。
