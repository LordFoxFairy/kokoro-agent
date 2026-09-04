# ADR-002：Canonical schema 与应用层关系完整性

- 状态：Accepted
- 日期：2026-09-04

## 决策

`database/schema.sql` 是 V1 唯一 DDL，支持空数据库安装；不保留历史 migration runner、外键或跨 owner
`JOIN`。同一 owner 内的 JOIN 允许，但必须参数化、带 tenant scope、使用明确列和稳定排序。跨表关系由
Application 事务、固定锁顺序、状态检查、唯一约束和 reconciliation 维护。

## 影响

这不是所有系统的行业默认值，而是 Kokoro V1 的 clean-slate 取舍。未来需要不停机演进时必须另立 ADR，不能
偷偷把迁移链塞回当前实现。
