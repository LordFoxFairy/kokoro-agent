# ADR-005：原预算无预占关闭事实及一次 additive expand

- 状态：Accepted；源码实施中，正常数据应用由 Root 单独执行。
- 日期：2026-11-15
- 授权及范围：Root `docs/reports/2026-11-14-historical-financial-unknown-readonly.md` §6；仅原两笔终态未派发、授权结果未知且 Billing 无预占的恢复残留，不处理旧 held、资金或真实模型。

## 上下文与选择

现 `financial_ack_*` 只保存真实 settlement ACK；Run receipt/control/checkpoint audit 各有投递、控制和 native 执行语义。复用它们会把“原预算永久未创建”伪装成结算或执行结果。因此在 Agent owner 内新增一张窄 append-only `kokoro_agent_model_financial_close`，而非增加通用 reconciliation 平台。Billing 沿原命令锁形成终局 fence，BFF 复用现受信 Agent 通道和原 Run submitter 转发；Agent 只保存 owner close 凭据及与原 sealed evidence 的关联，不成为账务 owner。

## 决策

1. 原 Run/Attempt/Evidence、unknown、usage 和 ACK 全部保持；只新增 close 行。该行与 attempt/evidence 同生命周期，不借 Chat retention 删除，不设置新的 TTL。
2. operator 必须指定单一 tenant/run/attempt 和预期 evidence event/revision/digest；前后短事务按 Run→Attempt 锁复验原 terminal、可信 user actor=subject、not_dispatched/requested_unknown、原 canonical head、dispatch/admission/ACK 空。BFF 网络在事务外；原 key/binding 不变。并发重放只返回原相同事实，漂移冲突。
3. 正常 worker 不触发 close。已有恢复选择只排除经过校验的对应 close；迟到 admission/ACK writer 在原 Attempt 锁内拒绝与关闭事实冲突。独立 close 查询不伪造 settlement ACK。
4. 有数据演进仅批准本表的显式 `db:expand-model-financial-close`：从唯一 `database/schema.sql` 标记块读取 DDL，校验旧 owner catalog，在 owner advisory transaction lock 下添加本表并核新 catalog。重复执行仅验证，漂移即停；事务超时失败回滚。不重跑 fresh installer，不清空或重建数据库。
5. 本次为 ADR-002 的窄 additive 例外，不引入 migration runner、ledger、历史迁移链、第二份可编辑 DDL，亦不授权其他未来 DDL。新 worker/readiness 要求扩展成功后才启用。

## 替代方案

- SELECT miss / 将 unknown 改零：缺少防迟到终局，且篡改原证据，淘汰。
- 复用 settlement ACK / control receipt：错误事实归属，淘汰。
- 通用 reconciliation 服务或自动扫描关闭：超出本两笔范围，淘汰。
- 空库重装：丢失正常数据，淘汰。

## 失败恢复与验证

Billing 关闭成功、Agent 未落行时，重试原命令读取相同 owner receipt 再关联；不 reserve/settle/capture/release/refund。新表已提交后回退应用保留表和 close 行，不自动 DROP；旧 worker 不认识关闭事实，不能将其重新启用称为完成回退，必须保留新 selector 或停止该恢复进程。

真实 PostgreSQL 验证包含 populated schema additive 重放/漂移停止、原行内容和 xmin 不变、前后权限及 head 复核、并发单行重放、迟到 writer 冲突、损坏 close 不冒完成。定向工程和资源结果由本次交付记录报告，不以本 ADR 代替验收。正常两笔和资金快照由 Root 独立核验；本仓隔离测试不代表已实际处置。
