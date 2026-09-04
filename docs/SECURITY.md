# kokoro-agent 安全设计

## Trust boundary

浏览器只能访问 Web/BFF；Agent HTTP 只接受部署注入的 service bearer。BFF/IAM 负责认证与授权断言，Agent
验证断言所需的 tenant、subject、actor 和 assertion ref，并从受信 context 派生 namespace。body 中的
tenant、user、workspace、namespace 或 permission 不作为授权真源。

## 数据隔离

每个 repository 查询显式接收 namespace/tenant scope；run、session、history、replay、control 和 evidence
都先做 scope 检查。无外键时由应用事务、锁、状态机、唯一约束和 reconciliation 维护引用完整性。跨 owner
不读数据库、不做 JOIN。

## Secret 与出站

API key、service token、MCP header 和 Storage credential 只来自环境/secret provider，不进入日志、Run
request、checkpoint、event payload 或公开 response。MCP egress policy 在 worker 启动时解析一次；外部
HTTP client 必须有 connect/read/overall timeout、取消传播、响应大小上限和错误归一。

## 日志与输入

结构化日志使用 service、operation、request_id、trace_id、结果和耗时；对 token、密码、原始 prompt、
provider payload 和文件内容做脱敏。JSON body 有大小上限，未知字段拒绝，动态 SQL 标识符使用白名单，值
全部参数绑定。错误只返回稳定 code/message。

## 发布

CI 阻断依赖、源码和 secret 扫描；镜像使用不可变 base digest、非 root、healthcheck、SBOM、provenance、
漏洞扫描和签名/attestation。第三方 GitHub Action 固定完整 commit SHA。
