# interfaces/http — Agent-owned HTTP ingress

## 边界

- `main.py` 是 HTTP 进程唯一 composition root：只解析 HTTP 业务配置与 public execution-proof descriptor；不加载 worker/private key/signer/model/sandbox/MCP。
- `execution_proof_jwks.py` 从 anchored trusted-parent walk 读取 strict Ed25519 public ring，预计算 immutable RFC 8785 JWKS snapshot；失败只形成 unavailable state。
- `server.py` 在 body、bearer、identity 与 PG/Redis factory 前处理 exact `GET|HEAD /v1/execution-proof/jwks`；其余业务路由继续委派 `AgentIngress`。
- `ingress.py` 保持既有 Run/control/evidence/chat application mapping。

HTTP process 不持有 private signing material，也不构造 signer。JWKS 是 internal-owner 匿名公钥例外；`/readyz` 仍需 bearer，并在 ring unavailable 时于依赖工厂前返回 `agent_unavailable`。

## ControlReceipt 输出

`ingress.py` 将内部入账回执投影为已发布 HTTP `ControlReceipt` 的字段白名单；首次、重放及入队失败均不输出 repository 的 `run_id`。执行作用域仍由路径、可信身份及命令摘要核验，内部持久化模型不变。
