# services — Agent 应用服务

- `chat_service.py`：把可信执行身份转换为 namespace，调用 `ChatRepository`，并编排查询结果。
- `chat_dto.py`：Chat 请求、分页和视图 DTO；游标编码也在这里，避免 DTO 与应用服务混在一个文件。
- `execution/` 相关运行编排暂保留在 `execution/`，因为它是 Agent runtime 的边界；新增用例优先放入本目录的服务，而不是把 SQL 写进 worker。

服务层只依赖 `repositories/` 的 port 和 `chat/` 的领域模型，不依赖 PostgreSQL、Redis 或 HTTP server。
