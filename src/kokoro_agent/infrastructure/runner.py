from __future__ import annotations

from kokoro_agent.infrastructure.http_server import build_server


# runner 只负责启动 HTTP + SSE 入口，避免把传输细节泄露回应用层。
def run(host: str = "127.0.0.1", port: int = 8001) -> None:
    server = build_server(host=host, port=port)
    server.serve_forever()
