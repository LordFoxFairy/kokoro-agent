"""web_fetch 规格：SSRF 拒绝矩阵、HTML 提取、JSON 原样、截断、重定向逐跳复检。"""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from langchain_core.tools import StructuredTool
from pydantic import ValidationError

from kokoro_agent.tools.web_fetch import FETCH_MAX_CHARS, WebFetchArgs, make_web_fetch_tool


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 — http.server 契约名
        routes = {
            "/html": (
                "text/html; charset=utf-8",
                "<html><head><style>.x{}</style><script>evil()</script></head>"
                "<body><h1>标题</h1><p>正文 A</p><p>正文 B</p></body></html>",
            ),
            "/json": ("application/json", json.dumps({"ok": True})),
            "/big": ("text/plain", "x" * (FETCH_MAX_CHARS + 500)),
        }
        if self.path == "/giant":
            data = b"g" * (5 * 1024 * 1024)
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("location", "/html")
            self.end_headers()
            return
        if self.path == "/forbidden":
            self.send_response(403)
            self.send_header("content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"nope")
            return
        ctype, body = routes.get(self.path, ("text/plain", "fallback"))
        data = body.encode()
        self.send_response(200)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 — http.server 契约签名
        pass


@pytest.fixture(scope="module")
def base_url():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = HTTPServer(("127.0.0.1", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    thread.join(timeout=5)


def _coro(tool: StructuredTool):
    assert tool.coroutine is not None
    return tool.coroutine


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "ftp://example.com/x",
        "http://127.0.0.1/",
        "http://localhost/",
        "http://[::1]/",
        "http://10.0.0.5/",
        "http://169.254.169.254/latest/meta-data/",
        "not a url",
    ],
)
async def test_fetch_rejects_unsafe_targets(url: str) -> None:
    tool = make_web_fetch_tool()
    with pytest.raises(ValueError):
        await _coro(tool)(url=url)


async def test_fetch_extracts_html_text(base_url: str) -> None:
    tool = make_web_fetch_tool(allow_private=True)
    text = await _coro(tool)(url=f"{base_url}/html")
    assert "标题" in text and "正文 A" in text and "正文 B" in text
    assert "evil()" not in text and ".x{}" not in text


async def test_fetch_returns_json_verbatim(base_url: str) -> None:
    tool = make_web_fetch_tool(allow_private=True)
    text = await _coro(tool)(url=f"{base_url}/json")
    assert json.loads(text) == {"ok": True}


async def test_fetch_clips_oversized_body(base_url: str) -> None:
    tool = make_web_fetch_tool(allow_private=True)
    text = await _coro(tool)(url=f"{base_url}/big")
    assert len(text) <= FETCH_MAX_CHARS + 100
    assert "truncated" in text


async def test_fetch_follows_redirect_with_recheck(base_url: str) -> None:
    tool = make_web_fetch_tool(allow_private=True)
    text = await _coro(tool)(url=f"{base_url}/redirect")
    assert "正文 A" in text


async def test_fetch_returns_http_error_as_result(base_url: str) -> None:
    """非 2xx 不炸整轮：把 HTTP 错误作工具结果回给模型自行改道，而非抛异常终结 run。"""
    tool = make_web_fetch_tool(allow_private=True)
    text = await _coro(tool)(url=f"{base_url}/forbidden")
    assert "403" in text


def test_fetch_args_schema_rejects_garbage() -> None:
    with pytest.raises(ValidationError):
        WebFetchArgs.model_validate({"url": ""})
    with pytest.raises(ValidationError):
        WebFetchArgs.model_validate({"url": "http://x", "extra": 1})


async def test_fetch_streams_and_caps_giant_body(base_url: str) -> None:
    # 复审实锤：非流式 get 会整包吞下任意大 body。流式封顶后 5MB 响应只读 1MB 即断。
    tool = make_web_fetch_tool(allow_private=True)
    text = await _coro(tool)(url=f"{base_url}/giant")
    assert len(text) <= FETCH_MAX_CHARS + 100
