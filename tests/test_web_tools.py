"""web 底层工具规格：fetch 的 SSRF 防御/提取/截断，search 的 provider 协议与 zhipu 解析边界。"""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from langchain_core.tools import StructuredTool
from pydantic import ValidationError

from kokoro_agent.tools.web import (
    FETCH_MAX_CHARS,
    SearchHit,
    WebFetchArgs,
    make_web_fetch_tool,
    make_web_search_tool,
    parse_zhipu_response,
)

# --- fixture 服务器（loopback：测试用 allow_private 工厂） ---


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
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("location", "/html")
            self.end_headers()
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


# --- web_fetch ---


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


def test_fetch_args_schema_rejects_garbage() -> None:
    with pytest.raises(ValidationError):
        WebFetchArgs.model_validate({"url": ""})
    with pytest.raises(ValidationError):
        WebFetchArgs.model_validate({"url": "http://x", "extra": 1})


# --- web_search ---


async def test_search_tool_formats_hits() -> None:
    async def fake_search(query: str, count: int) -> list[SearchHit]:
        assert query == "python"
        return [SearchHit(title="T1", url="https://a", snippet="S1")]

    tool = make_web_search_tool(fake_search)
    out = await _coro(tool)(query="python")
    assert "T1" in out and "https://a" in out and "S1" in out


async def test_search_tool_reports_empty() -> None:
    async def fake_search(query: str, count: int) -> list[SearchHit]:
        return []

    tool = make_web_search_tool(fake_search)
    assert "no results" in await _coro(tool)(query="python")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"search_result": []}, 0),
        ({}, 0),
        (
            {"search_result": [{"title": "T", "link": "https://a", "content": "C"}]},
            1,
        ),
        (
            {"search_result": [{"title": "T", "url": "https://b"}]},  # 缺 content/link 变体
            1,
        ),
        ({"search_result": [{"no_title": 1}]}, 0),  # 无 url 的脏条目剔除
    ],
)
def test_parse_zhipu_response_boundary_matrix(raw: dict[str, object], expected: int) -> None:
    hits = parse_zhipu_response(raw)
    assert len(hits) == expected
    for hit in hits:
        assert hit.url
