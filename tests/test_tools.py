from __future__ import annotations

from datetime import datetime

import httpx
import pytest
from _pytest.monkeypatch import MonkeyPatch

from kokoro_agent.infrastructure.tools import web_fetch as web_fetch_module
from kokoro_agent.infrastructure.tools import (
    BUILT_IN_TOOLS,
    RESERVED_TOOL_NAMES,
    assert_tool_names_allowed,
)
from kokoro_agent.infrastructure.tools.current_time import current_time
from kokoro_agent.infrastructure.tools.ask_user_question import (
    ASK_USER_QUESTION_TOOL,
    AskUserQuestionInput,
    ask_user_question,
)
from kokoro_agent.infrastructure.tools.web_fetch import (
    FETCH_MAX_BYTES,
    FETCH_MAX_CHARS,
    web_fetch,
)

# 公网 IP，让 _host_is_blocked 放行（真实 DNS 不参与测试）。
_PUBLIC_IP = "93.184.216.34"


def _public_resolver(_host: str) -> list[str]:
    return [_PUBLIC_IP]


def _client_with(handler: httpx.MockTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=handler)


def _serve(monkeypatch: MonkeyPatch, handler: httpx.MockTransport) -> None:
    monkeypatch.setattr(web_fetch_module, "make_http_client", lambda: _client_with(handler))
    monkeypatch.setattr(web_fetch_module, "_resolve_ips", _public_resolver)


# --- registry guard -----------------------------------------------------------


def test_built_in_tools_expose_current_time_web_fetch_and_ask_user_question() -> None:
    assert {tool.name for tool in BUILT_IN_TOOLS} == {
        "current_time",
        "web_fetch",
        "ask_user_question",
    }


@pytest.mark.parametrize("name", ["write_todos", "task", "read_file", "execute"])
def test_reserved_tool_name_is_rejected(name: str) -> None:
    with pytest.raises(ValueError):
        assert_tool_names_allowed([name])


def test_duplicate_tool_name_is_rejected() -> None:
    with pytest.raises(ValueError):
        assert_tool_names_allowed(["current_time", "current_time"])


def test_registry_names_pass_their_own_guard() -> None:
    assert_tool_names_allowed([tool.name for tool in BUILT_IN_TOOLS])
    assert RESERVED_TOOL_NAMES.isdisjoint({tool.name for tool in BUILT_IN_TOOLS})


def test_import_time_guard_is_non_vacuous_over_the_real_registry() -> None:
    # import 期 line 100 正是对 BUILT_IN_TOOLS 名字跑此护栏。钉死：注册表形态混入保留名必抛
    #（reload 路径在 Python 下不可行——它重跑源码会覆盖 monkeypatch 的 RESERVED）。
    names = [tool.name for tool in BUILT_IN_TOOLS] + ["task"]
    with pytest.raises(ValueError):
        assert_tool_names_allowed(names)


# --- current_time ------------------------------------------------------------------------


def test_current_time_returns_timezone_aware_iso() -> None:
    assert datetime.fromisoformat(current_time()).tzinfo is not None


# --- ask_user_question -------------------------------------------------------


def test_ask_user_question_input_accepts_camel_case_schema() -> None:
    parsed = AskUserQuestionInput.model_validate(
        {
            "prompt": "选一个",
            "inputType": "single_choice",
            "options": [{"id": "a", "label": "A"}],
            "allowCustomOption": True,
        }
    )

    assert parsed.input_type == "single_choice"
    assert parsed.allow_custom_option is True


def test_ask_user_question_rejects_unknown_fields() -> None:
    with pytest.raises(ValueError):
        AskUserQuestionInput.model_validate(
            {"prompt": "x", "inputType": "text", "unexpected": True}
        )


async def test_ask_user_question_direct_execution_raises() -> None:
    with pytest.raises(RuntimeError, match="HITL respond"):
        await ask_user_question(prompt="x", input_type="text")


def test_ask_user_question_tool_is_async_only() -> None:
    assert ASK_USER_QUESTION_TOOL.func is None
    assert ASK_USER_QUESTION_TOOL.coroutine is ask_user_question


# --- web_fetch: scheme guard (precedence-tight) ---------------------------------


@pytest.mark.parametrize(
    "url",
    ["file:///etc/passwd", "ftp://x/y", "javascript:alert(1)", "", "not-a-url"],
)
async def test_web_fetch_rejects_non_http_schemes(url: str) -> None:
    # 每个 scheme 独立锁定精确拒绝前缀（非 (A and B) or C 的恒真弱断言）。
    result = await web_fetch(url)
    assert result.startswith("抓取失败：只支持 http/https")


# --- web_fetch: SSRF -------------------------------------------------------------


async def test_web_fetch_blocks_a_direct_private_host(monkeypatch: MonkeyPatch) -> None:
    served = False

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal served
        served = True
        return httpx.Response(200, text="INTERNAL")

    def loopback(_host: str) -> list[str]:
        return ["127.0.0.1"]

    monkeypatch.setattr(web_fetch_module, "make_http_client", lambda: _client_with(httpx.MockTransport(handler)))
    monkeypatch.setattr(web_fetch_module, "_resolve_ips", loopback)

    result = await web_fetch("http://router.local/")
    assert result.startswith("抓取失败：拒绝访问")
    assert served is False  # 内网请求根本没发出


async def test_web_fetch_blocks_a_redirect_to_a_private_host(monkeypatch: MonkeyPatch) -> None:
    internal_served = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal internal_served
        if request.url.host == "public.test":
            return httpx.Response(302, headers={"location": "http://metadata.internal/latest/secret"})
        internal_served = True
        return httpx.Response(200, text="INTERNAL-METADATA-SECRET")

    def split_resolver(host: str) -> list[str]:
        # 重定向旁路核心：每跳都要按解析出的 IP 复校验。
        return [_PUBLIC_IP] if host == "public.test" else ["169.254.169.254"]

    monkeypatch.setattr(web_fetch_module, "make_http_client", lambda: _client_with(httpx.MockTransport(handler)))
    monkeypatch.setattr(web_fetch_module, "_resolve_ips", split_resolver)

    result = await web_fetch("http://public.test/")
    assert result.startswith("抓取失败：拒绝访问")
    assert internal_served is False  # 内网响应体绝不被取回
    assert "SECRET" not in result


@pytest.mark.parametrize(
    ("resolved_ip", "blocked"),
    [
        ("127.0.0.1", True),
        ("169.254.169.254", True),  # 云 metadata
        ("10.1.2.3", True),
        ("172.16.5.5", True),
        ("192.168.1.1", True),
        ("0.0.0.0", True),
        ("::1", True),
        ("8.8.8.8", False),  # 公网
        ("198.18.2.194", False),  # 基准段：TUN 代理把公网域名映射到这里，故意放行
    ],
)
async def test_web_fetch_blocks_by_resolved_ip_class(
    monkeypatch: MonkeyPatch, resolved_ip: str, blocked: bool
) -> None:
    def resolver(_host: str) -> list[str]:
        return [resolved_ip]

    monkeypatch.setattr(web_fetch_module, "_resolve_ips", resolver)
    monkeypatch.setattr(
        web_fetch_module, "make_http_client",
        lambda: _client_with(httpx.MockTransport(lambda _r: httpx.Response(200, text="OK"))),
    )
    result = await web_fetch("http://host.test/")
    if blocked:
        assert result.startswith("抓取失败：拒绝访问")
    else:
        assert result == "OK"


# --- web_fetch: SSRF 守卫 fail-closed 分支（解析失败/空/非法 IP/空 host/真实解析器）---------


async def test_web_fetch_blocks_empty_host() -> None:
    # 空 hostname（如 http:///path）→ host 收为 "" → fail closed，根本不解析、不发请求。
    result = await web_fetch("http:///nowhere")
    assert result.startswith("抓取失败：拒绝访问")


async def test_web_fetch_fails_closed_on_dns_failure(monkeypatch: MonkeyPatch) -> None:
    def boom(_host: str) -> list[str]:
        raise OSError("dns down")

    monkeypatch.setattr(web_fetch_module, "_resolve_ips", boom)
    # 解析失败即拒（fail closed）：DNS 不可用绝不退化为放行。
    assert (await web_fetch("http://anything.test/")).startswith("抓取失败：拒绝访问")


async def test_web_fetch_fails_closed_on_empty_resolution(monkeypatch: MonkeyPatch) -> None:
    def empty(_host: str) -> list[str]:
        return []

    monkeypatch.setattr(web_fetch_module, "_resolve_ips", empty)
    assert (await web_fetch("http://anything.test/")).startswith("抓取失败：拒绝访问")


async def test_web_fetch_fails_closed_on_unparseable_resolved_ip(
    monkeypatch: MonkeyPatch,
) -> None:
    def garbage(_host: str) -> list[str]:
        return ["not-an-ip"]

    monkeypatch.setattr(web_fetch_module, "_resolve_ips", garbage)
    assert (await web_fetch("http://anything.test/")).startswith("抓取失败：拒绝访问")


async def test_web_fetch_real_resolver_blocks_localhost() -> None:
    # 不打桩 _resolve_ips：走真实 getaddrinfo（覆盖 _resolve_ips 实体），localhost 解析为环回 → 拦截。
    assert (await web_fetch("http://localhost/")).startswith("抓取失败：拒绝访问")


async def test_web_fetch_allows_a_public_host(monkeypatch: MonkeyPatch) -> None:
    _serve(monkeypatch, httpx.MockTransport(lambda _r: httpx.Response(200, text="hello kokoro")))
    assert await web_fetch("https://example.com") == "hello kokoro"


async def test_web_fetch_follows_a_public_redirect(monkeypatch: MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            return httpx.Response(301, headers={"location": "https://example.com/final"})
        return httpx.Response(200, text="final page")

    _serve(monkeypatch, httpx.MockTransport(handler))
    assert await web_fetch("https://example.com/") == "final page"


async def test_web_fetch_caps_the_redirect_chain(monkeypatch: MonkeyPatch) -> None:
    _serve(
        monkeypatch,
        httpx.MockTransport(lambda _r: httpx.Response(302, headers={"location": "https://example.com/loop"})),
    )
    result = await web_fetch("https://example.com/")
    assert result.startswith("抓取失败：重定向")


# --- web_fetch: size + deadline + errors ----------------------------------------


@pytest.mark.parametrize(
    ("size", "marked"),
    [(FETCH_MAX_CHARS - 1, False), (FETCH_MAX_CHARS, False), (FETCH_MAX_CHARS + 1, True)],
)
async def test_web_fetch_truncation_boundary(monkeypatch: MonkeyPatch, size: int, marked: bool) -> None:
    _serve(monkeypatch, httpx.MockTransport(lambda _r: httpx.Response(200, text="x" * size)))
    result = await web_fetch("https://example.com")
    assert ("截断" in result) is marked
    assert len(result.replace("…（内容过长，已在 20000 字符处截断）", "")) <= FETCH_MAX_CHARS


async def test_web_fetch_byte_caps_a_huge_body(monkeypatch: MonkeyPatch) -> None:
    _serve(monkeypatch, httpx.MockTransport(lambda _r: httpx.Response(200, text="x" * (FETCH_MAX_CHARS * 3))))
    result = await web_fetch("https://example.com")
    assert len(result) <= FETCH_MAX_CHARS + 50
    assert "截断" in result


async def test_web_fetch_byte_break_stops_at_byte_cap(monkeypatch: MonkeyPatch) -> None:
    # 响应体 >= FETCH_MAX_BYTES：读到字节硬上限即 break（挡解压尖峰/超大体），不吞完整个流。
    _serve(
        monkeypatch,
        httpx.MockTransport(lambda _r: httpx.Response(200, text="x" * (FETCH_MAX_BYTES + 5000))),
    )
    result = await web_fetch("https://example.com")
    assert "截断" in result


async def test_web_fetch_wall_clock_deadline_returns_text(monkeypatch: MonkeyPatch) -> None:
    import asyncio

    async def slow(_request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(200, text="too late")

    _serve(monkeypatch, httpx.MockTransport(slow))
    monkeypatch.setattr(web_fetch_module, "FETCH_DEADLINE_S", 0.05)
    result = await web_fetch("https://example.com")
    # 慢速 drip：墙钟封顶以文本返回，不挂起、不抛异常打死整轮 run。
    assert result.startswith("抓取失败：超过")


async def test_web_fetch_surfaces_http_errors_as_text(monkeypatch: MonkeyPatch) -> None:
    _serve(monkeypatch, httpx.MockTransport(lambda _r: httpx.Response(503)))
    result = await web_fetch("https://example.com")
    assert "失败" in result and "503" in result


async def test_web_fetch_surfaces_connect_errors_as_text(monkeypatch: MonkeyPatch) -> None:
    def boom(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    _serve(monkeypatch, httpx.MockTransport(boom))
    result = await web_fetch("https://example.com")
    assert "失败" in result
