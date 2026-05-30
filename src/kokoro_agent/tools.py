from __future__ import annotations

from collections.abc import Callable

from langchain_core.tools import BaseTool, StructuredTool

# Deterministic stamp injected for reproducibility — NOT real wall-clock time.
CLOCK_STAMP = "2026-05-30T00:00:00Z"


def echo_search(query: str) -> str:
    """Echo a search query back as a deterministic result string."""
    return f"results for {query}"


def clock() -> str:
    """Return a fixed, deterministic timestamp (not real time)."""
    return CLOCK_STAMP


# Registry: tool_name -> the underlying callable invoked by run_tool.
TOOL_CALLABLES: dict[str, Callable[..., str]] = {
    "echo_search": echo_search,
    "clock": clock,
}

# LangChain tool objects for binding onto a real model (model.bind_tools).
TOOL_OBJECTS: list[BaseTool] = [
    StructuredTool.from_function(echo_search),  # pyright: ignore[reportUnknownMemberType]
    StructuredTool.from_function(clock),  # pyright: ignore[reportUnknownMemberType]
]


def run_tool(name: str, args: dict[str, object]) -> tuple[str, str]:
    """Run a registered tool by name with keyword ``args``.

    Returns ``(status, output)`` where ``status`` is ``"ok"`` on success or
    ``"error"`` for an unknown name or any tool exception. Failures never
    propagate — the brain turns them into a ``tool.returned{status:"error"}``.
    """
    func = TOOL_CALLABLES.get(name)
    if func is None:
        return ("error", f"unknown tool: {name}")
    try:
        return ("ok", func(**args))
    except Exception as error:  # noqa: BLE001 — boundary: tool failure -> error status
        return ("error", f"{type(error).__name__}: {error}")
