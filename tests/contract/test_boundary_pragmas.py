"""第三方边界豁免政策：pragma 全量清单锁死，新增或漂移必先过本测试评审。"""

from __future__ import annotations

import re
from pathlib import Path

# 每项必须能指认上游缺口（WHY 注释在对应文件头）；扩表 = 显式评审动作。
ALLOWED: dict[str, frozenset[str]] = {
    # deepagents create_deep_agent 返回未解 ResponseT，BACKEND_TYPES 的 ToolRuntime 泛型也未闭合。
    "src/kokoro_agent/agent_factory.py": frozenset(
        {
            "reportUnknownArgumentType",
            "reportUnknownParameterType",
            "reportUnknownVariableType",
        }
    ),
    # DeepAgents/langgraph-swarm 的 create_deep_agent/create_swarm 返回未解泛型；只隔离官方构造边界。
    "src/kokoro_agent/swarm.py": frozenset(
        {
            "reportUnknownVariableType",
            "reportUnknownMemberType",
            "reportArgumentType",
            "reportUnknownArgumentType",
        }
    ),
    # DeepAgents direct contract probe exercises third-party generic state members.
    "tests/contract/test_deepagents.py": frozenset(
        {
            "reportUnknownMemberType",
            "reportUnknownParameterType",
            "reportUnknownVariableType",
            "reportAttributeAccessIssue",
            "reportOptionalMemberAccess",
            "reportUnknownArgumentType",
            "reportIncompatibleMethodOverride",
            "reportPrivateUsage",
        }
    ),
    # Official Swarm probe exercises third-party generic graph/state members.
    "tests/contract/test_official_swarm.py": frozenset(
        {
            "reportUnknownMemberType",
            "reportUnknownParameterType",
            "reportUnknownVariableType",
        }
    ),
    # jsonschema 无 py.typed：kind=input 的 resume value 校验属未解上游类型（隔离于 _schema_error）。
    "src/kokoro_agent/hitl/input.py": frozenset(
        {
            "reportUnknownMemberType",
            "reportUnknownArgumentType",
            "reportUnknownVariableType",
        }
    ),
    # langchain-core BaseTool.ainvoke/tool_call_schema 未解泛型（MCP 稳定面转发调用）。
    "src/kokoro_agent/mcp/tools.py": frozenset({"reportUnknownMemberType"}),
    # langchain-core BaseTool.ainvoke 注解含裸 dict。
    "tests/e2e/test_mcp_live.py": frozenset({"reportUnknownMemberType"}),
    # 同上：deliver 工具经 ainvoke 驱动断言。
    "tests/unit/tools/test_deliver.py": frozenset({"reportUnknownMemberType"}),
    # 同上：MCP 稳定三工具经 ainvoke 驱动断言。
    "tests/unit/mcp/test_mcp_tools.py": frozenset({"reportUnknownMemberType"}),
    # Upstream constructor generics are normalized once for real-graph tests.
    "tests/support/deepagents.py": frozenset(
        {"reportUnknownArgumentType", "reportUnknownVariableType"}
    ),
    # psycopg dict-row adapters use qualified SQL built from validated schema identifiers.
    "src/kokoro_agent/storage/postgres.py": frozenset(
        {"reportCallIssue", "reportArgumentType", "reportReturnType"}
    ),
    "src/kokoro_agent/storage/checkpoints.py": frozenset(
        {"reportCallIssue", "reportArgumentType", "reportIncompatibleMethodOverride"}
    ),
    "src/kokoro_agent/storage/ledger.py": frozenset(
        {
            "reportCallIssue",
            "reportArgumentType",
            "reportReturnType",
            "reportUnknownArgumentType",
            "reportUnknownVariableType",
            "reportUnknownMemberType",
            "reportIncompatibleMethodOverride",
            "reportUnusedClass",
        }
    ),
    "src/kokoro_agent/storage/memory_store.py": frozenset(
        {
            "reportCallIssue",
            "reportArgumentType",
            "reportReturnType",
            "reportUnknownArgumentType",
            "reportUnknownVariableType",
            "reportUnknownMemberType",
            "reportIncompatibleMethodOverride",
            "reportAbstractUsage",
            "reportAttributeAccessIssue",
            "reportUnnecessaryIsInstance",
        }
    ),
    "src/kokoro_agent/chat/store.py": frozenset(
        {
            "reportCallIssue",
            "reportArgumentType",
            "reportReturnType",
            "reportUnknownArgumentType",
            "reportUnknownVariableType",
            "reportUnknownMemberType",
            "reportIncompatibleMethodOverride",
        }
    ),
    "src/kokoro_agent/mcp/local_registry.py": frozenset(
        {"reportIncompatibleMethodOverride", "reportUnnecessaryComparison"}
    ),
}

_PRAGMA = re.compile(r"^#\s*pyright:\s*(.+)$", re.MULTILINE)
_IGNORE = re.compile(r"#\s*type:\s*ignore|#\s*pyright:\s*ignore")


def _iter_sources() -> list[Path]:
    root = Path(__file__).resolve().parents[2]
    # 只扫本仓自有源；跳过 .venv/build，以及嵌套 git worktree（.wt/*，并行 worker 的隔离副本，
    # 不是本仓源树的一部分——否则并行 worktree 的 pragma 会误入清单）。
    return [
        p
        for p in root.glob("**/*.py")
        if ".venv" not in p.parts
        and ".wt" not in p.parts
        and not p.parts[len(root.parts)] == "build"
    ]


def test_pragma_inventory_matches_allowlist() -> None:
    root = Path(__file__).resolve().parents[2]
    found: dict[str, frozenset[str]] = {}
    for path in _iter_sources():
        match = _PRAGMA.search(path.read_text(encoding="utf-8"))
        if match:
            rules = frozenset(
                part.split("=")[0].strip() for part in match.group(1).split(",")
            )
            found[path.relative_to(root).as_posix()] = rules
    assert found == ALLOWED


def test_no_inline_type_or_pyright_ignores() -> None:
    offenders = [
        path.as_posix()
        for path in _iter_sources()
        if path.name != "test_boundary_pragmas.py"
        and _IGNORE.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
