"""第三方边界豁免政策：pragma 全量清单锁死，新增或漂移必先过本测试评审。"""

from __future__ import annotations

import re
from pathlib import Path

# 每项必须能指认上游缺口（WHY 注释在对应文件头）；扩表 = 显式评审动作。
ALLOWED: dict[str, frozenset[str]] = {
    # deepagents create_deep_agent 返回未解 ResponseT 泛型。
    "src/kokoro_agent/execution/build_agent.py": frozenset({"reportUnknownVariableType"}),
    # jsonschema 无 py.typed：kind=input 的 resume value 校验属未解上游类型（隔离于 _schema_error）。
    "src/kokoro_agent/hitl/input.py": frozenset(
        {"reportUnknownMemberType", "reportUnknownArgumentType", "reportUnknownVariableType"}
    ),
    # langchain-core BaseTool.ainvoke/tool_call_schema 未解泛型（MCP 稳定面转发调用）。
    "src/kokoro_agent/mcp/tools.py": frozenset({"reportUnknownMemberType"}),
    # langchain-core BaseTool.ainvoke 注解含裸 dict。
    "tests/e2e/test_mcp_live.py": frozenset({"reportUnknownMemberType"}),
    # 同上：技能库工具经 ainvoke 驱动断言。
    "tests/test_skill_tools.py": frozenset({"reportUnknownMemberType"}),
    # 同上：deliver 工具经 ainvoke 驱动断言。
    "tests/test_deliver_tool.py": frozenset({"reportUnknownMemberType"}),
    # 同上：MCP 稳定三工具经 ainvoke 驱动断言。
    "tests/test_mcp_tools.py": frozenset({"reportUnknownMemberType"}),
    # 同上：注册表合并结果经稳定三工具 ainvoke 驱动断言。
    "tests/test_mcp_registry.py": frozenset({"reportUnknownMemberType"}),
    # StructuredTool.from_function 未解 Runnable 泛型（ToolRuntime 注入唯一构造路径）。
    "tests/test_run_scope_state.py": frozenset({"reportUnknownMemberType"}),
}

_PRAGMA = re.compile(r"^#\s*pyright:\s*(.+)$", re.MULTILINE)
_IGNORE = re.compile(r"#\s*type:\s*ignore|#\s*pyright:\s*ignore")


def _iter_sources() -> list[Path]:
    root = Path(__file__).resolve().parents[1]
    return [
        p
        for p in root.glob("**/*.py")
        if ".venv" not in p.parts and not p.parts[len(root.parts)] == "build"
    ]


def test_pragma_inventory_matches_allowlist() -> None:
    root = Path(__file__).resolve().parents[1]
    found: dict[str, frozenset[str]] = {}
    for path in _iter_sources():
        match = _PRAGMA.search(path.read_text(encoding="utf-8"))
        if match:
            rules = frozenset(part.split("=")[0].strip() for part in match.group(1).split(","))
            found[path.relative_to(root).as_posix()] = rules
    assert found == ALLOWED


def test_no_inline_type_or_pyright_ignores() -> None:
    offenders = [
        path.as_posix()
        for path in _iter_sources()
        if path.name != "test_boundary_pragmas.py" and _IGNORE.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
