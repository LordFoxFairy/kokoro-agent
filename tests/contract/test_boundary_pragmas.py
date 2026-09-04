"""第三方类型边界政策：生产代码严格检查，测试探针的豁免可审查且可追踪。"""

from __future__ import annotations

import re
from pathlib import Path

# 这些文件直接探测上游图/工具的未参数化运行时 API，豁免只允许存在于测试支持代码。
# 生产 src/ 不接受 file-wide pragma；第三方不完整类型必须在窄 adapter 内收敛。
ALLOWED_TEST_PRAGMAS: dict[str, frozenset[str]] = {
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
    "tests/contract/test_official_swarm.py": frozenset(
        {
            "reportUnknownMemberType",
            "reportUnknownParameterType",
            "reportUnknownVariableType",
        }
    ),
    "tests/e2e/test_mcp_live.py": frozenset({"reportUnknownMemberType"}),
    "tests/support/deepagents.py": frozenset(
        {"reportUnknownArgumentType", "reportUnknownVariableType"}
    ),
    "tests/unit/mcp/test_mcp_tools.py": frozenset({"reportUnknownMemberType"}),
    "tests/unit/tools/test_deliver.py": frozenset({"reportUnknownMemberType"}),
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


def _pragma_inventory() -> dict[str, frozenset[str]]:
    root = Path(__file__).resolve().parents[2]
    found: dict[str, frozenset[str]] = {}
    for path in _iter_sources():
        match = _PRAGMA.search(path.read_text(encoding="utf-8"))
        if match:
            rules = frozenset(
                part.split("=")[0].strip() for part in match.group(1).split(",")
            )
            found[path.relative_to(root).as_posix()] = rules
    return found


def test_production_sources_have_no_file_wide_type_pragma() -> None:
    offenders = [path for path in _pragma_inventory() if path.startswith("src/")]
    assert offenders == []


def test_test_pragma_inventory_matches_allowlist() -> None:
    found = {
        path: rules
        for path, rules in _pragma_inventory().items()
        if path.startswith("tests/")
    }
    assert found == ALLOWED_TEST_PRAGMAS


def test_no_inline_type_or_pyright_ignores() -> None:
    offenders = [
        path.as_posix()
        for path in _iter_sources()
        if path.name != "test_boundary_pragmas.py"
        and _IGNORE.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
