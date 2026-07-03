"""架构测试：只断依赖方向与字面量纪律，不冻结文件清单。"""

from __future__ import annotations

import ast
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src" / "kokoro_agent"


def _py_files() -> list[Path]:
    files = sorted(_SRC.rglob("*.py"))
    assert files, "src tree must not be empty"
    return files


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    return modules


def _rel(path: Path) -> str:
    return str(path.relative_to(_SRC))


def test_contract_has_zero_inward_dependencies() -> None:
    for path in _py_files():
        if not _rel(path).startswith("contract"):
            continue
        offenders = {
            module
            for module in _imports(path)
            if module.startswith("kokoro_agent") and not module.startswith("kokoro_agent.contract")
        }
        assert not offenders, f"{_rel(path)} imports non-contract internals: {sorted(offenders)}"


def test_config_only_imported_by_worker_main() -> None:
    for path in _py_files():
        if _rel(path) in {"config.py", "worker/main.py"}:
            continue
        offenders = {m for m in _imports(path) if m.startswith("kokoro_agent.config")}
        assert not offenders, f"{_rel(path)} must not import kokoro_agent.config"


def test_run_does_not_import_worker() -> None:
    for path in _py_files():
        if not _rel(path).startswith("run/"):
            continue
        offenders = {m for m in _imports(path) if m.startswith("kokoro_agent.worker")}
        assert not offenders, f"{_rel(path)} must not import kokoro_agent.worker"


def test_no_stream_name_literals_outside_contract() -> None:
    # 边界法典 §7：流名前缀字面量只许活在生成物里。
    for path in _py_files():
        if _rel(path).startswith("contract"):
            continue
        assert "kokoro:" not in path.read_text(encoding="utf-8"), (
            f"{_rel(path)} hardcodes a stream name literal"
        )


def test_environ_read_only_in_worker_main() -> None:
    # env 单点纪律：os.environ 只出现在 worker/main.py（经 AppConfig 读一次）。
    for path in _py_files():
        if _rel(path) == "worker/main.py":
            continue
        assert "os.environ" not in path.read_text(encoding="utf-8"), (
            f"{_rel(path)} reads os.environ outside worker/main.py"
        )


def test_no_type_escape_hatches() -> None:
    # 零遮掩纪律：type: ignore / cast / TYPE_CHECKING 全仓（含生成物）不得出现。
    for path in _py_files():
        text = path.read_text(encoding="utf-8")
        assert "type: ignore" not in text, f"{_rel(path)} uses type: ignore"
        assert "TYPE_CHECKING" not in text, f"{_rel(path)} uses TYPE_CHECKING"
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module is not None:
                assert not any(alias.name == "cast" for alias in node.names), (
                    f"{_rel(path)} imports cast"
                )


def test_no_function_level_imports() -> None:
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for inner in ast.walk(node):
                assert not isinstance(inner, (ast.Import, ast.ImportFrom)), (
                    f"{_rel(path)}:{node.name} contains a deferred import"
                )
