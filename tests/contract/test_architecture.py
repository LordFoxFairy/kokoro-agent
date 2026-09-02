"""架构测试：只断依赖方向与字面量纪律，不冻结文件清单。"""

from __future__ import annotations

import ast
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src" / "kokoro_agent"


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
            if module.startswith("kokoro_agent")
            and not module.startswith("kokoro_agent.contract")
        }
        assert not offenders, (
            f"{_rel(path)} imports non-contract internals: {sorted(offenders)}"
        )


def test_config_only_imported_by_worker_main() -> None:
    for path in _py_files():
        if _rel(path) in {"config.py", "worker/main.py"}:
            continue
        offenders = {m for m in _imports(path) if m.startswith("kokoro_agent.config")}
        assert not offenders, f"{_rel(path)} must not import kokoro_agent.config"


def test_agent_core_does_not_import_worker() -> None:
    """Worker is the transport edge, never a dependency of the Agent core."""
    core_roots = (
        "agents/",
        "execution/",
        "tools/",
        "skills/",
        "mcp/",
        "sandbox/",
        "repositories/",
        "infrastructure/",
        "model/",
        "streams/",
    )
    for path in _py_files():
        if not _rel(path).startswith(core_roots):
            continue
        offenders = {
            m
            for m in _imports(path)
            if m.startswith("kokoro_agent.worker")
        }
        assert not offenders, f"{_rel(path)} must not import kokoro_agent.worker"


def test_deepagents_construction_has_one_ga_adapter() -> None:
    """GA must not grow a second Agent runtime or duplicate create_deep_agent calls."""
    allowed = {"agent_factory.py"}
    for path in _py_files():
        imports = _imports(path)
        if "deepagents" not in imports:
            continue
        assert _rel(path) in allowed, (
            f"{_rel(path)} calls/imports create_deep_agent outside agent_factory.py"
        )


def test_deepagents_first_tree_has_no_parallel_runtime_directories() -> None:
    """DeepAgents owns execution; GA must not grow a shadow framework tree."""
    forbidden = ("ga", "factory", "framework", "compiler", "runtime", "ports")
    present = [
        name
        for name in forbidden
        if (_SRC / name).is_dir() and any((_SRC / name).rglob("*.py"))
    ]
    assert not present, f"parallel runtime directories are forbidden: {present}"


def test_deterministic_model_driver_lives_in_tests_only() -> None:
    """A fake provider is a test harness concern, never a worker feature/config flag."""
    assert not (_SRC / "model" / "local_fake.py").exists()
    for path in _py_files():
        text = path.read_text(encoding="utf-8")
        assert "LocalFakeChatModel" not in text
        assert "KOKORO_LOCAL_FAKE" not in text


def test_ga_does_not_define_framework_state_or_graph_wrappers() -> None:
    """Protocols may describe upstream objects, but GA must not define replacement types."""
    forbidden_names = {
        "DeepAgentState",
        "KokoroAgentState",
        "CompiledGraph",
        "Workflow",
    }
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        defined = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert not (defined & forbidden_names), f"{_rel(path)} defines shadow types"


def test_ga_does_not_export_a_second_create_deep_agent() -> None:
    """The only constructor name belongs to the upstream DeepAgents package."""
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert "create_deep_agent" not in names, (
            f"{_rel(path)} defines a GA constructor shadow"
        )


def test_agent_declaration_does_not_carry_external_grants() -> None:
    """Capability resolution is per construction; Agent remains a names-only declaration."""
    from dataclasses import fields

    from kokoro_agent.agents.definition import Agent

    names = {field.name for field in fields(Agent)}
    assert names.isdisjoint({"resolved_skills", "resolved_mcp"})


def test_agent_core_does_not_import_product_service_packages() -> None:
    """Agent executes an opaque RunRequest; it does not couple to product-service clients."""
    forbidden = (
        "kokoro_session",
        "kokoro_web",
        "kokoro_storage",
        "kokoro_capability",
        "kokoro_chat",
        "kokoro_iam",
        "kokoro_payment",
        "kokoro_credit",
    )
    for path in _py_files():
        offenders = {m for m in _imports(path) if m.startswith(forbidden)}
        assert not offenders, (
            f"{_rel(path)} imports product-service package(s): {sorted(offenders)}"
        )


def test_worker_entrypoint_does_not_boot_owner_database_fixtures() -> None:
    """Production startup consumes injected public clients, never owner-private fixtures."""
    imports = _imports(_SRC / "worker" / "main.py")
    forbidden = {
        "kokoro_agent.mcp.local_registry",
        "kokoro_agent.skills.local_reader",
        "kokoro_agent.skills.reader",
    }
    assert not (imports & forbidden)


def test_skills_package_exports_only_runtime_integration() -> None:
    """Fixture CRUD/package adapters are not a supported GA package-level API."""
    from kokoro_agent import skills

    assert set(skills.__all__) == {"CapabilitySkillBackend", "SKILLS_ROOT"}


def test_delivery_tool_uses_only_storage_public_client() -> None:
    """Artifact publishing must never address buckets, keys, or local package stores."""
    imports = _imports(_SRC / "tools" / "deliver.py")
    forbidden = {
        "kokoro_agent.skills.local_reader",
        "kokoro_agent.sandbox.archive",
    }
    assert not (imports & forbidden)
    assert "PackageStore" not in (_SRC / "tools" / "deliver.py").read_text()


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
