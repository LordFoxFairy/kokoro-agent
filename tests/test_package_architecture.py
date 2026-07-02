from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "src" / "kokoro_agent"


def test_agent_package_uses_execution_link_architecture() -> None:
    expected_dirs = {
        "worker",
        "run",
        "execution",
        "tools",
        "subagents",
        "skills",
        "mcp",
        "sandbox",
        "storage",
        "streams",
        "model",
    }
    forbidden_dirs = {
        "deepagents",
        "runtime",
        "adapters",
        "domain",
        "application",
        "infrastructure",
        "interfaces",
        "ports",
    }

    actual_dirs = {path.name for path in PACKAGE_ROOT.iterdir() if path.is_dir()}

    assert expected_dirs <= actual_dirs
    assert actual_dirs.isdisjoint(forbidden_dirs)


def test_agent_package_has_documented_python_files() -> None:
    expected_files = {
        "__init__.py",
        "config.py",
        "observability.py",
        "execution/approvals.py",
        "worker/main.py",
        "worker/messages.py",
        "run/request.py",
        "run/context.py",
        "run/capabilities.py",
        "run/lifecycle.py",
        "run/events.py",
        "run/json_payload.py",
        "execution/build_agent.py",
        "execution/protocols.py",
        "execution/run_agent.py",
        "execution/resume_agent.py",
        "execution/events.py",
        "execution/publish_agent_events.py",
        "execution/prompts/__init__.py",
        "tools/registry.py",
        "tools/permissions.py",
        "tools/ask_user.py",
        "tools/middleware.py",
        "tools/names.py",
        "subagents/__init__.py",
        "subagents/catalog.py",
        "subagents/definitions.py",
        "subagents/types.py",
        "skills/mounts.py",
        "mcp/servers.py",
        "mcp/tools.py",
        "sandbox/backend.py",
        "sandbox/policy.py",
        "storage/__init__.py",
        "storage/checkpoints.py",
        "storage/memory.py",
        "storage/leases.py",
        "storage/mongo_lease_store.py",
        "storage/run_state.py",
        "storage/sqlite_lease_store.py",
        "streams/__init__.py",
        "streams/factory.py",
        "streams/json_types.py",
        "streams/redis.py",
        "streams/memory.py",
        "streams/protocol.py",
        "model/__init__.py",
        "model/factory.py",
        "model/local_fake.py",
        "model/settings.py",
    }

    actual_files = {
        path.relative_to(PACKAGE_ROOT).as_posix()
        for path in PACKAGE_ROOT.rglob("*.py")
        if "__pycache__" not in path.parts
    }

    assert actual_files == expected_files
