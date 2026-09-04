"""Keep repository ports, application services, and technical adapters distinct."""

from __future__ import annotations

from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_repository_and_service_boundaries_are_explicit() -> None:
    root = _root() / "src" / "kokoro_agent"
    assert (root / "domain" / "run" / "repository.py").is_file()
    assert (root / "domain" / "run" / "repositories.py").is_file()
    assert (root / "domain" / "chat" / "repositories.py").is_file()
    assert (root / "application" / "chat" / "service.py").is_file()
    assert (root / "application" / "chat" / "dto.py").is_file()
    assert (root / "infrastructure" / "postgres_run_repository.py").is_file()
    assert (root / "infrastructure" / "postgres_chat_repository.py").is_file()
    assert (root / "infrastructure" / "schema.py").is_file()
    assert (root / "interfaces" / "http" / "ingress.py").is_file()
    assert (root / "interfaces" / "http" / "server.py").is_file()
    assert not (root / "repositories").exists()
    assert not (root / "services").exists()
    assert not (root / "http").exists()
    assert not (root / "persistence").exists()
    assert not (root / "chat" / "store.py").exists()
    assert not (root / "chat" / "query.py").exists()


def test_postgres_run_adapter_is_split_by_repository_capability() -> None:
    """A single 2k-line adapter hides ownership and makes review unsafe."""

    root = _root() / "src" / "kokoro_agent" / "infrastructure"
    modules = (
        "postgres_run_repository.py",
        "postgres_run_admission.py",
        "postgres_run_dispatch.py",
        "postgres_run_events.py",
        "postgres_run_leases.py",
        "postgres_run_effects.py",
        "postgres_run_sandbox.py",
    )
    for name in modules:
        path = root / name
        assert path.is_file(), f"missing capability adapter: {name}"
        assert len(path.read_text(encoding="utf-8").splitlines()) <= 800


def test_supervisor_is_split_by_operational_capability() -> None:
    root = _root() / "src" / "kokoro_agent" / "worker"
    modules = (
        "supervisor.py",
        "supervisor_context.py",
        "supervisor_control.py",
        "supervisor_execution.py",
        "supervisor_recovery.py",
    )
    for name in modules:
        path = root / name
        assert path.is_file(), f"missing supervisor boundary: {name}"
        assert len(path.read_text(encoding="utf-8").splitlines()) <= 800

    façade = (root / "supervisor.py").read_text(encoding="utf-8")
    assert "class RunSupervisor" in façade
    assert "supervisor_control" in façade
    assert "supervisor_execution" in façade
    assert "supervisor_recovery" in façade
    assert "psycopg" not in façade


def test_ports_do_not_import_database_or_transport_adapters() -> None:
    root = _root() / "src" / "kokoro_agent"
    for name in ("repository.py", "repositories.py"):
        source = (root / "domain" / "run" / name).read_text(encoding="utf-8")
        assert "psycopg" not in source
        assert "CREATE TABLE" not in source
        assert "connect_pg" not in source
    chat_repository = (root / "domain" / "chat" / "repositories.py").read_text(
        encoding="utf-8"
    )
    assert "psycopg" not in chat_repository
    service = (root / "application" / "chat" / "service.py").read_text(encoding="utf-8")
    assert "infrastructure" not in service
    assert "http.server" not in service


def test_chat_service_only_contains_application_orchestration() -> None:
    root = _root() / "src" / "kokoro_agent" / "application" / "chat"
    service = (root / "service.py").read_text(encoding="utf-8")
    assert "from pydantic import" not in service
    assert "class ChatQueryRequest" not in service
    assert "class ChatService" in service
