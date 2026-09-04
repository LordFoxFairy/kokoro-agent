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
    service = (root / "application" / "chat" / "service.py").read_text(
        encoding="utf-8"
    )
    assert "infrastructure" not in service
    assert "http.server" not in service


def test_chat_service_only_contains_application_orchestration() -> None:
    root = _root() / "src" / "kokoro_agent" / "application" / "chat"
    service = (root / "service.py").read_text(encoding="utf-8")
    assert "from pydantic import" not in service
    assert "class ChatQueryRequest" not in service
    assert "class ChatService" in service
