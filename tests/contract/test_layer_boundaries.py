"""Keep repository ports, application services, and technical adapters distinct."""

from __future__ import annotations

from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_repository_and_service_boundaries_are_explicit() -> None:
    root = _root() / "src" / "kokoro_agent"
    assert (root / "repositories" / "run_repository.py").is_file()
    assert (root / "repositories" / "chat_repository.py").is_file()
    assert (root / "services" / "chat_service.py").is_file()
    assert (root / "infrastructure" / "postgres_run_repository.py").is_file()
    assert (root / "infrastructure" / "postgres_chat_repository.py").is_file()
    assert (root / "infrastructure" / "schema.py").is_file()
    assert not (root / "persistence").exists()
    assert not (root / "chat" / "store.py").exists()
    assert not (root / "chat" / "query.py").exists()
    assert not (root / "repositories" / "schema.py").exists()


def test_ports_do_not_import_database_or_transport_adapters() -> None:
    root = _root() / "src" / "kokoro_agent"
    for name in ("run_repository.py", "chat_repository.py"):
        source = (root / "repositories" / name).read_text(encoding="utf-8")
        assert "psycopg" not in source
        assert "CREATE TABLE" not in source
        assert "connect_pg" not in source
    service = (root / "services" / "chat_service.py").read_text(encoding="utf-8")
    assert "infrastructure" not in service
    assert "http.server" not in service
