from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_every_outbound_mtls_client_uses_the_single_secure_material_loader() -> None:
    sources = (
        ROOT / "src/kokoro_agent/hub/client.py",
        ROOT / "src/kokoro_agent/model/platform_gateway.py",
        ROOT / "src/kokoro_agent/platform/media.py",
        ROOT / "src/kokoro_agent/readiness.py",
    )

    for source in sources:
        text = source.read_text()
        assert "from kokoro_agent.security import read_secure_tls_material" in text
        assert "def _tls_file" not in text
        assert "def _tls_material" not in text
        assert ".read_bytes()" not in text
