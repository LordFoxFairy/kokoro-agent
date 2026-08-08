from __future__ import annotations

import tomllib
from pathlib import Path
from typing import cast

from kokoro_agent.presentation.main import build_presentation_app
from kokoro_agent.presentation.delivery import PresentationProviderStore


ROOT = Path(__file__).resolve().parents[1]


def test_console_script_and_generated_asgi_service_use_new_root_name() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)["project"]

    assert project["scripts"]["kokoro-agent-presentation"] == (
        "kokoro_agent.presentation.main:main"
    )
    app = build_presentation_app(cast(PresentationProviderStore, object()))
    assert app.path == "/kokoro.agent.presentation.v1.PresentationService"


def test_production_image_documents_presentation_process_override() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert "kokoro-agent-presentation" in dockerfile
