from __future__ import annotations

from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[2]
PRESENTATION = REPOSITORY / "src/kokoro_agent/presentation"


def test_presentation_package_has_the_closed_maintainable_layout() -> None:
    required = {
        "model.py",
        "planner.py",
        "delivery.py",
        "integrity.py",
        "store_baseline.py",
        "adapters/__init__.py",
        "adapters/ag_ui.py",
        "adapters/connect.py",
    }
    retired = {
        "runtime.py",
        "adapter.py",
        "profile.py",
        "provider.py",
        "submission.py",
    }

    assert all((PRESENTATION / path).is_file() for path in required)
    assert all(not (PRESENTATION / path).exists() for path in retired)


def test_presentation_boundary_has_no_duplicate_delivery_api() -> None:
    forbidden = (
        "PresentationDeliveryService",
        "PresentationAcknowledgeCommand",
        "PresentationAcknowledgeState",
        "PresentationAdmissionReceipt",
        "PresentationQuarantineCommand",
        "PresentationAdmissionReceiptStore",
        "acknowledge_presentation_admissions",
        "quarantine_presentation_admission",
        "get_presentation_delivery_state",
        "pull_delivery_records",
    )
    production = [
        *PRESENTATION.rglob("*.py"),
        REPOSITORY / "src/kokoro_agent/storage/ledger.py",
        REPOSITORY / "src/kokoro_agent/storage/mongo.py",
    ]
    matches = {
        token: str(path.relative_to(REPOSITORY))
        for path in production
        for token in forbidden
        if token in path.read_text()
    }

    assert matches == {}


def test_ag_ui_sdk_is_confined_to_the_ag_ui_adapter() -> None:
    allowed = PRESENTATION / "adapters/ag_ui.py"
    offenders = {
        str(path.relative_to(REPOSITORY))
        for path in PRESENTATION.rglob("*.py")
        if path != allowed and ("from ag_ui" in path.read_text() or "import ag_ui" in path.read_text())
    }

    assert offenders == set()
