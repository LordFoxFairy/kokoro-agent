"""Worker financial resources are explicit and cannot stall the execution heartbeat."""

from __future__ import annotations

import asyncio
import importlib
import json
from typing import Protocol

import pytest
from pydantic import SecretStr, ValidationError
from kokoro_agent.config import AppConfig
from kokoro_agent.clients.model_budget import BffModelBudgetClient, ModelBudgetError


class Secrets(Protocol):
    def __call__(self, tenant_id: str) -> SecretStr: ...


@pytest.fixture
def module():
    return importlib.import_module("kokoro_agent.worker.model_financial")


def test_budget_deployment_configuration_is_an_explicit_pair() -> None:
    for value in [
        {"KOKORO_BFF_BASE_URL": "http://bff.test"},
        {"KOKORO_BFF_MODEL_ADMISSION_SECRETS": "{}"},
    ]:
        with pytest.raises(ValidationError):
            AppConfig.from_env(value)


async def test_worker_requires_financial_client_and_owns_one_real_pool(
    module: object,
) -> None:
    make_client = getattr(module, "worker_model_billing")
    with pytest.raises(ValueError, match="BFF model billing configuration"):
        async with make_client(base_url=None, secret_snapshot=None):
            raise AssertionError("no unconfigured financial worker")
    config = AppConfig.from_env(
        {
            "KOKORO_BFF_BASE_URL": "http://bff.test",
            "KOKORO_BFF_MODEL_ADMISSION_SECRETS": json.dumps({"tenant-a": "a" * 32}),
        }
    )
    assert "a" * 32 not in repr(config)
    async with make_client(
        base_url=config.bff_base_url, secret_snapshot=config.bff_model_admission_secrets
    ) as client:
        assert isinstance(client, BffModelBudgetClient)


def test_tenant_secrets_have_no_default_or_foreign_fallback(module: object) -> None:
    make_secrets = getattr(module, "ModelAdmissionSecrets")
    secrets: Secrets = make_secrets(
        SecretStr(json.dumps({"tenant-a": "a" * 32, "tenant-b": "b" * 32}))
    )
    assert secrets("tenant-a").get_secret_value() == "a" * 32
    assert secrets("tenant-b").get_secret_value() == "b" * 32
    assert "a" * 32 not in repr(secrets)
    with pytest.raises(ModelBudgetError, match="^MODEL_BUDGET_CREDENTIAL_UNAVAILABLE$"):
        secrets("other")


@pytest.mark.parametrize(
    "raw",
    [
        "{}",
        "[]",
        '{"t":"short"}',
        '{"t":"' + "a" * 32 + '","t":"' + "b" * 32 + '"}',
        json.dumps({"t": "a" * 32, "u": "a" * 32}),
        json.dumps({" ": "a" * 32}),
        json.dumps({"t": "a\n" * 32}),
        " " * 65537,
    ],
)
def test_invalid_secret_snapshot_fails_safely_before_network(
    module: object, raw: str
) -> None:
    with pytest.raises(
        ValueError, match="^BFF model billing credentials unavailable$"
    ) as error:
        getattr(module, "ModelAdmissionSecrets")(SecretStr(raw))
    assert raw not in str(error.value) and error.value.__cause__ is None


class Queue:
    def __init__(self) -> None:
        self.rows = [("tenant-a", str(i)) for i in range(6)]
        self.calls: list[tuple[int, tuple[str, str] | None]] = []

    async def list_pending(
        self, *, limit: int, after: tuple[str, str] | None = None
    ) -> list[tuple[str, str]]:
        self.calls.append((limit, after))
        return [r for r in self.rows if after is None or r > after][:limit]


class Reconciler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.entered = asyncio.Event()
        self.block = False
        self.cancelled = False

    async def reconcile(self, tenant: str, attempt: str) -> None:
        self.calls.append((tenant, attempt))
        self.entered.set()
        if self.block:
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        if attempt == "0":
            raise ValueError("private upstream payload must not be logged")


async def test_pages_progress_past_forever_pending_or_bad_rows_then_wrap(
    module: object, caplog: pytest.LogCaptureFixture
) -> None:
    q, r = Queue(), Reconciler()
    recovery = getattr(module, "ModelFinancialRecovery")(q, r, batch_size=2)
    for _ in range(4):
        await recovery.step()
    assert r.calls == q.rows
    await recovery.step()
    assert r.calls == q.rows + q.rows[:2]
    assert [after for _, after in q.calls] == [
        None,
        q.rows[1],
        q.rows[3],
        q.rows[5],
        None,
    ]
    assert "private upstream payload" not in caplog.text


async def test_background_recovery_cancels_inflight_call_before_scope_exit(
    module: object,
) -> None:
    q, r = Queue(), Reconciler()
    r.block = True
    recovery = getattr(module, "ModelFinancialRecovery")(q, r, interval_s=0.01)
    scope = getattr(module, "active_model_financial_recovery")
    async with scope(recovery):
        await asyncio.wait_for(r.entered.wait(), 1)
        # The scope does not await blocked financial I/O before letting the Supervisor run.
        assert len(r.calls) == 1
    assert r.cancelled


async def test_failed_scan_retries_next_tick_without_terminating_background(
    module: object, caplog: pytest.LogCaptureFixture
) -> None:
    class BrokenOnce(Queue):
        async def list_pending(
            self, *, limit: int, after: tuple[str, str] | None = None
        ) -> list[tuple[str, str]]:
            if not self.calls:
                self.calls.append((limit, after))
                raise ValueError("private database connection must not be logged")
            return await super().list_pending(limit=limit, after=after)

    q, r = BrokenOnce(), Reconciler()
    recovery = getattr(module, "ModelFinancialRecovery")(q, r, interval_s=0.01)
    async with getattr(module, "active_model_financial_recovery")(recovery):
        await asyncio.wait_for(r.entered.wait(), 1)
    assert len(q.calls) >= 2 and "private database connection" not in caplog.text


def test_partial_financial_configuration_does_not_print_workload_secret() -> None:
    secret = "CANARY_SECRET"
    with pytest.raises(ValidationError) as failure:
        AppConfig.from_env({"KOKORO_BFF_MODEL_ADMISSION_SECRETS": secret})
    assert secret not in str(failure.value) and "private-workload-secret" not in str(
        failure.value
    )


@pytest.mark.parametrize(
    "options",
    [
        {"batch_size": 0},
        {"batch_size": True},
        {"interval_s": 0},
        {"interval_s": float("nan")},
    ],
)
def test_recovery_bounds_reject_unbounded_or_ambiguous_configuration(
    module: object, options: dict[str, object]
) -> None:
    with pytest.raises(ValueError, match="invalid model financial recovery bounds"):
        getattr(module, "ModelFinancialRecovery")(Queue(), Reconciler(), **options)
