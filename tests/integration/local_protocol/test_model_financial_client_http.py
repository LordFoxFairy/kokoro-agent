"""Real loopback TCP, not an owner/database or provider-execution integration."""

from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from pydantic import SecretStr
from kokoro_agent.clients.model_budget import BffModelBudgetClient
from kokoro_agent.model.budget import ModelBudgetCommand
from kokoro_agent.model.settlement import ModelSettlementCommand

ID = "11111111-1111-4111-8111-111111111111"
COMMAND = ModelBudgetCommand(
    run_id="run",
    attempt_id=ID,
    request_ref="request",
    attempt_binding_digest="a" * 64,
    planned_digest="b" * 64,
    final_request_digest="c" * 64,
)


@pytest.mark.parametrize(
    "operation",
    ["reserve", "recover", "miss", "captured", "released", "pending_reconciliation"],
)
async def test_formal_financial_client_over_actual_http_with_exact_body_and_ack(
    operation: str,
) -> None:
    seen: list[tuple[str, dict[str, str], bytes]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_POST(self) -> None:
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            seen.append((self.path, dict(self.headers), raw))
            if operation == "miss":
                status = 404
                payload = {
                    "error": {
                        "code": "model_admission_not_found",
                        "retryable": False,
                        "message": "not found",
                    }
                }
            elif operation in {"reserve", "recover"}:
                status = 201 if operation == "reserve" else 200
                payload = {
                    "data": {
                        **asdict(COMMAND),
                        "admission_id": ID,
                        "hold_id": ID,
                        "budget_credits": "10",
                        "pricing_snapshot_digest": "d" * 64,
                        "status": "held",
                        "created_at": "2026-10-23T00:00:00.000Z",
                        "updated_at": "2026-10-23T00:00:00.000Z",
                    }
                }
            else:
                status = 200
                payload = {
                    "data": dict(
                        admission_id=ID,
                        run_id="run",
                        attempt_id=ID,
                        evidence_event_id=ID,
                        evidence_revision="1",
                        evidence_digest="e" * 64,
                        status=operation,
                        reason="budget_exceeded"
                        if operation == "pending_reconciliation"
                        else None,
                        captured_credits="1" if operation == "captured" else "0",
                        released_credits="9"
                        if operation == "captured"
                        else "10"
                        if operation == "released"
                        else "0",
                        usage_event_id=ID if operation == "captured" else None,
                        settlement_id=ID if operation == "captured" else None,
                        updated_at="2026-10-23T00:00:00.000Z",
                    )
                }
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("cache-control", "no-store")
            self.send_header("x-request-id", "tcp-finance")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        async with BffModelBudgetClient(
            f"http://127.0.0.1:{server.server_port}", lambda _: SecretStr("s" * 32)
        ) as c:
            common = dict(
                tenant_id="tenant", key="original-key", request_id="tcp-finance"
            )
            if operation == "reserve":
                result = await c.reserve(
                    **common, command=COMMAND, proof=SecretStr("p" * 32)
                )
                assert result.admission_ref == ID
            elif operation in {"recover", "miss"}:
                result = await c.recover(**common, command=COMMAND)
                if operation == "miss":
                    assert result is None
                else:
                    assert result is not None and result.admission_ref == ID
            else:
                settled = await c.settle(
                    **common,
                    admission_ref=ID,
                    command=ModelSettlementCommand(
                        run_id="run",
                        attempt_id=ID,
                        event_id=ID,
                        revision="1",
                        digest="e" * 64,
                    ),
                )
                assert settled.status == operation and settled.admission_ref == ID
        assert len(seen) == 1
        path, h, raw = seen[0]
        h = {k.lower(): v for k, v in h.items()}
        assert (
            h["authorization"] == "Bearer " + "s" * 32
            and h["idempotency-key"] == "original-key"
        )
        assert "cookie" not in h and "x-billing-consumption-token" not in h
        assert ("x-billing-execution-proof" in h) is (operation == "reserve")
        if operation in {"reserve", "recover", "miss"}:
            assert json.loads(raw) == asdict(COMMAND)
            assert path == "/internal/v1/model-admissions" + (
                "" if operation == "reserve" else "/recover"
            )
        else:
            assert path == f"/internal/v1/model-admissions/{ID}/settle"
            assert json.loads(raw) == dict(
                run_id="run",
                evidence_event_id=ID,
                evidence_revision="1",
                evidence_digest="e" * 64,
            )
    finally:
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        await asyncio.to_thread(thread.join, 5)
        assert not thread.is_alive()
