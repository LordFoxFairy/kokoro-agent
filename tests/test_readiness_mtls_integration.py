# Hypercorn's public serve annotation includes an unparameterized WSGI fallback.
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import socket
import uuid

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier
from hypercorn.asyncio import serve as serve_asgi
import pytest
from pymongo import AsyncMongoClient

from kokoro.agent.execution.v1.agent_execution_evidence_connect import (
    AgentExecutionEvidenceServiceASGIApplication,
)
from kokoro_agent.evidence.server import EvidenceServerSettings, build_hypercorn_config
from kokoro_agent.evidence.service import AgentExecutionEvidenceConnectService
from kokoro_agent.presentation.main import build_presentation_app
from kokoro_agent.presentation.server import (
    PresentationServerSettings,
    build_hypercorn_config as build_presentation_hypercorn_config,
)
from kokoro_agent.readiness import (
    MtlsRpcReadinessSettings,
    check_evidence_listener,
    check_presentation_listener,
)
from kokoro_agent.storage.ledger import LedgerSettings, make_ledger


MONGO_URL = os.environ.get(
    "KOKORO_MONGO_URL",
    "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true",
)


def _available_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _write_pki(root: Path) -> tuple[Path, Path, Path, Path, Path]:
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Kokoro test CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                content_commitment=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    def leaf(name: str, usage: ObjectIdentifier) -> tuple[Path, Path]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    key_cert_sign=False,
                    crl_sign=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    content_commitment=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                critical=False,
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
                critical=False,
            )
        )
        if usage == ExtendedKeyUsageOID.SERVER_AUTH:
            builder = builder.add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
            )
        certificate = builder.sign(ca_key, hashes.SHA256())
        cert_path = root / f"{name}.pem"
        key_path = root / f"{name}.key"
        cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        key_path.chmod(0o600)
        return cert_path, key_path

    ca_path = root / "ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    server_cert, server_key = leaf("server", ExtendedKeyUsageOID.SERVER_AUTH)
    client_cert, client_key = leaf("client", ExtendedKeyUsageOID.CLIENT_AUTH)
    return ca_path, server_cert, server_key, client_cert, client_key


async def _eventually_ready(check: Callable[[], Awaitable[None]]) -> None:
    last: Exception | None = None
    for _attempt in range(40):
        try:
            await check()
            return
        except Exception as error:  # noqa: BLE001 - startup convergence assertion
            last = error
            await asyncio.sleep(0.025)
    raise AssertionError("mTLS readiness RPC did not recover") from last


@pytest.mark.parametrize("role", ["evidence", "presentation"])
async def test_provider_readiness_tracks_real_mtls_listener_down_up(
    role: str, tmp_path: Path
) -> None:
    ca, server_cert, server_key, client_cert, client_key = _write_pki(tmp_path)
    port = _available_port()
    database = f"kokoro_readiness_mtls_{uuid.uuid4().hex}"
    client_settings = MtlsRpcReadinessSettings(
        url=f"https://localhost:{port}",
        ca_file=str(ca),
        cert_file=str(client_cert),
        key_file=str(client_key),
        timeout_ms=100,
    )
    async def check() -> None:
        if role == "evidence":
            await check_evidence_listener(client_settings)
        else:
            await check_presentation_listener(client_settings)
    with pytest.raises(Exception):
        await check()

    stop = asyncio.Event()
    mongo: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    try:
        async with make_ledger(
            LedgerSettings(
                mongo_url=MONGO_URL,
                mongo_db=database,
                lease_ttl_ms=90_000,
            )
        ) as store:
            if role == "evidence":
                app = AgentExecutionEvidenceServiceASGIApplication(
                    AgentExecutionEvidenceConnectService(store),
                    read_max_bytes=128 * 1024,
                )
                server = build_hypercorn_config(
                    EvidenceServerSettings.from_values(
                        host="127.0.0.1",
                        port=port,
                        tls_cert=str(server_cert),
                        tls_key=str(server_key),
                        caller_ca_bundle=str(ca),
                        allowed_callers="kokoro-session,kokoro-platform",
                    )
                )
            else:
                app = build_presentation_app(store)
                server = build_presentation_hypercorn_config(
                    PresentationServerSettings.from_values(
                        host="127.0.0.1",
                        port=port,
                        tls_cert=str(server_cert),
                        tls_key=str(server_key),
                        caller_ca_bundle=str(ca),
                        allowed_callers="kokoro-session",
                    )
                )
            task = asyncio.create_task(
                serve_asgi(app, server, shutdown_trigger=stop.wait)
            )
            try:
                await _eventually_ready(check)
            finally:
                stop.set()
                await asyncio.wait_for(task, timeout=2)

        with pytest.raises(Exception):
            await check()
    finally:
        await mongo.drop_database(database)
        await mongo.close()
