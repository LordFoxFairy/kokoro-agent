#!/usr/bin/env python3
"""Official live Agent probe for the Agent-only Hub mTLS ConnectRPC boundary."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TextIO

from kokoro_agent.mcp.secret_client import HubSecretResolver, HubSecretSettings

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class ProbeConfig:
    rpc_url: str
    server_name: str
    ca_file: str
    cert_file: str
    key_file: str
    namespace: str
    handle: str
    expected_sha256: str
    timeout_ms: int = 5_000


def _non_empty(value: str) -> str:
    if not value:
        raise argparse.ArgumentTypeError("value must be non-empty")
    return value


def _sha256(value: str) -> str:
    if _SHA256_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("expected a lowercase SHA-256 digest")
    return value


def _timeout(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timeout must be an integer") from error
    if not 100 <= parsed <= 5_000:
        raise argparse.ArgumentTypeError("timeout must be between 100 and 5000 milliseconds")
    return parsed


def parse_arguments(argv: Sequence[str] | None = None) -> ProbeConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-url", required=True, type=_non_empty)
    parser.add_argument("--server-name", required=True, type=_non_empty)
    parser.add_argument("--ca-file", required=True, type=_non_empty)
    parser.add_argument("--cert-file", required=True, type=_non_empty)
    parser.add_argument("--key-file", required=True, type=_non_empty)
    parser.add_argument("--namespace", required=True, type=_non_empty)
    parser.add_argument("--handle", required=True, type=_non_empty)
    parser.add_argument("--expected-sha256", required=True, type=_sha256)
    parser.add_argument("--timeout-ms", default=5_000, type=_timeout)
    parsed = parser.parse_args(argv)
    return ProbeConfig(
        rpc_url=parsed.rpc_url,
        server_name=parsed.server_name,
        ca_file=parsed.ca_file,
        cert_file=parsed.cert_file,
        key_file=parsed.key_file,
        namespace=parsed.namespace,
        handle=parsed.handle,
        expected_sha256=parsed.expected_sha256,
        timeout_ms=parsed.timeout_ms,
    )


async def run_probe(config: ProbeConfig) -> dict[str, int]:
    resolver = HubSecretResolver(
        HubSecretSettings(
            rpc_url=config.rpc_url,
            server_name=config.server_name,
            ca_file=config.ca_file,
            cert_file=config.cert_file,
            key_file=config.key_file,
            timeout_ms=config.timeout_ms,
        )
    )
    resolved = await resolver.resolve(config.namespace, [config.handle])
    if set(resolved) != {config.handle}:
        raise RuntimeError("hub_runtime_consumer_result_invalid")

    actual_sha256 = hashlib.sha256(resolved[config.handle].encode()).hexdigest()
    if not hmac.compare_digest(actual_sha256, config.expected_sha256):
        raise RuntimeError("hub_runtime_consumer_digest_mismatch")
    return {"schemaVersion": 1, "resolvedHandles": 1}


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    config = parse_arguments(argv)
    try:
        result = asyncio.run(run_probe(config))
    except Exception:  # CLI trust boundary: never expose response bodies or resolved values.
        stderr.write("hub_runtime_consumer_failed\n")
        return 1
    stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
