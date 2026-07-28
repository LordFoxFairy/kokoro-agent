#!/usr/bin/env python3
"""Live consumer probe for the Hub runtime secret resolver."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TextIO

from pydantic import SecretStr

from kokoro_agent.mcp.secret_client import HubSecretResolver, HubSecretSettings

_CALLER_SECRET_ENV = "KOKORO_INTERNAL_SECRET_AGENT"
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class ProbeConfig:
    base_url: str
    namespace: str
    handle: str
    expected_sha256: str


def _non_empty(value: str) -> str:
    if not value:
        raise argparse.ArgumentTypeError("value must be non-empty")
    return value


def _sha256(value: str) -> str:
    if _SHA256_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("expected a lowercase SHA-256 digest")
    return value


def parse_arguments(argv: Sequence[str] | None = None) -> ProbeConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, type=_non_empty)
    parser.add_argument("--namespace", required=True, type=_non_empty)
    parser.add_argument("--handle", required=True, type=_non_empty)
    parser.add_argument("--expected-sha256", required=True, type=_sha256)
    parsed = parser.parse_args(argv)
    return ProbeConfig(
        base_url=parsed.base_url,
        namespace=parsed.namespace,
        handle=parsed.handle,
        expected_sha256=parsed.expected_sha256,
    )


async def run_probe(config: ProbeConfig, environ: Mapping[str, str]) -> dict[str, int]:
    caller_secret = environ.get(_CALLER_SECRET_ENV)
    if not caller_secret:
        raise RuntimeError("hub_runtime_consumer_config_invalid")

    resolver = HubSecretResolver(
        HubSecretSettings(
            base_url=config.base_url,
            service_secret=SecretStr(caller_secret),
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
    environ: Mapping[str, str] | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    config = parse_arguments(argv)
    try:
        result = asyncio.run(run_probe(config, os.environ if environ is None else environ))
    except Exception:  # CLI trust boundary: never expose response bodies or resolved values.
        stderr.write("hub_runtime_consumer_failed\n")
        return 1
    stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
