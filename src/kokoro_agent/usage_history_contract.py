"""Verify frozen usage releases without regenerating historical bytes.

The v1 compiler, decoder and generated models remain in their original paths
because its published manifest binds those paths. Only a future active v2
producer may change; this check never upgrades or repairs a historical release.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from kokoro_agent.model_usage_contract import validate_model_usage_contract

_V1_MANIFEST = "contract/usage/v1/manifest.json"
_V1_RELEASE_SHA256 = "29d15de901e60b1026ff3834249a4f5c442092b7ccef1981baaabcfaf30cbea1"


def validate_usage_history(root: Path) -> None:
    """Pin the published manifest, then verify all original sources and vectors."""
    if (
        hashlib.sha256((root / _V1_MANIFEST).read_bytes()).hexdigest()
        != _V1_RELEASE_SHA256
    ):
        raise ValueError("historical usage v1 release differs from its frozen manifest")
    validate_model_usage_contract(root)
