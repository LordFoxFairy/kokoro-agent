"""Check the single immutable System consumer receipt, not owner Git originals."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from typing import TypeGuard

_RECEIPT_SHA256 = "f7610568b2052a96eca4865306d240c836ce9e7263edcf46c655116e69f516eb"
_RECEIPT_KEYS = {
    "owner",
    "contract",
    "version",
    "source_commit",
    "sha256",
    "provenance",
    "source_sha256",
    "operations",
}


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate provenance JSON member")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    raise ValueError("nonfinite provenance JSON value")


def _is_object(value: object) -> TypeGuard[dict[str, object]]:
    # Called only with json.loads output: JSON object keys are strings.
    return isinstance(value, dict)


def _is_array(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def validate_system_consumer_receipt(root: Path) -> None:
    """Strict closed receipt; source and installed entrypoints use this same gate."""
    document: object = json.loads(
        (root / "contract/provenance.json").read_bytes(),
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
    )
    if not _is_object(document):
        raise ValueError("provenance must be an object")
    records = document.get("consumer_dependencies")
    if not _is_array(records):
        raise ValueError("System dependency receipt missing")
    matches: list[dict[str, object]] = []
    for record in records:
        if not _is_object(record):
            raise ValueError("invalid consumer dependency")
        if record.get("owner") == "kokoro-system":
            matches.append(record)
    if len(matches) != 1:
        raise ValueError("System dependency receipt must be unique")
    receipt = matches[0]
    provenance = receipt.get("provenance")
    sources = receipt.get("source_sha256")
    if (
        set(receipt) != _RECEIPT_KEYS
        or not _is_object(provenance)
        or set(provenance) != {"path", "sha256"}
        or not _is_object(sources)
        or len(sources) != 20
        or receipt.get("operations") != ["POST /v1/system/model-catalog/resolve"]
    ):
        raise ValueError("System dependency receipt shape differs")
    # Exact canonical equality also pins all key/value types, paths and hashes.
    # It is deliberately not evidence of owner source bytes; Root checks Git.
    try:
        # The pinned receipt contains only ASCII strings, objects and arrays.
        # Sorted compact JSON equals RFC8785 for this exact value. Any other
        # value/type differs from the fixed digest; no numeric coercion occurs.
        canonical = json.dumps(
            receipt,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (ValueError, TypeError):
        raise ValueError("System dependency receipt is not canonical") from None
    if hashlib.sha256(canonical).hexdigest() != _RECEIPT_SHA256:
        raise ValueError("System dependency receipt differs from fixed release")
