"""Owner discovery schema digest: finite ECMAScript Number domain, UTF-16 key order."""

from __future__ import annotations

import hashlib
import math
import rfc8785
from pydantic import JsonValue


def _numbers(value: JsonValue, depth: int = 0) -> JsonValue:
    if depth > 64:
        raise ValueError("MCP_SCHEMA_INVALID")
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        try:
            number = float(value)
        except OverflowError:
            raise ValueError("MCP_SCHEMA_INVALID") from None
        if not math.isfinite(number):
            raise ValueError("MCP_SCHEMA_INVALID")
        return number
    if isinstance(value, list):
        return [_numbers(item, depth + 1) for item in value]
    return {key: _numbers(item, depth + 1) for key, item in value.items()}


def schema_digest(schema: dict[str, JsonValue]) -> str:
    # Do not use the execution-proof encoder: its safe-integer domain is stricter.
    payload = rfc8785.dumps(_numbers(schema))
    if len(payload) > 1_048_576:
        raise ValueError("MCP_SCHEMA_INVALID")
    return hashlib.sha256(payload).hexdigest()
