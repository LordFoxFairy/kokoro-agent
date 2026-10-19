"""Cross-language owner schema hashes; proof integer policy is deliberately unchanged."""

from __future__ import annotations

import pytest


def test_schema_numbers_match_owner_ieee754_domain() -> None:
    from kokoro_agent.mcp.schema import schema_digest
    import hashlib

    # JSON.parse in the owner first rounds this integer to IEEE754 binary64.
    assert (
        schema_digest({"maximum": 9223372036854775807})
        == hashlib.sha256(b'{"maximum":9223372036854776000}').hexdigest()
    )
    assert (
        schema_digest({"a": -0.0, "z": [1, True, "\\u00e9"]})
        == hashlib.sha256(b'{"a":0,"z":[1,true,"\\\\u00e9"]}').hexdigest()
    )
    for bad in [float("inf"), float("nan"), 10**400]:
        with pytest.raises(ValueError):
            schema_digest({"bad": bad})
