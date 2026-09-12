"""Unit tests for the exact execution-proof profile and Ed25519 signer."""

from __future__ import annotations

import base64
from dataclasses import FrozenInstanceError
from typing import Any, SupportsIndex
import unicodedata

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.ed448 import Ed448PrivateKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
import jwt
import pytest

import kokoro_agent.execution.execution_proof_signer as signer_module
from kokoro_agent.execution.execution_proof_profile import (
    MAX_SAFE_INTEGER,
    ExecutionProofInput,
    ExecutionProofProfileError,
)
from kokoro_agent.execution.execution_proof_signer import (
    ExecutionProofSigner,
    ExecutionProofSignerConfig,
    ExecutionProofSigningError,
)
from kokoro_agent.protocol import IdentityRef


RFC8032_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
SECOND_RFC8032_SEED = bytes.fromhex(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb"
)
SECOND_RFC8032_SIGNATURE = bytes.fromhex(
    "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da"
    "085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"
)


def _input(**changes: object) -> ExecutionProofInput:
    values: dict[str, Any] = {
        "tenant_ref": "tenant/TENANT",
        "actor": IdentityRef(kind="user", opaque_ref="usr_A7f9"),
        "subject": IdentityRef(kind="project", opaque_ref="project/cafe\u0301"),
        "run_id": "run_01JTEST000000000000000001",
        "execution_session_id": "session_01JTEST00000000000001",
        "lease_generation": MAX_SAFE_INTEGER,
        "operation": "skills.resolve",
        "request_binding_sha256": (
            "691422ac1ca52c03760fd683b58178423dcfe0b8158224d1d63bb4d2224c5470"
        ),
        "iat": 1_789_236_000,
        "exp": 1_789_236_060,
        "jti": "AAECAwQFBgcICQoLDA0ODw",
    }
    values.update(changes)
    return ExecutionProofInput(**values)


def _signer(
    *,
    private_key: Any = None,
    issuer: str = "https://agent.internal.kokoro.dev",
) -> ExecutionProofSigner:
    key = private_key or Ed25519PrivateKey.from_private_bytes(RFC8032_SEED)
    return ExecutionProofSigner(
        ExecutionProofSignerConfig(
            issuer=issuer,
            kid="agent-proof-ed25519-2026-09-a",
            private_key=key,
        )
    )


def _assert_sanitized_exception(error: BaseException, sentinel: str) -> None:
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        assert sentinel not in str(current)
        assert sentinel not in repr(current)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    assert error.__cause__ is None
    assert error.__context__ is None


def _decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _mutate_segment(compact: str, index: int, mutation: str) -> str:
    segments = compact.split(".")
    raw = bytearray(_decode(segments[index]))
    raw[0] ^= 1
    segments[index] = _encode(bytes(raw))
    if mutation == "short-signature":
        segments[index] = _encode(bytes(raw[:-1]))
    elif mutation == "long-signature":
        segments[index] = _encode(bytes(raw + b"x"))
    return ".".join(segments)


class _TextSubclass(str):
    pass


class _ExtendedIdentityRef(IdentityRef):
    extra: str


class _MaliciousCompact(str):
    encode_called = False
    split_called = False

    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        type(self).encode_called = True
        return super().encode(encoding, errors)

    def split(self, sep: str | None = None, maxsplit: SupportsIndex = -1) -> list[str]:
        type(self).split_called = True
        return super().split(sep, maxsplit)


def test_signer_config_is_immutable_and_rejects_wrong_curve() -> None:
    config = ExecutionProofSignerConfig(
        issuer="https://agent.internal.kokoro.dev",
        kid="key-a",
        private_key=Ed25519PrivateKey.from_private_bytes(RFC8032_SEED),
    )

    with pytest.raises(FrozenInstanceError):
        setattr(config, "kid", "changed")
    with pytest.raises(ExecutionProofSigningError, match="configuration"):
        _signer(private_key=X25519PrivateKey.generate())
    with pytest.raises(ExecutionProofSigningError, match="configuration"):
        _signer(private_key=Ed448PrivateKey.generate())


@pytest.mark.parametrize("field", ["issuer", "kid"])
def test_signer_config_rejects_nonexact_strings(field: str) -> None:
    values: dict[str, Any] = {
        "issuer": "https://agent.internal.kokoro.dev",
        "kid": "key-a",
        "private_key": Ed25519PrivateKey.from_private_bytes(RFC8032_SEED),
    }
    values[field] = _TextSubclass(values[field])

    with pytest.raises(ExecutionProofSigningError, match="configuration"):
        ExecutionProofSignerConfig(**values)


def test_signer_rejects_nonexact_config_at_runtime_boundary() -> None:
    config = ExecutionProofSignerConfig(
        issuer="https://agent.internal.kokoro.dev",
        kid="key-a",
        private_key=Ed25519PrivateKey.from_private_bytes(RFC8032_SEED),
    )

    class ConfigSubclass(ExecutionProofSignerConfig):
        pass

    subclass = ConfigSubclass(
        issuer=config.issuer,
        kid=config.kid,
        private_key=config.private_key,
    )
    duck: Any = type(
        "DuckConfig",
        (),
        {"issuer": config.issuer, "kid": config.kid, "private_key": config.private_key},
    )()

    with pytest.raises(ExecutionProofSigningError, match="configuration"):
        ExecutionProofSigner(subclass)
    with pytest.raises(ExecutionProofSigningError, match="configuration"):
        ExecutionProofSigner(duck)


def test_signer_copies_config_snapshot_at_construction() -> None:
    config = ExecutionProofSignerConfig(
        issuer="https://agent.internal.kokoro.dev",
        kid="agent-proof-ed25519-2026-09-a",
        private_key=Ed25519PrivateKey.from_private_bytes(RFC8032_SEED),
    )
    signer = ExecutionProofSigner(config)
    expected = signer.issue_execution_proof(_input())

    object.__setattr__(config, "issuer", "mutated-issuer")
    object.__setattr__(config, "kid", "mutated-key")
    object.__setattr__(config, "private_key", Ed25519PrivateKey.generate())

    assert signer.issue_execution_proof(_input()) == expected


def test_signer_revalidates_its_config_snapshot_before_encode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signer = _signer()
    config = object.__getattribute__(signer, "_config")
    object.__setattr__(config, "kid", _TextSubclass("key-a"))
    encode_calls = 0

    def unexpected_encode(*args: Any, **kwargs: Any) -> str:
        nonlocal encode_calls
        encode_calls += 1
        return "unexpected"

    monkeypatch.setattr(jwt, "encode", unexpected_encode)

    with pytest.raises(ExecutionProofSigningError, match="signing failed"):
        signer.issue_execution_proof(_input())

    assert encode_calls == 0


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("tenant_ref", _TextSubclass("tenant/TENANT")),
        ("run_id", _TextSubclass("run-id")),
        ("execution_session_id", _TextSubclass("session-id")),
        ("operation", _TextSubclass("skills.resolve")),
        ("request_binding_sha256", _TextSubclass("0" * 64)),
        ("jti", _TextSubclass("AAECAwQFBgcICQoLDA0ODw")),
        ("actor", IdentityRef.model_construct(kind="admin", opaque_ref="actor")),
        (
            "actor",
            IdentityRef.model_construct(kind=_TextSubclass("user"), opaque_ref="actor"),
        ),
        (
            "subject",
            IdentityRef.model_construct(
                kind="user", opaque_ref=_TextSubclass("subject")
            ),
        ),
        ("subject", IdentityRef.model_construct(kind="user", opaque_ref="")),
        (
            "actor",
            _ExtendedIdentityRef(
                kind="user", opaque_ref="actor", extra="must-not-project"
            ),
        ),
        (
            "actor",
            IdentityRef(kind="user", opaque_ref="actor").model_copy(
                update={"kind": "admin"}
            ),
        ),
    ],
)
def test_signer_rejects_mutated_or_invalid_input_before_encode(
    monkeypatch: pytest.MonkeyPatch, field: str, invalid_value: object
) -> None:
    issue = _input()
    object.__setattr__(issue, field, invalid_value)
    encode_calls = 0

    def unexpected_encode(*args: Any, **kwargs: Any) -> str:
        nonlocal encode_calls
        encode_calls += 1
        return "unexpected"

    monkeypatch.setattr(jwt, "encode", unexpected_encode)

    with pytest.raises(ExecutionProofSigningError, match="signing failed") as error:
        _signer().issue_execution_proof(issue)

    assert encode_calls == 0
    assert "must-not-project" not in str(error.value)


def test_signer_rejects_duck_and_subclass_input_before_encode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid = _input()

    class InputSubclass(ExecutionProofInput):
        pass

    subclass = InputSubclass(
        **{name: getattr(valid, name) for name in valid.__dataclass_fields__}
    )
    duck: Any = type(
        "DuckInput",
        (),
        {name: getattr(valid, name) for name in valid.__dataclass_fields__},
    )()
    encode_calls = 0

    def unexpected_encode(*args: Any, **kwargs: Any) -> str:
        nonlocal encode_calls
        encode_calls += 1
        return "unexpected"

    monkeypatch.setattr(jwt, "encode", unexpected_encode)

    candidates: tuple[Any, ...] = (subclass, duck)
    for issue in candidates:
        with pytest.raises(ExecutionProofSigningError, match="signing failed"):
            _signer().issue_execution_proof(issue)

    assert encode_calls == 0


def test_signer_revalidates_mutated_frozen_input_before_encode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _input()
    object.__setattr__(issue, "operation", "Invalid.operation")
    encode_calls = 0

    def unexpected_encode(*args: Any, **kwargs: Any) -> str:
        nonlocal encode_calls
        encode_calls += 1
        return "unexpected"

    monkeypatch.setattr(jwt, "encode", unexpected_encode)

    with pytest.raises(ExecutionProofSigningError, match="signing failed"):
        _signer().issue_execution_proof(issue)

    assert encode_calls == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("lease_generation", True),
        ("lease_generation", 1.0),
        ("lease_generation", 0),
        ("lease_generation", MAX_SAFE_INTEGER + 1),
        ("iat", False),
        ("iat", 1.0),
        ("iat", -1),
        ("iat", MAX_SAFE_INTEGER + 1),
        ("exp", True),
        ("exp", 1.0),
        ("exp", -1),
        ("exp", MAX_SAFE_INTEGER + 1),
    ],
)
def test_profile_rejects_non_integer_or_unsafe_numeric_claims(
    field: str, value: object
) -> None:
    with pytest.raises(ExecutionProofProfileError, match="numeric claim"):
        _input(**{field: value})


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"exp": 1_789_236_000}, "time claims"),
        ({"exp": 1_789_236_061}, "time claims"),
        ({"operation": ""}, "operation"),
        ({"operation": "Skills.resolve"}, "operation"),
        ({"operation": "skills-resolve"}, "operation"),
        ({"operation": "s" * 129}, "operation"),
        ({"request_binding_sha256": "A" * 64}, "request binding"),
        ({"request_binding_sha256": "0" * 63}, "request binding"),
        ({"request_binding_sha256": "z" * 64}, "request binding"),
        ({"jti": "AAECAwQFBgcICQoLDA0ODw="}, "jti"),
        ({"jti": "AAECAwQFBgcICQoLDA0ODE"}, "jti"),
        ({"jti": "short"}, "jti"),
    ],
)
def test_profile_rejects_invalid_pair_operation_binding_and_jti(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ExecutionProofProfileError, match=message):
        _input(**changes)


def test_callers_cannot_override_owner_controlled_fields() -> None:
    fields: dict[str, object] = {
        "issuer": "other-issuer",
        "aud": "other-audience",
        "contract_version": "9.9.9",
        "typ": "JWT",
        "alg": "HS256",
        "kid": "other-key",
    }

    for name, value in fields.items():
        with pytest.raises(TypeError):
            _input(**{name: value})


def test_signer_preserves_unicode_without_normalization() -> None:
    decomposed = "project/cafe\u0301"
    composed = unicodedata.normalize("NFC", decomposed)

    decomposed_proof = _signer().issue_execution_proof(
        _input(subject=IdentityRef(kind="project", opaque_ref=decomposed))
    )
    composed_proof = _signer().issue_execution_proof(
        _input(subject=IdentityRef(kind="project", opaque_ref=composed))
    )

    assert decomposed != composed
    assert decomposed_proof.split(".")[1] != composed_proof.split(".")[1]
    assert decomposed.encode() in _decode(decomposed_proof.split(".")[1])


def test_signer_rejects_compact_proof_over_16_kib() -> None:
    huge = "x" * 13_000
    issue = _input(actor=IdentityRef(kind="user", opaque_ref=huge))

    with pytest.raises(ExecutionProofSigningError, match="signing failed") as error:
        _signer().issue_execution_proof(issue)

    assert huge not in str(error.value)


def test_signer_accepts_exact_16_kib_and_rejects_16_385_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_encode = jwt.encode
    emitted_sizes: list[int] = []

    def recording_encode(*args: Any, **kwargs: Any) -> str:
        compact = original_encode(*args, **kwargs)
        emitted_sizes.append(len(compact.encode("ascii")))
        return compact

    monkeypatch.setattr(jwt, "encode", recording_encode)
    exact = _input(actor=IdentityRef(kind="user", opaque_ref='"' * 5_776))
    over = _input(actor=IdentityRef(kind="user", opaque_ref='"' * 5_776 + "x"))

    compact = _signer().issue_execution_proof(exact)
    assert len(compact.encode("ascii")) == 16_384
    with pytest.raises(ExecutionProofSigningError, match="signing failed"):
        _signer().issue_execution_proof(over)
    assert emitted_sizes == [16_384, 16_385]


@pytest.mark.parametrize("failure_path", ["jcs", "jose", "crypto", "normalized"])
def test_signer_discards_sensitive_exception_context(
    monkeypatch: pytest.MonkeyPatch, failure_path: str
) -> None:
    sentinel = f"SECRET_SENTINEL_{failure_path}"

    def fail(*args: Any, **kwargs: Any) -> Any:
        if failure_path == "normalized":
            raise ExecutionProofSigningError(sentinel)
        raise RuntimeError(sentinel)

    if failure_path == "jcs":
        monkeypatch.setattr(signer_module, "canonical_json_bytes", fail)
    elif failure_path in {"jose", "normalized"}:
        monkeypatch.setattr(jwt, "encode", fail)
    else:
        monkeypatch.setattr(ExecutionProofSigner, "_verify_signed_output", fail)

    with pytest.raises(ExecutionProofSigningError) as captured:
        _signer().issue_execution_proof(_input())

    assert type(captured.value) is ExecutionProofSigningError
    _assert_sanitized_exception(captured.value, sentinel)


def test_signer_rejects_str_subclass_before_using_overridden_methods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_encode = jwt.encode
    compact = original_encode(
        {"placeholder": "the signer must reject this output before inspecting it"},
        key=Ed25519PrivateKey.from_private_bytes(RFC8032_SEED),
        algorithm="EdDSA",
    )
    malicious = _MaliciousCompact(compact)
    _MaliciousCompact.encode_called = False
    _MaliciousCompact.split_called = False

    def malicious_encode(*args: Any, **kwargs: Any) -> str:
        return malicious

    monkeypatch.setattr(jwt, "encode", malicious_encode)

    with pytest.raises(ExecutionProofSigningError, match="signing failed"):
        _signer().issue_execution_proof(_input())

    assert not _MaliciousCompact.encode_called
    assert not _MaliciousCompact.split_called


@pytest.mark.parametrize(
    "mutation",
    [
        "header-bit",
        "payload-bit",
        "signature-bit",
        "short-signature",
        "long-signature",
        "wrong-key",
        "wrong-alg",
        "two-segments",
        "four-segments",
        "empty-segment",
        "padded-header",
    ],
)
def test_post_sign_checks_reject_malformed_or_tampered_output(
    monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    original_encode = jwt.encode
    wrong_key = Ed25519PrivateKey.generate()

    def corrupted_encode(*args: Any, **kwargs: Any) -> str:
        compact = original_encode(*args, **kwargs)
        if mutation == "header-bit":
            return _mutate_segment(compact, 0, mutation)
        if mutation == "payload-bit":
            return _mutate_segment(compact, 1, mutation)
        if mutation in {"signature-bit", "short-signature", "long-signature"}:
            return _mutate_segment(compact, 2, mutation)
        if mutation == "wrong-key":
            kwargs["key"] = wrong_key
            return original_encode(*args, **kwargs)
        if mutation == "wrong-alg":
            segments = compact.split(".")
            segments[0] = _encode(_decode(segments[0]).replace(b'"EdDSA"', b'"HS256"'))
            return ".".join(segments)
        if mutation == "two-segments":
            return ".".join(compact.split(".")[:2])
        if mutation == "four-segments":
            return compact + ".extra"
        if mutation == "empty-segment":
            return compact.replace(".", "..", 1)
        return compact.split(".")[0] + "=." + ".".join(compact.split(".")[1:])

    monkeypatch.setattr(jwt, "encode", corrupted_encode)

    with pytest.raises(ExecutionProofSigningError, match="signing failed"):
        _signer().issue_execution_proof(_input())


@pytest.mark.parametrize("segment_index", [0, 1, 2])
def test_post_sign_checks_reject_padding_in_each_segment(
    monkeypatch: pytest.MonkeyPatch, segment_index: int
) -> None:
    original_encode = jwt.encode

    def padded_encode(*args: Any, **kwargs: Any) -> str:
        segments = original_encode(*args, **kwargs).split(".")
        segments[segment_index] += "="
        if segment_index in {0, 1}:
            signing_input = f"{segments[0]}.{segments[1]}".encode("ascii")
            segments[2] = _encode(kwargs["key"].sign(signing_input))
        return ".".join(segments)

    monkeypatch.setattr(jwt, "encode", padded_encode)

    with pytest.raises(ExecutionProofSigningError, match="signing failed"):
        _signer().issue_execution_proof(_input())


_BASE64URL_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def _same_bytes_trailing_bit_alias(segment: str) -> str:
    decoded = _decode(segment)
    for candidate in _BASE64URL_ALPHABET:
        alias = segment[:-1] + candidate
        if alias != segment and _decode(alias) == decoded:
            return alias
    raise AssertionError("fixture segment has no trailing-bit alias")


@pytest.mark.parametrize("segment_index", [0, 1, 2])
def test_post_sign_checks_reject_same_bytes_trailing_bit_alias_in_each_segment(
    monkeypatch: pytest.MonkeyPatch, segment_index: int
) -> None:
    original_encode = jwt.encode

    def aliased_encode(*args: Any, **kwargs: Any) -> str:
        segments = original_encode(*args, **kwargs).split(".")
        original = segments[segment_index]
        segments[segment_index] = _same_bytes_trailing_bit_alias(original)
        assert segments[segment_index] != original
        assert _decode(segments[segment_index]) == _decode(original)
        if segment_index in {0, 1}:
            signing_input = f"{segments[0]}.{segments[1]}".encode("ascii")
            segments[2] = _encode(kwargs["key"].sign(signing_input))
        return ".".join(segments)

    monkeypatch.setattr(jwt, "encode", aliased_encode)

    with pytest.raises(ExecutionProofSigningError, match="signing failed"):
        _signer().issue_execution_proof(_input())


def test_signer_rejects_sensitive_output_without_echoing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_compact = "private-path.signature.jti-binding"

    def invalid_encode(*args: Any, **kwargs: Any) -> str:
        return sensitive_compact

    monkeypatch.setattr(jwt, "encode", invalid_encode)

    with pytest.raises(ExecutionProofSigningError) as error:
        _signer().issue_execution_proof(_input())

    text = str(error.value)
    assert sensitive_compact not in text
    assert "AAECAwQFBgcICQoLDA0ODw" not in text
    assert "691422ac1ca52c03760fd683b58178423" not in text


def test_independent_rfc8032_known_answer() -> None:
    private_key = Ed25519PrivateKey.from_private_bytes(SECOND_RFC8032_SEED)

    assert private_key.sign(b"\x72") == SECOND_RFC8032_SIGNATURE
