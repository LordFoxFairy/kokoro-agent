"""Shared text-only display validation, independent of activity models."""

from __future__ import annotations

import re
import unicodedata


def sensitive_display_text(value: str) -> bool:
    return bool(
        re.search(
            r"(?i)(?:^|[._ /-])(?:credentials?|secrets?|tokens?|passwords?|private[_-]?key)(?:$|[._ /-])"
            r"|(?:^|/)\.(?:ssh|aws|gnupg|kube)(?:/|$)|(?:^|/)\.env(?:[./_-]|$)"
            r"|(?:^|/)(?:\.netrc|\.npmrc|\.git-credentials|service[_-]account\.json)(?:$|/)"
            r"|(?:id_rsa|id_ed25519|\.pem$|\.p12$|\.pfx$|bearer\s|(?<![A-Za-z0-9])sk[-_]|gh[pousr]_|github_pat_|AKIA[A-Z0-9]{16}|-----BEGIN)"
            r"|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",
            value,
        )
    )


_PRIVATE_TEXT = re.compile(
    r"(?i)(?:https?://[^\s]+[?&](?:token|key|api_key|signature|sig|auth|x-amz-[a-z-]+)=|https?://[^\s/]+@|(?:^|[\s\"'])(?:/(?:Users|home|private|etc|var|proc|sys|root|tmp|opt)/|[A-Z]:\\)|"
    r"Traceback \(most recent call last\)|\b[A-Za-z]*(?:Error|Exception)\s*:|\b(?:Authorization|Set-Cookie|Cookie)\s*:|"
    r"\b(?:api[_-]?key|password|secret|access[_-]?token)\s*[=:]|"
    r"\b(?:DATABASE_URL|REDIS_URL|HOME|PATH)\s*=)"
)


def private_text(value: str) -> bool:
    return (
        any(
            unicodedata.category(c) in {"Cs", "Cf"}
            or (unicodedata.category(c) == "Cc" and c not in "\n\r\t")
            for c in value
        )
        or sensitive_display_text(value)
        or _PRIVATE_TEXT.search(value) is not None
    )


def public_action_title(value: object) -> str | None:
    if not isinstance(value, str) or not value or value != value.strip():
        return None
    if (
        len(value) > 256
        or any(unicodedata.category(c) in {"Cc", "Cf", "Cs"} for c in value)
        or len(value.encode("utf-8")) > 256
        or private_text(value)
        or re.search(
            r"(?i)\b(?:token|credential|secret|password|api[_-]?key)\s*[:=]", value
        )
    ):
        return None
    return value
