"""UTC timestamp invariants for Agent-owned Chat facts."""

from __future__ import annotations

from datetime import UTC, datetime


def epoch_millis_to_utc(value: int) -> datetime:
    """Convert a non-negative epoch-millisecond value to an aware UTC instant."""

    if value < 0:
        raise ValueError("epoch-millisecond timestamp must be non-negative")
    seconds, milliseconds = divmod(value, 1000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(
        microsecond=milliseconds * 1000
    )


def normalize_utc_datetime(value: datetime) -> datetime:
    """Require an aware instant and normalize it to PostgreSQL millisecond precision."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Chat timestamps must be timezone-aware")
    utc = value.astimezone(UTC)
    return utc.replace(microsecond=(utc.microsecond // 1000) * 1000)


__all__ = ["epoch_millis_to_utc", "normalize_utc_datetime"]
