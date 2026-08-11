"""Small defensive redaction layer for gateway audit metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

REDACTED = "<redacted>"
SENSITIVE_KEYS = frozenset(
    {
        "access_token",
        "refresh_token",
        "client_secret",
        "app_secret",
        "api_key",
        "authorization",
        "cookie",
        "accountnumber",
        "account_number",
        "accounthash",
        "account_hash",
        "hashvalue",
        "streamerinfo",
        "webhook_url",
    }
)


def redact(value: Any) -> Any:
    """Return a recursively redacted copy suitable for bounded audit metadata."""
    if isinstance(value, Mapping):
        return {
            str(key): REDACTED if str(key).lower() in SENSITIVE_KEYS else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact(item) for item in value]
    return value
