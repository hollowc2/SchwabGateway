"""Read-only Schwab gateway foundation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from schwab_gateway.api import create_app

__all__ = ["create_app"]


def __getattr__(name: str) -> Any:
    """Import ``api`` lazily so the reviewed credential-proof subset loads standalone."""
    if name == "create_app":
        from schwab_gateway.api import create_app

        return create_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
