"""Internal service authentication with hashed, capability-scoped API keys."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from aiohttp import web

KNOWN_CAPABILITIES = frozenset({"market_data:read"})
# Retained as documentation and compatibility metadata only. New validated application
# IDs are accepted without a server release.
KNOWN_CLIENT_IDS = frozenset(
    {
        "butterfly-guy",
        "equity-scanner",
        "afterhours-lab",
    }
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_APPLICATION_ID_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")


class PriorityClass(StrEnum):
    PROTECTED = "protected"
    BACKGROUND = "background"


LEGACY_PRIORITY_BY_CLIENT = {
    "butterfly-guy": PriorityClass.PROTECTED,
    "equity-scanner": PriorityClass.BACKGROUND,
    "afterhours-lab": PriorityClass.BACKGROUND,
}


def hash_api_key(api_key: str) -> str:
    if not api_key:
        raise ValueError("API key must not be empty")
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class InternalPrincipal:
    client_id: str
    key_sha256: str
    capabilities: frozenset[str]
    priority_class: PriorityClass

    def __post_init__(self) -> None:
        if not _APPLICATION_ID_PATTERN.fullmatch(self.client_id):
            raise ValueError("invalid gateway application ID")
        if not _SHA256_PATTERN.fullmatch(self.key_sha256):
            raise ValueError("key_sha256 must be a lowercase SHA-256 digest")
        unknown = self.capabilities - KNOWN_CAPABILITIES
        if unknown:
            raise ValueError(f"unknown gateway capabilities: {sorted(unknown)}")


class InternalKeyAuthenticator:
    def __init__(self, principals: tuple[InternalPrincipal, ...]) -> None:
        if not principals:
            raise ValueError("at least one gateway principal is required")
        if len({p.client_id for p in principals}) != len(principals):
            raise ValueError("gateway client IDs must be unique")
        if len({p.key_sha256 for p in principals}) != len(principals):
            raise ValueError("gateway API key digests must be unique")
        self._principals = principals

    @classmethod
    def from_document(cls, payload: Any) -> InternalKeyAuthenticator:
        """Build an authenticator from one already-parsed keys document."""
        if (
            not isinstance(payload, dict)
            or set(payload) != {"version", "clients"}
            or payload.get("version") != 1
            or not isinstance(payload.get("clients"), list)
        ):
            raise ValueError("invalid gateway keys file schema")
        try:
            principals = tuple(
                InternalPrincipal(
                    client_id=item["id"],
                    key_sha256=item["key_sha256"],
                    capabilities=frozenset(item["capabilities"]),
                    priority_class=PriorityClass(item["priority_class"]),
                )
                for item in payload["clients"]
                if isinstance(item, dict)
                and set(item)
                == {"id", "key_sha256", "capabilities", "priority_class"}
                and isinstance(item["id"], str)
                and isinstance(item["key_sha256"], str)
                and isinstance(item["capabilities"], list)
                and isinstance(item["priority_class"], str)
                and all(isinstance(value, str) for value in item["capabilities"])
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid gateway client entry") from exc
        if len(principals) != len(payload["clients"]):
            raise ValueError("invalid gateway client entry")
        return cls(principals)

    @classmethod
    def load_file(
        cls,
        path: Path,
    ) -> tuple[InternalKeyAuthenticator, dict[str, Any]]:
        """Read once and return both the authenticator and exact validated document."""
        try:
            handle = path.open(encoding="utf-8")
        except OSError as exc:
            raise ValueError("gateway keys file could not be read or parsed") from exc
        with handle:
            mode = os.fstat(handle.fileno()).st_mode
            if mode & 0o022:
                raise ValueError("gateway keys file must not be group/world writable")
            try:
                payload = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError("gateway keys file could not be read or parsed") from exc
        authenticator = cls.from_document(payload)
        return authenticator, payload

    @classmethod
    def from_file(cls, path: Path) -> InternalKeyAuthenticator:
        authenticator, _ = cls.load_file(path)
        return authenticator

    def authenticate(self, api_key: str) -> InternalPrincipal | None:
        if not api_key:
            return None
        candidate = hash_api_key(api_key)
        authenticated: InternalPrincipal | None = None
        for principal in self._principals:
            if hmac.compare_digest(candidate, principal.key_sha256):
                authenticated = principal
        return authenticated


@web.middleware
async def authentication_middleware(
    request: web.Request,
    handler,
) -> web.StreamResponse:
    if not request.path.startswith("/v1/"):
        return await handler(request)
    authenticator = request.app[AUTHENTICATOR_KEY]
    principal = authenticator.authenticate(
        request.headers.get("X-Internal-API-Key", "")
    )
    if principal is None:
        return web.json_response(
            {
                "schema_version": "1.0",
                "error": {
                    "code": "authentication_required",
                    "message": "valid internal authentication is required",
                },
            },
            status=401,
        )
    request[PRINCIPAL_KEY] = principal
    return await handler(request)


def require_capability(request: web.Request, capability: str) -> web.Response | None:
    principal = request[PRINCIPAL_KEY]
    if capability in principal.capabilities:
        return None
    return web.json_response(
        {
            "schema_version": "1.0",
            "error": {
                "code": "capability_denied",
                "message": "the caller lacks the required capability",
            },
        },
        status=403,
    )


AUTHENTICATOR_KEY = web.AppKey("gateway_authenticator", InternalKeyAuthenticator)
PRINCIPAL_KEY = "gateway_principal"
