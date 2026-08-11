from __future__ import annotations

import json
from pathlib import Path

import pytest

from schwab_gateway import auth
from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)

SYNTHETIC_KEYS = {
    "butterfly-guy": "synthetic-butterfly-key",
    "equity-scanner": "synthetic-scanner-key",
    "afterhours-lab": "synthetic-lab-key",
}


def principal(client_id: str, *, key: str | None = None) -> InternalPrincipal:
    return InternalPrincipal(
        client_id=client_id,
        key_sha256=hash_api_key(key or SYNTHETIC_KEYS[client_id]),
        capabilities=frozenset({"market_data:read"}),
        priority_class=(
            PriorityClass.PROTECTED
            if client_id == "butterfly-guy"
            else PriorityClass.BACKGROUND
        ),
    )


def test_authenticator_matches_hashed_keys_without_storing_plaintext() -> None:
    principal = InternalPrincipal(
        client_id="afterhours-lab",
        key_sha256=hash_api_key("test-only-secret"),
        capabilities=frozenset({"market_data:read"}),
        priority_class=PriorityClass.BACKGROUND,
    )
    authenticator = InternalKeyAuthenticator((principal,))

    assert authenticator.authenticate("test-only-secret") == principal
    assert authenticator.authenticate("wrong") is None
    assert "test-only-secret" not in repr(authenticator)


def test_authenticator_loads_versioned_file(tmp_path) -> None:
    path = tmp_path / "keys.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "clients": [
                    {
                        "id": "butterfly-guy",
                        "key_sha256": hash_api_key("client-key"),
                        "capabilities": ["market_data:read"],
                        "priority_class": "protected",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)

    authenticator = InternalKeyAuthenticator.from_file(path)

    assert authenticator.authenticate("client-key").client_id == "butterfly-guy"


def test_load_file_returns_the_exact_document_it_validated(tmp_path) -> None:
    path = tmp_path / "keys.json"
    payload = {
        "version": 1,
        "clients": [
            {
                "id": "butterfly-guy",
                "key_sha256": hash_api_key("client-key"),
                "capabilities": ["market_data:read"],
                "priority_class": "protected",
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)

    authenticator, validated_payload = InternalKeyAuthenticator.load_file(path)

    assert validated_payload == payload
    assert authenticator.authenticate("client-key").client_id == "butterfly-guy"


def test_authenticator_rejects_writable_key_file(tmp_path) -> None:
    path = tmp_path / "keys.json"
    path.write_text('{"version": 1, "clients": []}', encoding="utf-8")
    path.chmod(0o622)

    with pytest.raises(ValueError, match="must not be group/world writable"):
        InternalKeyAuthenticator.from_file(path)


def test_authenticator_rejects_unknown_key_file_fields(tmp_path) -> None:
    path = tmp_path / "keys.json"
    path.write_text(
        json.dumps({"version": 1, "clients": [], "raw_key": "must-not-be-accepted"}),
        encoding="utf-8",
    )
    path.chmod(0o600)

    with pytest.raises(ValueError, match="invalid gateway keys file schema"):
        InternalKeyAuthenticator.from_file(path)


@pytest.mark.parametrize("capability", ["history:read", "options:read", "orders:write"])
def test_principal_rejects_capabilities_without_implemented_routes(capability: str) -> None:
    with pytest.raises(ValueError, match="unknown gateway capabilities"):
        InternalPrincipal(
            client_id="equity-scanner",
            key_sha256=hash_api_key("key"),
            capabilities=frozenset({capability}),
            priority_class=PriorityClass.BACKGROUND,
        )


def test_all_three_service_identities_authenticate_independently() -> None:
    authenticator = InternalKeyAuthenticator(tuple(principal(name) for name in SYNTHETIC_KEYS))

    for client_id, key in SYNTHETIC_KEYS.items():
        authenticated = authenticator.authenticate(key)
        assert authenticated is not None
        assert authenticated.client_id == client_id
        assert all(
            authenticator.authenticate(other_key) != authenticated
            for other_id, other_key in SYNTHETIC_KEYS.items()
            if other_id != client_id
        )


def test_changing_one_key_does_not_change_other_identities() -> None:
    original = InternalKeyAuthenticator(tuple(principal(name) for name in SYNTHETIC_KEYS))
    changed = InternalKeyAuthenticator(
        tuple(
            principal(name, key="rotated-butterfly-key")
            if name == "butterfly-guy"
            else principal(name)
            for name in SYNTHETIC_KEYS
        )
    )

    assert original.authenticate(SYNTHETIC_KEYS["butterfly-guy"]) is not None
    assert changed.authenticate(SYNTHETIC_KEYS["butterfly-guy"]) is None
    assert changed.authenticate("rotated-butterfly-key").client_id == "butterfly-guy"
    for client_id in ("equity-scanner", "afterhours-lab"):
        assert changed.authenticate(SYNTHETIC_KEYS[client_id]) == original.authenticate(
            SYNTHETIC_KEYS[client_id]
        )


def test_authentication_compares_every_digest_in_constant_time(monkeypatch) -> None:
    compared: list[tuple[str, str]] = []
    real_compare = auth.hmac.compare_digest

    def recording_compare(candidate: str, configured: str) -> bool:
        compared.append((candidate, configured))
        return real_compare(candidate, configured)

    monkeypatch.setattr(auth.hmac, "compare_digest", recording_compare)
    authenticator = InternalKeyAuthenticator(tuple(principal(name) for name in SYNTHETIC_KEYS))

    assert authenticator.authenticate(SYNTHETIC_KEYS["butterfly-guy"]) is not None
    assert len(compared) == len(SYNTHETIC_KEYS)


@pytest.mark.parametrize(
    "client_id",
    [
        "UPPERCASE",
        "-leading-hyphen",
        "trailing-hyphen-",
        "contains_underscore",
        "x" * 65,
    ],
)
def test_invalid_application_id_fails_closed(client_id: str) -> None:
    with pytest.raises(ValueError, match="application ID"):
        InternalPrincipal(
            client_id=client_id,
            key_sha256=hash_api_key("synthetic-key"),
            capabilities=frozenset({"market_data:read"}),
            priority_class=PriorityClass.BACKGROUND,
        )


@pytest.mark.parametrize("priority", list(PriorityClass))
def test_new_application_ids_are_configuration_driven(priority: PriorityClass) -> None:
    principal = InternalPrincipal(
        client_id="new-research-consumer",
        key_sha256=hash_api_key("synthetic-key"),
        capabilities=frozenset({"market_data:read"}),
        priority_class=priority,
    )
    assert principal.priority_class is priority


def test_committed_example_contains_only_bounded_identity_metadata() -> None:
    payload = json.loads(Path("configs/schwab_gateway_keys.example.json").read_text())

    assert {item["id"] for item in payload["clients"]} == set(SYNTHETIC_KEYS)
    assert len({item["key_sha256"] for item in payload["clients"]}) == 3
    assert all(
        set(item) == {"id", "key_sha256", "capabilities", "priority_class"}
        for item in payload["clients"]
    )
    assert not any(key in repr(payload) for key in SYNTHETIC_KEYS.values())
