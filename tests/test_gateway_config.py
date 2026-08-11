from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError
from schwab_gateway_sdk.config import GatewayClientSettings

from schwab_gateway.config import GatewaySettings


def settings(**overrides) -> GatewaySettings:
    return GatewaySettings(
        internal_keys_path=Path("/run/secrets/keys.json"),
        **overrides,
    )


def test_gateway_defaults_to_loopback_and_no_order_writes() -> None:
    value = settings()

    assert value.bind_host == "127.0.0.1"
    assert value.port == 8010
    assert value.order_writes_enabled is False
    assert value.protected_capacity == 4
    assert value.background_capacity == 8


def test_gateway_rejects_public_bind_and_order_writes() -> None:
    with pytest.raises(ValidationError, match="must not be public"):
        settings(bind_host="8.8.8.8")
    with pytest.raises(ValidationError, match="order writes are not available"):
        settings(order_writes_enabled=True)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("protected_capacity", 0),
        ("protected_capacity", 257),
        ("background_capacity", 0),
        ("background_capacity", 257),
    ],
)
def test_gateway_rejects_nonpositive_or_unbounded_capacity(field: str, value: int) -> None:
    with pytest.raises(ValidationError, match="capacity must be between 1 and 256"):
        settings(**{field: value})


def test_gateway_client_mode_is_opt_in_and_secret_is_hidden() -> None:
    direct = GatewayClientSettings(SCHWAB_ACCESS_MODE="direct")
    gateway = GatewayClientSettings(
        SCHWAB_ACCESS_MODE="gateway",
        SCHWAB_GATEWAY_URL="http://127.0.0.1:8010",
        SCHWAB_GATEWAY_API_KEY="test-secret",
    )

    assert direct.access_mode == "direct"
    assert gateway.access_mode == "gateway"
    assert "test-secret" not in repr(gateway)


def test_gateway_client_mode_requires_url_and_key() -> None:
    with pytest.raises(ValidationError, match="gateway mode requires"):
        GatewayClientSettings(SCHWAB_ACCESS_MODE="gateway")


def test_gateway_client_mode_rejects_shadow_reads() -> None:
    with pytest.raises(ValidationError, match="shadow reads"):
        GatewayClientSettings(
            SCHWAB_ACCESS_MODE="gateway",
            SCHWAB_GATEWAY_SHADOW_READS="true",
            SCHWAB_GATEWAY_URL="http://127.0.0.1:8010",
            SCHWAB_GATEWAY_API_KEY="test-secret",
        )


def test_gateway_client_valid_mode_and_shadow_combinations() -> None:
    direct_no_shadow = GatewayClientSettings(SCHWAB_ACCESS_MODE="direct")
    direct_with_shadow = GatewayClientSettings(
        SCHWAB_ACCESS_MODE="direct",
        SCHWAB_GATEWAY_SHADOW_READS="true",
        SCHWAB_GATEWAY_URL="http://127.0.0.1:8010",
        SCHWAB_GATEWAY_API_KEY="test-secret",
    )
    gateway_no_shadow = GatewayClientSettings(
        SCHWAB_ACCESS_MODE="gateway",
        SCHWAB_GATEWAY_URL="http://127.0.0.1:8010",
        SCHWAB_GATEWAY_API_KEY="test-secret",
    )

    assert direct_no_shadow.access_mode == "direct"
    assert direct_no_shadow.shadow_reads is False
    assert direct_with_shadow.shadow_reads is True
    assert gateway_no_shadow.access_mode == "gateway"
    assert gateway_no_shadow.shadow_reads is False
