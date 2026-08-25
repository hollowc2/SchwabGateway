"""Validated configuration for the isolated gateway process."""

from __future__ import annotations

import ipaddress
import math
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SCHWAB_GATEWAY_",
        extra="forbid",
    )

    bind_host: str = "127.0.0.1"
    port: int = 8010
    internal_keys_path: Path
    log_level: str = "INFO"
    order_writes_enabled: bool = False
    upstream_timeout_seconds: float = 3.0
    protected_capacity: int = 8
    background_capacity: int = 8
    option_chain_cache_ttl_seconds: float = 4.0
    option_chain_cache_max_entries: int = 16
    option_chain_max_inflight: int = 4

    @field_validator("bind_host")
    @classmethod
    def bind_host_must_be_private(cls, value: str) -> str:
        if value == "localhost":
            return value
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise ValueError("gateway bind host must be a literal private address") from exc
        if not (address.is_loopback or address.is_private or address.is_unspecified):
            raise ValueError("gateway bind host must not be public")
        return value

    @field_validator("port")
    @classmethod
    def port_must_be_valid(cls, value: int) -> int:
        if not 1 <= value <= 65535:
            raise ValueError("gateway port must be between 1 and 65535")
        return value

    @field_validator("upstream_timeout_seconds")
    @classmethod
    def timeout_must_be_positive(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("gateway upstream timeout must be positive")
        return value

    @field_validator("protected_capacity", "background_capacity")
    @classmethod
    def capacity_must_be_bounded(cls, value: int) -> int:
        if not 1 <= value <= 256:
            raise ValueError("gateway capacity must be between 1 and 256")
        return value

    @field_validator("option_chain_cache_ttl_seconds")
    @classmethod
    def option_chain_cache_ttl_must_be_bounded(cls, value: float) -> float:
        if not math.isfinite(value) or not 0 < value <= 4:
            raise ValueError("option-chain cache TTL must be greater than 0 and at most 4s")
        return value

    @field_validator("option_chain_cache_max_entries", "option_chain_max_inflight")
    @classmethod
    def option_chain_cache_capacity_must_be_bounded(cls, value: int) -> int:
        if not 1 <= value <= 16:
            raise ValueError("option-chain cache capacity must be between 1 and 16")
        return value

    @field_validator("order_writes_enabled")
    @classmethod
    def foundation_disables_order_writes(cls, value: bool) -> bool:
        if value:
            raise ValueError("order writes are not available in the gateway foundation")
        return value


class GatewayCredentialProbeSettings(BaseSettings):
    """Explicit real-credential inputs for the standalone quote proof only."""

    model_config = SettingsConfigDict(extra="ignore")

    api_key: SecretStr = Field(validation_alias="SCHWAB_API_KEY")
    app_secret: SecretStr = Field(validation_alias="SCHWAB_SECRET_KEY")
    token_path: Path = Field(validation_alias="SCHWAB_TOKEN_PATH", repr=False)

    @field_validator("token_path")
    @classmethod
    def token_path_must_be_absolute(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("credential probe token path must be absolute")
        return value
