"""Opt-in client configuration; direct access remains the safe default."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewayClientSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    access_mode: Literal["direct", "gateway"] = Field(
        default="direct",
        validation_alias="SCHWAB_ACCESS_MODE",
    )
    gateway_url: str = Field(
        default="",
        validation_alias="SCHWAB_GATEWAY_URL",
    )
    gateway_api_key: SecretStr = Field(
        default=SecretStr(""),
        validation_alias="SCHWAB_GATEWAY_API_KEY",
    )
    shadow_reads: bool = Field(
        default=False,
        validation_alias="SCHWAB_GATEWAY_SHADOW_READS",
    )

    @model_validator(mode="after")
    def gateway_mode_requires_connection_settings(self) -> GatewayClientSettings:
        if self.access_mode == "gateway" and (
            not self.gateway_url or not self.gateway_api_key.get_secret_value()
        ):
            raise ValueError("gateway mode requires SCHWAB_GATEWAY_URL and API key")
        return self

    @model_validator(mode="after")
    def shadow_reads_require_connection_settings(self) -> GatewayClientSettings:
        if self.shadow_reads and (
            not self.gateway_url or not self.gateway_api_key.get_secret_value()
        ):
            raise ValueError("shadow reads require SCHWAB_GATEWAY_URL and API key")
        return self

    @model_validator(mode="after")
    def shadow_reads_incompatible_with_gateway_mode(self) -> GatewayClientSettings:
        if self.access_mode == "gateway" and self.shadow_reads:
            raise ValueError(
                "shadow reads compare a direct read against the gateway and are "
                "meaningless once access_mode is already 'gateway'"
            )
        return self
