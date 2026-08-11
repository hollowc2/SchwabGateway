"""One bounded quote proof through the locked token adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from schwab_token_store import (
    AtomicFileTokenStore,
    AtomicTokenManager,
    TokenManagerError,
    TokenManagerState,
)

from schwab_gateway.config import GatewayCredentialProbeSettings
from schwab_gateway.token_adapter import (
    LockedSchwabClientAdapter,
    SchwabAccessFunctionClientFactory,
    SchwabClientConstructionError,
    SchwabClientOperationError,
    SchwabTokenAdapterError,
)

PROBE_SYMBOL = "AAPL"

# Every reason below is raised only after the manager transaction has opened the token
# store, so each one proves a token read was reached. Failures that occur before the
# transaction opens are not represented here; they are classified by the CLI instead.
GatewayCredentialProbeReason = Literal[
    "token_invalid",
    "client_construction_failed",
    "quote_failed",
    "state_invalid",
]


class GatewayCredentialProbeError(RuntimeError):
    """Bounded failure safe for operator output.

    The message is fixed. ``reason`` is a fixed literal naming the failing stage; it never
    carries exception text, token state, paths, payloads, or account identifiers.
    """

    def __init__(self, reason: GatewayCredentialProbeReason) -> None:
        super().__init__("Schwab gateway credential probe failed")
        self.reason: GatewayCredentialProbeReason = reason


@dataclass(frozen=True)
class GatewayCredentialProbeResult:
    status: Literal["ok"]
    token_state: Literal["ready"]
    quote_count: Literal[1]


def run_gateway_credential_probe(
    settings: GatewayCredentialProbeSettings,
    client_factory: SchwabAccessFunctionClientFactory[Any],
) -> GatewayCredentialProbeResult:
    """Read one public quote without resolving an account or exposing response data."""

    manager = AtomicTokenManager(AtomicFileTokenStore(settings.token_path))
    adapter = LockedSchwabClientAdapter(
        manager,
        client_factory,
        api_key=settings.api_key.get_secret_value(),
        app_secret=settings.app_secret.get_secret_value(),
    )

    def quote_operation(client: Any) -> int:
        session = getattr(client, "session", None)
        close = getattr(session, "close", None)
        try:
            fields = [client.Quote.Fields.QUOTE, client.Quote.Fields.EXTENDED]
            response = client.get_quotes([PROBE_SYMBOL], fields=fields)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get(PROBE_SYMBOL), dict):
                raise ValueError("credential probe quote response is malformed")
            return 1
        finally:
            if callable(close):
                close()

    try:
        quote_count = adapter.execute(quote_operation)
    except TokenManagerError:
        raise GatewayCredentialProbeError("token_invalid") from None
    except SchwabClientConstructionError:
        raise GatewayCredentialProbeError("client_construction_failed") from None
    except SchwabClientOperationError:
        raise GatewayCredentialProbeError("quote_failed") from None
    except SchwabTokenAdapterError:
        # The adapter only raises the two subclasses above; a bare adapter error is
        # classified as the earliest of them so the reason never overstates progress.
        raise GatewayCredentialProbeError("client_construction_failed") from None

    if manager.health().state is not TokenManagerState.READY or quote_count != 1:
        raise GatewayCredentialProbeError("state_invalid")
    return GatewayCredentialProbeResult(
        status="ok",
        token_state="ready",
        quote_count=1,
    )
