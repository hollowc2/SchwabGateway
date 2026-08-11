"""Run one explicitly authorized Schwab gateway credential proof without starting a server."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from typing import NoReturn

# Fixed failure codes, split by the only question a failed proof must answer: did a token
# read occur? These three are raised before the credential probe is entered, so no token
# store was opened and no Schwab request was made.
PROBE_NO_TOKEN_READ_CODES = (
    "probe_import_failed",
    "probe_settings_invalid",
    "probe_sdk_import_failed",
)
# These four are raised from inside the probe, after the token-manager transaction has
# opened the token store, so a token read was reached.
PROBE_TOKEN_READ_CODES = (
    "probe_token_invalid",
    "probe_client_construction_failed",
    "probe_quote_failed",
    "probe_state_invalid",
)
PROBE_FAILURE_CODES = frozenset(PROBE_NO_TOKEN_READ_CODES + PROBE_TOKEN_READ_CODES)

_REASON_CODES = {
    "token_invalid": "probe_token_invalid",
    "client_construction_failed": "probe_client_construction_failed",
    "quote_failed": "probe_quote_failed",
    "state_invalid": "probe_state_invalid",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authorize-real-credential-read", action="store_true")
    parser.add_argument("--confirm-single-token-writer", action="store_true")
    parser.add_argument("--confirm-no-deployment", action="store_true")
    return parser


def _load_runtime_dependencies():
    """Import failure-prone runtime dependencies inside the bounded CLI path."""
    from schwab_gateway.config import GatewayCredentialProbeSettings
    from schwab_gateway.credential_probe import (
        GatewayCredentialProbeError,
        run_gateway_credential_probe,
    )
    from schwab_gateway.logging import setup_logging

    return (
        setup_logging,
        GatewayCredentialProbeSettings,
        run_gateway_credential_probe,
        GatewayCredentialProbeError,
    )


def _fail(code: str) -> NoReturn:
    """Emit one fixed code on stdout, nothing on stderr, and exit nonzero."""
    if code not in PROBE_FAILURE_CODES:
        code = "probe_state_invalid"
    sys.stdout.write(
        json.dumps({"code": code, "status": "error"}, separators=(",", ":"), sort_keys=True) + "\n"
    )
    sys.stdout.flush()
    raise SystemExit(1)


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    if not all(
        (
            args.authorize_real_credential_read,
            args.confirm_single_token_writer,
            args.confirm_no_deployment,
        )
    ):
        # Refusal is not a runtime failure. The operator's refusal gate proves this exact
        # argparse behaviour — stderr message, empty stdout, exit 2 — so it emits no code.
        _parser().error(
            "credential proof requires explicit credential, single-writer, "
            "and no-deploy confirmations"
        )

    try:
        setup_logging, settings_type, run_probe, probe_error = _load_runtime_dependencies()
        setup_logging("CRITICAL", json_output=True)
    except Exception:
        _fail("probe_import_failed")

    try:
        settings = settings_type()
    except Exception:
        _fail("probe_settings_invalid")

    try:
        from schwab.auth import client_from_access_functions
    except Exception:
        _fail("probe_sdk_import_failed")

    try:
        result = run_probe(settings, client_from_access_functions)
    except probe_error as exc:
        _fail(_REASON_CODES.get(getattr(exc, "reason", ""), "probe_state_invalid"))
    except Exception:
        _fail("probe_state_invalid")

    try:
        print(json.dumps(asdict(result), separators=(",", ":"), sort_keys=True))
    except Exception:
        _fail("probe_state_invalid")


if __name__ == "__main__":
    main()
