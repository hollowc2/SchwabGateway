from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from schwab_gateway import probe_credentials as probe_command
from schwab_gateway.config import GatewayCredentialProbeSettings
from schwab_gateway.credential_probe import (
    GatewayCredentialProbeError,
    GatewayCredentialProbeResult,
    run_gateway_credential_probe,
)

NOW = 2_000_000_000
FAKE_API_KEY = "fake-api-key"
FAKE_APP_SECRET = "fake-app-secret"


def token_document() -> dict:
    return {
        "creation_timestamp": NOW - 60,
        "token": {
            "access_token": "access-secret",
            "refresh_token": "refresh-secret",
        },
    }


def write_token(path: Path) -> None:
    path.write_text(json.dumps(token_document()), encoding="utf-8")
    path.chmod(0o600)


def settings(path: Path) -> GatewayCredentialProbeSettings:
    return GatewayCredentialProbeSettings(
        SCHWAB_API_KEY=FAKE_API_KEY,
        SCHWAB_SECRET_KEY=FAKE_APP_SECRET,
        SCHWAB_TOKEN_PATH=path,
    )


class FakeResponse:
    def __init__(self, *, malformed: bool = False) -> None:
        self._malformed = malformed

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        if self._malformed:
            return {"unexpected": {}}
        return {"AAPL": {"quote": {"mark": 100.0}}}


class FakeClient:
    Quote = SimpleNamespace(
        Fields=SimpleNamespace(QUOTE="QUOTE", EXTENDED="EXTENDED")
    )

    def __init__(self, *, malformed: bool = False) -> None:
        self.calls: list[tuple[list[str], list[str]]] = []
        self.session = MagicMock()
        self._malformed = malformed

    def get_quotes(self, symbols, *, fields):
        self.calls.append((symbols, fields))
        return FakeResponse(malformed=self._malformed)


class FakeFactory:
    def __init__(self, client: FakeClient) -> None:
        self.client = client
        self.calls: list[dict] = []

    def __call__(
        self,
        api_key,
        app_secret,
        token_read_func,
        token_write_func,
        asyncio=False,
        enforce_enums=True,
    ) -> FakeClient:
        del token_write_func
        loaded = token_read_func()
        assert loaded == token_document()
        self.calls.append(
            {
                "api_key": api_key,
                "app_secret": app_secret,
                "asyncio": asyncio,
                "enforce_enums": enforce_enums,
            }
        )
        return self.client


def test_probe_performs_only_one_bounded_quote_read(tmp_path: Path) -> None:
    token_path = tmp_path / "synthetic-token.json"
    write_token(token_path)
    client = FakeClient()
    factory = FakeFactory(client)

    result = run_gateway_credential_probe(settings(token_path), factory)

    assert result.status == "ok"
    assert result.token_state == "ready"
    assert result.quote_count == 1
    assert factory.calls == [
        {
            "api_key": FAKE_API_KEY,
            "app_secret": FAKE_APP_SECRET,
            "asyncio": False,
            "enforce_enums": True,
        }
    ]
    assert client.calls == [(["AAPL"], ["QUOTE", "EXTENDED"])]
    client.session.close.assert_called_once_with()


def test_probe_normalizes_malformed_response_without_exposing_data(tmp_path: Path) -> None:
    token_path = tmp_path / "synthetic-token.json"
    write_token(token_path)

    with pytest.raises(GatewayCredentialProbeError) as exc:
        run_gateway_credential_probe(settings(token_path), FakeFactory(FakeClient(malformed=True)))

    assert str(exc.value) == "Schwab gateway credential probe failed"
    assert "unexpected" not in str(exc.value)
    assert exc.value.reason == "quote_failed"


def test_probe_reports_client_construction_separately_from_the_quote(tmp_path: Path) -> None:
    """Construction and operation must stay distinguishable; both prove a token read."""
    token_path = tmp_path / "synthetic-token.json"
    write_token(token_path)

    def failing_factory(*_args, **_kwargs):
        raise RuntimeError("sensitive-construction-detail")

    with pytest.raises(GatewayCredentialProbeError) as exc:
        run_gateway_credential_probe(settings(token_path), failing_factory)

    assert exc.value.reason == "client_construction_failed"
    assert "sensitive-construction-detail" not in str(exc.value)


def test_probe_reports_a_token_fault_distinctly(tmp_path: Path) -> None:
    missing_token = tmp_path / "absent-token.json"

    with pytest.raises(GatewayCredentialProbeError) as exc:
        run_gateway_credential_probe(settings(missing_token), FakeFactory(FakeClient()))

    assert exc.value.reason == "token_invalid"
    assert str(missing_token) not in str(exc.value)


def test_probe_settings_require_absolute_path_and_redact_inputs(tmp_path: Path) -> None:
    token_path = tmp_path / "synthetic-token.json"
    value = settings(token_path)
    assert FAKE_API_KEY not in repr(value)
    assert FAKE_APP_SECRET not in repr(value)
    assert str(token_path) not in repr(value)

    with pytest.raises(ValidationError, match="must be absolute"):
        GatewayCredentialProbeSettings(
            SCHWAB_API_KEY=FAKE_API_KEY,
            SCHWAB_SECRET_KEY=FAKE_APP_SECRET,
            SCHWAB_TOKEN_PATH="relative-token.json",
        )


def test_probe_command_refuses_to_load_settings_without_all_confirmations() -> None:
    with pytest.raises(SystemExit):
        probe_command.main([])


CONFIRMATIONS = [
    "--authorize-real-credential-read",
    "--confirm-single-token-writer",
    "--confirm-no-deployment",
]


def run_probe_command(capsys: pytest.CaptureFixture) -> tuple[int | None, str, str]:
    with pytest.raises(SystemExit) as exc:
        probe_command.main(CONFIRMATIONS)
    captured = capsys.readouterr()
    return exc.value.code, captured.out, captured.err


def install_fake_sdk(monkeypatch: pytest.MonkeyPatch, factory: object) -> None:
    fake_schwab = ModuleType("schwab")
    fake_auth = ModuleType("schwab.auth")
    fake_auth.client_from_access_functions = factory
    fake_schwab.auth = fake_auth
    monkeypatch.setitem(sys.modules, "schwab", fake_schwab)
    monkeypatch.setitem(sys.modules, "schwab.auth", fake_auth)


def install_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    settings_type=lambda: object(),
    run_probe=None,
) -> None:
    monkeypatch.setattr(
        probe_command,
        "_load_runtime_dependencies",
        lambda: (
            MagicMock(),
            settings_type,
            run_probe if run_probe is not None else MagicMock(return_value=object()),
            GatewayCredentialProbeError,
        ),
    )


def test_probe_command_bounds_runtime_import_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """An import fault proves no token read occurred, and must say so on stdout only."""

    def fail_runtime_import():
        raise ImportError("sensitive-runtime-import-detail")

    monkeypatch.setattr(
        probe_command,
        "_load_runtime_dependencies",
        fail_runtime_import,
    )

    code, out, err = run_probe_command(capsys)

    assert code == 1
    assert out == '{"code":"probe_import_failed","status":"error"}\n'
    assert err == ""


def test_probe_command_bounds_configuration_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    monkeypatch.setenv("SCHWAB_API_KEY", "sensitive-api-key")
    monkeypatch.setenv("SCHWAB_SECRET_KEY", "sensitive-app-secret")
    monkeypatch.setenv("SCHWAB_TOKEN_PATH", "sensitive/relative-token-path")

    code, out, err = run_probe_command(capsys)

    assert code == 1
    assert out == '{"code":"probe_settings_invalid","status":"error"}\n'
    assert err == ""
    assert "sensitive" not in out
    assert "token-path" not in out


def test_probe_command_bounds_sdk_import_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """A missing SDK is the last stage that proves no token read occurred."""
    install_dependencies(monkeypatch)
    fake_schwab = ModuleType("schwab")
    monkeypatch.setitem(sys.modules, "schwab", fake_schwab)
    monkeypatch.setitem(sys.modules, "schwab.auth", None)

    code, out, err = run_probe_command(capsys)

    assert code == 1
    assert out == '{"code":"probe_sdk_import_failed","status":"error"}\n'
    assert err == ""


@pytest.mark.parametrize(
    ("reason", "expected_code"),
    [
        ("token_invalid", "probe_token_invalid"),
        ("client_construction_failed", "probe_client_construction_failed"),
        ("quote_failed", "probe_quote_failed"),
        ("state_invalid", "probe_state_invalid"),
    ],
)
def test_probe_command_maps_each_probe_reason_to_its_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    reason: str,
    expected_code: str,
) -> None:
    """Each of these proves a token read was reached, so each needs its own code."""

    def fail(*_args, **_kwargs):
        raise GatewayCredentialProbeError(reason)

    install_dependencies(monkeypatch, run_probe=fail)
    install_fake_sdk(monkeypatch, object())

    code, out, err = run_probe_command(capsys)

    assert code == 1
    assert out == '{"code":"%s","status":"error"}\n' % expected_code
    assert err == ""


def test_probe_command_bounds_an_unexpected_probe_exception(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    def fail(*_args, **_kwargs):
        raise RuntimeError("sensitive-unexpected-detail")

    install_dependencies(monkeypatch, run_probe=fail)
    install_fake_sdk(monkeypatch, object())

    code, out, err = run_probe_command(capsys)

    assert code == 1
    assert out == '{"code":"probe_state_invalid","status":"error"}\n'
    assert err == ""


def test_probe_command_bounds_result_serialization_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    install_dependencies(monkeypatch)
    install_fake_sdk(monkeypatch, object())

    code, out, err = run_probe_command(capsys)

    assert code == 1
    assert out == '{"code":"probe_state_invalid","status":"error"}\n'
    assert err == ""


def test_probe_command_failure_codes_are_a_closed_set() -> None:
    """No code may be derived from an exception message or a token state value."""
    assert probe_command.PROBE_FAILURE_CODES == {
        "probe_import_failed",
        "probe_settings_invalid",
        "probe_sdk_import_failed",
        "probe_token_invalid",
        "probe_client_construction_failed",
        "probe_quote_failed",
        "probe_state_invalid",
    }
    assert set(probe_command.PROBE_NO_TOKEN_READ_CODES).isdisjoint(
        probe_command.PROBE_TOKEN_READ_CODES
    )
    assert set(probe_command._REASON_CODES.values()) == set(
        probe_command.PROBE_TOKEN_READ_CODES
    )


def test_probe_command_emits_only_bounded_success_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    fake_settings = object()
    fake_factory = object()
    fake_schwab = ModuleType("schwab")
    fake_auth = ModuleType("schwab.auth")
    fake_auth.client_from_access_functions = fake_factory
    fake_schwab.auth = fake_auth
    setup_logging = MagicMock()
    run_probe = MagicMock(
        return_value=GatewayCredentialProbeResult(
            status="ok",
            token_state="ready",
            quote_count=1,
        )
    )
    monkeypatch.setattr(
        probe_command,
        "_load_runtime_dependencies",
        lambda: (
            setup_logging,
            lambda: fake_settings,
            run_probe,
            GatewayCredentialProbeError,
        ),
    )
    monkeypatch.setitem(sys.modules, "schwab", fake_schwab)
    monkeypatch.setitem(sys.modules, "schwab.auth", fake_auth)

    probe_command.main(
        [
            "--authorize-real-credential-read",
            "--confirm-single-token-writer",
            "--confirm-no-deployment",
        ]
    )

    captured = capsys.readouterr()
    assert captured.out == '{"quote_count":1,"status":"ok","token_state":"ready"}\n'
    assert captured.err == ""
    setup_logging.assert_called_once_with("CRITICAL", json_output=True)
    run_probe.assert_called_once_with(fake_settings, fake_factory)
