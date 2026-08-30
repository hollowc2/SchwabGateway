from __future__ import annotations

import json
import re
import stat
from pathlib import Path

import pytest

from schwab_gateway import issue_keys
from schwab_gateway.auth import InternalKeyAuthenticator, PriorityClass


def args(output: Path, application_id: str = "new-consumer") -> list[str]:
    return [
        "--output",
        str(output),
        "--application-id",
        application_id,
        "--capability",
        "market_data:read",
        "--priority",
        "background",
    ]


def test_cli_writes_private_digest_only_configuration_driven_key(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    output = tmp_path / "keys.json"
    issue_keys.main(args(output))
    printed = capsys.readouterr().out
    payload = json.loads(output.read_text())
    plaintext = re.search(r"^new-consumer: (\S+)$", printed, re.MULTILINE).group(1)

    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert plaintext not in output.read_text()
    assert payload["clients"] == [
        {
            "id": "new-consumer",
            "key_sha256": issue_keys.key_digest(plaintext),
            "capabilities": ["market_data:read"],
            "priority_class": "background",
        }
    ]
    assert InternalKeyAuthenticator.from_file(output).authenticate(plaintext).client_id == (
        "new-consumer"
    )


def test_cli_requires_explicit_capability_and_priority(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        issue_keys.main(
            ["--output", str(tmp_path / "keys.json"), "--application-id", "consumer"]
        )
    assert exc.value.code == 2


def test_append_preserves_existing_digest_and_rejects_duplicate_id(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    existing = tmp_path / "existing.json"
    document, original_key = issue_keys.build_keys_document(
        "butterfly-guy", "market_data:read", PriorityClass.PROTECTED
    )
    issue_keys.write_private_json(existing, document)
    output = tmp_path / "output.json"
    issue_keys.main(["--existing-input", str(existing), *args(output, "afterhours-lab")])
    capsys.readouterr()

    payload = json.loads(output.read_text())
    assert payload["clients"][0] == document["clients"][0]
    assert InternalKeyAuthenticator.from_file(output).authenticate(original_key).client_id == (
        "butterfly-guy"
    )

    duplicate = tmp_path / "duplicate.json"
    with pytest.raises(SystemExit) as exc:
        issue_keys.main(
            ["--existing-input", str(existing), *args(duplicate, "butterfly-guy")]
        )
    assert exc.value.code == 2
    assert not duplicate.exists()


def test_cli_never_overwrites_existing_output(tmp_path: Path) -> None:
    output = tmp_path / "keys.json"
    output.write_text("preserve")
    with pytest.raises(SystemExit):
        issue_keys.main(args(output))
    assert output.read_text() == "preserve"


def test_cli_can_write_plaintext_to_private_file_without_printing_it(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    output = tmp_path / "keys.json"
    plaintext_output = tmp_path / "load-test.key"

    issue_keys.main(
        [*args(output), "--plaintext-output", str(plaintext_output)]
    )

    printed = capsys.readouterr().out
    plaintext = plaintext_output.read_text().strip()
    assert plaintext
    assert plaintext not in printed
    assert "value was not printed" in printed
    assert stat.S_IMODE(plaintext_output.stat().st_mode) == 0o600
    assert plaintext not in output.read_text()
    assert InternalKeyAuthenticator.from_file(output).authenticate(plaintext).client_id == (
        "new-consumer"
    )


def test_cli_never_overwrites_plaintext_output(tmp_path: Path) -> None:
    output = tmp_path / "keys.json"
    plaintext_output = tmp_path / "load-test.key"
    plaintext_output.write_text("preserve")

    with pytest.raises(SystemExit):
        issue_keys.main(
            [*args(output), "--plaintext-output", str(plaintext_output)]
        )

    assert not output.exists()
    assert plaintext_output.read_text() == "preserve"
