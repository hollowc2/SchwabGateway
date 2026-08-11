from schwab_gateway.redaction import REDACTED, redact


def test_redaction_removes_nested_credentials_and_account_identifiers() -> None:
    payload = {
        "token": {
            "access_token": "access-value",
            "refresh_token": "refresh-value",
        },
        "accountNumber": "account-value",
        "orders": [{"hashValue": "hash-value", "status": "FILLED"}],
    }

    result = redact(payload)

    assert result == {
        "token": {
            "access_token": REDACTED,
            "refresh_token": REDACTED,
        },
        "accountNumber": REDACTED,
        "orders": [{"hashValue": REDACTED, "status": "FILLED"}],
    }
    assert "access-value" not in repr(result)
    assert "account-value" not in repr(result)
