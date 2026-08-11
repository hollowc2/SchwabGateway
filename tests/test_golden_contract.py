from __future__ import annotations

import json
from pathlib import Path

import pytest
from schwab_gateway_sdk.models import (
    ChainMetadataResponseV1,
    GatewayErrorV1,
    GatewayHealthV1,
    GatewayReadinessV1,
    QuoteResponseV1,
    SpotResponseV1,
)

from schwab_gateway import api


@pytest.fixture(scope="module")
def golden() -> dict:
    return json.loads(Path("tests/fixtures/schwab_gateway_http_v1.json").read_text())


def test_success_fixtures_are_exactly_accepted_by_v1_models(golden: dict) -> None:
    models = {
        "health": GatewayHealthV1,
        "ready": GatewayReadinessV1,
        "quotes": QuoteResponseV1,
        "spot": SpotResponseV1,
        "chain": ChainMetadataResponseV1,
    }
    for name, model in models.items():
        body = golden["success"][name]["body"]
        assert model.model_validate(body).model_dump(mode="json") == body


@pytest.mark.parametrize(
    "name",
    [
        "invalid_quotes_missing",
        "invalid_spot_missing",
        "invalid_chain_expiration",
        "gateway_not_ready",
        "gateway_capacity_exceeded",
        "quote_upstream_timeout",
        "quote_upstream_unavailable",
        "quote_upstream_malformed",
        "market_data_upstream_timeout",
        "market_data_upstream_unavailable",
        "market_data_upstream_malformed",
    ],
)
def test_api_error_serializer_matches_golden_fixture(golden: dict, name: str) -> None:
    expected = golden["bounded_errors"][name]
    detail = expected["body"]["error"]
    response = api._error(detail["code"], detail["message"], expected["status"])
    assert response.status == expected["status"]
    assert json.loads(response.body) == expected["body"]
    GatewayErrorV1.model_validate_json(response.body)
