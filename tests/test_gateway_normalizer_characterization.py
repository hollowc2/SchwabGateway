"""Pin the complete normalized output for mixed Schwab payloads.

The golden file is the normalizers' own output, captured before refactoring them. Set
``REGENERATE_NORMALIZER_GOLDEN=1`` only when a contract change is intended.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

from schwab_gateway.upstream import (
    normalize_schwab_history,
    normalize_schwab_movers,
    normalize_schwab_option_chain,
    normalize_schwab_session_history,
)

GOLDEN = Path(__file__).parent / "fixtures" / "normalizer_characterization.json"
RECEIVED_AT = dt.datetime(2026, 9, 29, 15, 0, tzinfo=dt.UTC)
EXPIRATION = dt.date(2026, 9, 29)


def _millis(at: dt.datetime) -> int:
    return int(at.timestamp() * 1000)


def _contract(symbol: str, **overrides: Any) -> dict[str, Any]:
    contract: dict[str, Any] = {
        "symbol": symbol,
        "bid": 1.0,
        "ask": 1.2,
        "mark": 1.1,
        "last": 1.05,
        "totalVolume": 10,
        "openInterest": 20,
        "volatility": 18.5,
        "delta": 0.5,
        "gamma": 0.01,
        "theta": -0.2,
        "vega": 0.1,
        "rho": 0.01,
        "bidSize": 3,
        "askSize": 4,
        "intrinsicValue": 2.0,
        "timeValue": 0.5,
        "inTheMoney": True,
        "daysToExpiration": 0,
        "multiplier": 100,
        "theoreticalOptionValue": 1.1,
        "quoteTimeInLong": _millis(RECEIVED_AT - dt.timedelta(seconds=2)),
        "tradeTimeInLong": _millis(RECEIVED_AT - dt.timedelta(seconds=5)),
    }
    contract.update(overrides)
    return contract


def _option_chain_payload() -> dict[str, Any]:
    key = f"{EXPIRATION}:0"
    old = _millis(RECEIVED_AT - dt.timedelta(minutes=10))
    return {
        "underlyingPrice": 5000.5,
        "underlying": {"quoteTime": _millis(RECEIVED_AT - dt.timedelta(seconds=1))},
        "callExpDateMap": {
            key: {
                "4990.0": [_contract("SPXW C4990")],
                "5000.0": [_contract("SPXW C5000", bid=2.5, ask=2.0)],
                "5010.0": [
                    _contract(
                        "SPXW C5010",
                        intrinsicValue=-10.0,
                        timeValue=-0.3,
                        theoreticalOptionValue=-0.1,
                        delta=-999.0,
                    )
                ],
            },
            "2026-10-02:3": {"5000.0": [_contract("SPXW C5000 OCT")]},
        },
        "putExpDateMap": {
            key: {
                "4990.0": [_contract("SPXW P4990", quoteTimeInLong=old, tradeTimeInLong=old)],
                "5000.0": [_contract("SPXW P5000", quoteTimeInLong=None, tradeTimeInLong=0)],
            }
        },
    }


def _candles(start: dt.datetime, count: int, step: dt.timedelta) -> list[dict[str, Any]]:
    return [
        {
            "datetime": _millis(start + step * index),
            "open": 100 + index,
            "high": 101 + index,
            "low": 99 + index,
            "close": 100.5 + index,
            "volume": 1000 + index,
        }
        for index in range(count)
    ]


def _outputs() -> dict[str, Any]:
    chain = normalize_schwab_option_chain(
        "SPX",
        _option_chain_payload(),
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=300,
    )
    movers = normalize_schwab_movers(
        "$SPX",
        "up",
        [
            {"symbol": "AAPL", "changePercent": 1.5, "change": 3, "lastPrice": 200, "volume": 9},
            {"ticker": "MSFT", "netPercentChange": 0.5, "netChange": 2, "last": 400},
            {"symbol": "TSLA", "totalVolume": 7, "volume": 99},
            {"symbol": ""},
            "not-a-mover",
            {"symbol": "BAD", "totalVolume": -5},
        ],
        received_at=RECEIVED_AT,
    )
    minute = normalize_schwab_history(
        "AAPL",
        "minute",
        _candles(RECEIVED_AT - dt.timedelta(days=3), 6, dt.timedelta(hours=14)),
        received_at=RECEIVED_AT,
        stale_after_seconds=900,
        days_back=2,
    )
    daily = normalize_schwab_history(
        "AAPL",
        "daily",
        _candles(RECEIVED_AT - dt.timedelta(days=6), 5, dt.timedelta(days=1)),
        received_at=RECEIVED_AT,
        stale_after_seconds=86400,
        days_back=3,
    )
    day = dt.date(2026, 9, 28)
    full_day = _candles(
        dt.datetime(2026, 9, 28, 8, 0, tzinfo=dt.UTC), 20, dt.timedelta(minutes=45)
    ) + [{"datetime": "bad"}]
    sessions = {
        session: normalize_schwab_session_history(
            "AAPL",
            day,
            session,
            full_day,
            received_at=RECEIVED_AT,
            stale_after_seconds=86400,
        )
        for session in ("regular", "extended")
    }
    return {
        "option_chain": chain.model_dump(mode="json"),
        "movers": movers.model_dump(mode="json"),
        "minute_history": minute.model_dump(mode="json"),
        "daily_history": daily.model_dump(mode="json"),
        "session_history": {
            name: value.model_dump(mode="json") for name, value in sessions.items()
        },
    }


def test_normalizers_match_the_pinned_golden_output() -> None:
    outputs = json.loads(json.dumps(_outputs(), sort_keys=True))
    if os.environ.get("REGENERATE_NORMALIZER_GOLDEN") == "1":
        GOLDEN.write_text(json.dumps(outputs, indent=2, sort_keys=True) + "\n")
    assert outputs == json.loads(GOLDEN.read_text())
