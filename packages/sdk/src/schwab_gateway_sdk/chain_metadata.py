"""Pure, bounded option-chain metadata extraction.

Both the gateway's chain upstream and the consumer-side shadow comparator must derive
the same summary from the same raw Schwab payload, so the derivation lives here rather
than inside either of them. It never returns, counts, or copies contract rows.

A payload carrying neither ``callExpDateMap`` nor ``putExpDateMap`` (or carrying one or
both as something other than a dict, e.g. absent, ``null``, or empty) is treated as a
legitimate zero-result chain, exactly like the two live parsers (``data.chain_utils
.iter_chain_options`` and ``data.collector.OptionChainCollector._parse_chain_response``,
both of which use ``payload.get(map_key, {})`` and never raise on a missing map). This
used to raise ``ValueError`` here as a guard against unparseable payloads, but that guard
had a real cost: the gateway's ``/v1/chain`` upstream converted the raise into a 502, and
the shadow comparator logged it as a "parsing" discrepancy, for a shape (e.g. an
after-hours or halted-symbol response with no expiration maps) that the collector already
handles as zero rows without complaint. Only a payload that is not a dict at all is still
rejected, since that is a shape none of the three parsers can walk.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

UTC = dt.timezone.utc


@dataclass(frozen=True)
class ChainMetadataFields:
    """The bounded summary shared by the gateway upstream and the shadow comparator.

    ``strike_count`` counts distinct strikes that carry at least one contract, across
    both maps. A strike whose option list is empty is not counted, because neither live
    parser can produce anything from it: ``iter_chain_options`` skips it (``if options:``)
    and ``_parse_chain_response`` writes no row for it. That keeps the count fields
    mutually consistent — a strike is present here only if it contributed contracts.
    """

    underlying_price: float | None
    call_contract_count: int
    put_contract_count: int
    strike_count: int
    event_timestamp: dt.datetime | None
    data_quality_flags: tuple[str, ...]


def _epoch_millis(payload: dict[str, Any], name: str) -> dt.datetime | None:
    value = payload.get(name)
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return dt.datetime.fromtimestamp(millis / 1000, tz=UTC)


def _underlying_event_time(payload: dict[str, Any]) -> dt.datetime | None:
    underlying = payload.get("underlying")
    if not isinstance(underlying, dict):
        return None
    candidates = [
        value
        for name in ("quoteTime", "tradeTime")
        if (value := _epoch_millis(underlying, name)) is not None
    ]
    if not candidates:
        return None
    return max(candidates)


def _count_expiration(exp_map: Any, expiration: dt.date) -> tuple[int, frozenset[float]]:
    """Count contracts and contract-carrying strikes for one expiration.

    Mirrors the collector's expiration-key filter, and mirrors both live parsers in
    ignoring a strike whose option list is empty.
    """
    if not isinstance(exp_map, dict):
        return 0, frozenset()
    contracts = 0
    strikes: set[float] = set()
    for exp_key, strike_map in exp_map.items():
        if not isinstance(exp_key, str) or str(expiration) not in exp_key:
            continue
        if not isinstance(strike_map, dict):
            continue
        for strike_str, options in strike_map.items():
            if not isinstance(options, list):
                continue
            try:
                strike = float(strike_str)
            except (TypeError, ValueError):
                continue
            if not options:
                continue
            strikes.add(strike)
            contracts += len(options)
    return contracts, frozenset(strikes)


def extract_chain_metadata(
    payload: dict[str, Any],
    expiration: dt.date,
) -> ChainMetadataFields:
    """Summarize a raw Schwab option-chain response for one expiration.

    Raises ``ValueError`` only when the payload itself is not an object; that is the one
    shape none of the three chain parsers can walk. A payload missing one or both
    expiration maps is not an error here, matching both live parsers.
    """
    if not isinstance(payload, dict):
        raise ValueError("option chain payload must be an object")
    call_map = payload.get("callExpDateMap")
    put_map = payload.get("putExpDateMap")

    call_contracts, call_strikes = _count_expiration(call_map, expiration)
    put_contracts, put_strikes = _count_expiration(put_map, expiration)

    raw_price = payload.get("underlyingPrice")
    try:
        underlying_price = None if raw_price is None else float(raw_price)
    except (TypeError, ValueError):
        underlying_price = None

    event_timestamp = _underlying_event_time(payload)

    flags: list[str] = []
    if underlying_price is None:
        flags.append("missing_underlying_price")
    if call_contracts == 0:
        flags.append("missing_call_contracts")
    if put_contracts == 0:
        flags.append("missing_put_contracts")
    if event_timestamp is None:
        flags.append("missing_event_timestamp")

    return ChainMetadataFields(
        underlying_price=underlying_price,
        call_contract_count=call_contracts,
        put_contract_count=put_contracts,
        strike_count=len(call_strikes | put_strikes),
        event_timestamp=event_timestamp,
        data_quality_flags=tuple(flags),
    )
