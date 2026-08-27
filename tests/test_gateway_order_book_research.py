"""Tests for traceable derivation, catalog validation, and retention planning."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest
from schwab_gateway_sdk.models import OrderBookLevelV1, OrderBookSnapshotV1

from schwab_gateway.order_book_analysis import (
    OrderBookAnalysisError,
    derive_metrics,
    load_verified_capture,
    write_derived_dataset,
)
from schwab_gateway.order_book_capture import (
    OrderBookCaptureRequest,
    OrderBookResearchRecorder,
)
from schwab_gateway.order_book_catalog import build_catalog_document, write_catalog

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 8, 27, 17, 0, tzinfo=UTC)


def _snapshot(
    second: int,
    *,
    bid_price: float,
    ask_price: float,
    bid_size: int,
    ask_size: int,
) -> OrderBookSnapshotV1:
    received_at = NOW + dt.timedelta(seconds=second)
    return OrderBookSnapshotV1(
        symbol="AAPL",
        venue="NASDAQ",
        service="NASDAQ_BOOK",
        gateway_received_at=received_at,
        event_timestamp=received_at,
        bids=(
            OrderBookLevelV1(
                price=bid_price,
                total_size=bid_size,
                participant_count=0,
            ),
        ),
        asks=(
            OrderBookLevelV1(
                price=ask_price,
                total_size=ask_size,
                participant_count=0,
            ),
        ),
    )


def _capture(tmp_path: Path) -> Path:
    times = iter((NOW, NOW + dt.timedelta(seconds=4)))
    request = OrderBookCaptureRequest(
        venue="NASDAQ",
        symbols=("AAPL",),
        duration_seconds=4,
        output_root=tmp_path,
    )
    recorder = OrderBookResearchRecorder(request, clock=lambda: next(times))
    recorder.start()
    recorder.record_raw_frame(
        '{"data":[{"service":"NASDAQ_BOOK","content":[]}]}', NOW
    )
    recorder.record_snapshot(
        _snapshot(0, bid_price=100.0, ask_price=100.2, bid_size=10, ask_size=5)
    )
    recorder.record_snapshot(
        _snapshot(1, bid_price=100.1, ask_price=100.2, bid_size=14, ask_size=8)
    )
    recorder.record_snapshot(
        _snapshot(2, bid_price=100.0, ask_price=100.3, bid_size=7, ask_size=12)
    )
    return recorder.finalize(termination_reason="completed")


def test_derivation_is_traceable_and_labels_snapshot_delta_inference(
    tmp_path: Path,
) -> None:
    capture_manifest = _capture(tmp_path)
    output = tmp_path / "derived_v1"
    derived_manifest_path = write_derived_dataset(
        capture_manifest,
        output,
        depth_levels=1,
        clock=lambda: NOW + dt.timedelta(minutes=1),
    )
    derived_manifest = json.loads(derived_manifest_path.read_text())
    rows = [
        json.loads(line)
        for line in (output / "order_book_metrics.ndjson").read_text().splitlines()
    ]

    assert derived_manifest["source_normalized_sha256"] == json.loads(
        capture_manifest.read_text()
    )["normalized_sha256"]
    assert derived_manifest["summary"]["row_count"] == 3
    assert derived_manifest["summary"]["correlations_are_descriptive_not_causal"] is True
    assert rows[0]["spread"] == pytest.approx(0.2)
    assert rows[0]["book_imbalance"] == pytest.approx(1 / 3)
    assert rows[0]["microprice"] == pytest.approx((100.2 * 10 + 100.0 * 5) / 15)
    assert rows[1]["inferred_added_size"] == 17
    assert rows[1]["inferred_removed_size"] == 10
    assert "not order events" in rows[1]["inference_note"]


def test_derivation_refuses_tampered_normalized_evidence(tmp_path: Path) -> None:
    capture_manifest = _capture(tmp_path)
    manifest = json.loads(capture_manifest.read_text())
    normalized = capture_manifest.parent / manifest["normalized_path"]
    normalized.write_text(normalized.read_text() + "{}\n")

    with pytest.raises(OrderBookAnalysisError, match="hash does not match"):
        load_verified_capture(capture_manifest)


def test_catalog_verifies_evidence_and_only_plans_archive(tmp_path: Path) -> None:
    capture_manifest = _capture(tmp_path)
    catalog = build_catalog_document(
        tmp_path,
        archive_after_days=30,
        now=NOW + dt.timedelta(days=31),
    )

    assert catalog["capture_count"] == 1
    assert catalog["verified_capture_count"] == 1
    assert catalog["retention_policy"]["raw_deletion"] == "never_automatic"
    assert catalog["retention_policy"]["this_catalog_mutates_captures"] is False
    assert catalog["captures"][0]["retention_action"] == "archive_copy_then_verify"
    before = capture_manifest.read_bytes()
    output = write_catalog(
        tmp_path,
        tmp_path / "catalog.json",
        archive_after_days=30,
        now=NOW + dt.timedelta(days=31),
    )
    assert output.is_file()
    assert capture_manifest.read_bytes() == before


def test_metric_deltas_reset_at_continuity_epoch_boundaries() -> None:
    first = _snapshot(0, bid_price=100, ask_price=101, bid_size=10, ask_size=10)
    second = _snapshot(1, bid_price=99, ask_price=102, bid_size=1, ask_size=1).model_copy(
        update={"connection_id": 2, "continuity_epoch": 2}
    )
    rows, _summary = derive_metrics([first, second], depth_levels=1)

    assert rows[1]["snapshot_interval_seconds"] is None
    assert rows[1]["inferred_added_size"] == 0
    assert rows[1]["inferred_removed_size"] == 0
