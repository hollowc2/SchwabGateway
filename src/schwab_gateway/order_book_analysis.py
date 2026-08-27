"""Derive reproducible research metrics from an immutable order-book capture."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import statistics
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from schwab_gateway_sdk.models import OrderBookSnapshotV1

UTC = dt.timezone.utc


class OrderBookAnalysisError(RuntimeError):
    """A capture could not be verified or derived without ambiguity."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _capture_file(manifest_path: Path, relative_name: Any) -> Path:
    if not isinstance(relative_name, str) or not relative_name:
        raise OrderBookAnalysisError("capture manifest has an invalid evidence path")
    relative = Path(relative_name)
    if relative.is_absolute() or ".." in relative.parts:
        raise OrderBookAnalysisError("capture evidence path escapes its run directory")
    resolved = manifest_path.parent / relative
    if not resolved.is_file():
        raise OrderBookAnalysisError("capture evidence file is missing")
    return resolved


def load_verified_capture(
    manifest_path: Path,
) -> tuple[dict[str, Any], list[OrderBookSnapshotV1]]:
    """Load normalized snapshots only after their source hash is verified."""

    manifest_path = manifest_path.resolve()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise OrderBookAnalysisError("capture manifest is unreadable") from exc
    if manifest.get("purpose") != "order_book_research":
        raise OrderBookAnalysisError("manifest is not an order-book research capture")
    normalized_path = _capture_file(manifest_path, manifest.get("normalized_path"))
    if sha256_file(normalized_path) != manifest.get("normalized_sha256"):
        raise OrderBookAnalysisError("normalized evidence hash does not match manifest")

    snapshots: list[OrderBookSnapshotV1] = []
    try:
        with normalized_path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if line.strip():
                    try:
                        snapshots.append(OrderBookSnapshotV1.model_validate_json(line))
                    except ValueError as exc:
                        raise OrderBookAnalysisError(
                            f"normalized snapshot line {line_number} is invalid"
                        ) from exc
    except OSError as exc:
        raise OrderBookAnalysisError("normalized evidence is unreadable") from exc
    if len(snapshots) != manifest.get("normalized_snapshot_count"):
        raise OrderBookAnalysisError("normalized evidence count does not match manifest")
    return manifest, snapshots


def _pearson(pairs: Iterable[tuple[float, float]]) -> float | None:
    usable = [(x, y) for x, y in pairs if math.isfinite(x) and math.isfinite(y)]
    if len(usable) < 3:
        return None
    xs, ys = zip(*usable, strict=True)
    if len(set(xs)) < 2 or len(set(ys)) < 2:
        return None
    return statistics.correlation(xs, ys)


def derive_metrics(
    snapshots: list[OrderBookSnapshotV1], *, depth_levels: int = 10
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Compute per-snapshot liquidity metrics and descriptive correlations."""

    if not 1 <= depth_levels <= 100:
        raise ValueError("depth levels must be between 1 and 100")
    previous: dict[tuple[str, str, int], dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []

    for snapshot in snapshots:
        bid_levels = snapshot.bids[:depth_levels]
        ask_levels = snapshot.asks[:depth_levels]
        best_bid = bid_levels[0].price if bid_levels else None
        best_ask = ask_levels[0].price if ask_levels else None
        spread = (
            best_ask - best_bid
            if best_bid is not None and best_ask is not None
            else None
        )
        midpoint = (
            (best_bid + best_ask) / 2
            if best_bid is not None and best_ask is not None
            else None
        )
        bid_depth = sum(level.total_size for level in bid_levels)
        ask_depth = sum(level.total_size for level in ask_levels)
        total_depth = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total_depth if total_depth else None
        top_bid_size = bid_levels[0].total_size if bid_levels else 0
        top_ask_size = ask_levels[0].total_size if ask_levels else 0
        top_total = top_bid_size + top_ask_size
        microprice = (
            (best_ask * top_bid_size + best_bid * top_ask_size) / top_total
            if best_bid is not None and best_ask is not None and top_total
            else None
        )
        depth_map = {
            (side, level.price): level.total_size
            for side, levels in (("bid", bid_levels), ("ask", ask_levels))
            for level in levels
        }
        key = (snapshot.symbol, snapshot.venue, snapshot.continuity_epoch)
        prior = previous.get(key)
        inferred_added = inferred_removed = 0
        interval_seconds = midpoint_change = midpoint_return_bps = None
        if prior is not None:
            prices = set(depth_map) | set(prior["depth_map"])
            deltas = [
                depth_map.get(price, 0) - prior["depth_map"].get(price, 0)
                for price in prices
            ]
            inferred_added = sum(delta for delta in deltas if delta > 0)
            inferred_removed = -sum(delta for delta in deltas if delta < 0)
            interval_seconds = (
                snapshot.gateway_received_at - prior["received_at"]
            ).total_seconds()
            prior_midpoint = prior["midpoint"]
            if midpoint is not None and prior_midpoint is not None:
                midpoint_change = midpoint - prior_midpoint
                if prior_midpoint:
                    midpoint_return_bps = midpoint_change / prior_midpoint * 10_000
        add_rate = (
            inferred_added / interval_seconds
            if interval_seconds is not None and interval_seconds > 0
            else None
        )
        removal_rate = (
            inferred_removed / interval_seconds
            if interval_seconds is not None and interval_seconds > 0
            else None
        )
        row = {
            "schema_version": "1.0",
            "symbol": snapshot.symbol,
            "venue": snapshot.venue,
            "is_consolidated": False,
            "event_timestamp": (
                snapshot.event_timestamp.isoformat()
                if snapshot.event_timestamp is not None
                else None
            ),
            "gateway_received_at": snapshot.gateway_received_at.isoformat(),
            "connection_id": snapshot.connection_id,
            "continuity_epoch": snapshot.continuity_epoch,
            "depth_levels": depth_levels,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "midpoint": midpoint,
            "microprice": microprice,
            "bid_depth_size": bid_depth,
            "ask_depth_size": ask_depth,
            "total_depth_size": total_depth,
            "book_imbalance": imbalance,
            "snapshot_interval_seconds": interval_seconds,
            "inferred_added_size": inferred_added,
            "inferred_removed_size": inferred_removed,
            "inferred_add_rate_per_second": add_rate,
            "inferred_removal_rate_per_second": removal_rate,
            "midpoint_change": midpoint_change,
            "midpoint_return_bps": midpoint_return_bps,
            "inference_note": "depth changes inferred from adjacent snapshots; not order events",
        }
        rows.append(row)
        previous[key] = {
            "depth_map": depth_map,
            "received_at": snapshot.gateway_received_at,
            "midpoint": midpoint,
        }

    next_pairs = list(zip(rows, rows[1:], strict=False))
    summary = {
        "row_count": len(rows),
        "imbalance_vs_next_midpoint_return_pearson": _pearson(
            (
                (current["book_imbalance"], following["midpoint_return_bps"])
                for current, following in next_pairs
                if current["symbol"] == following["symbol"]
                and current["venue"] == following["venue"]
                and current["continuity_epoch"] == following["continuity_epoch"]
                and current["book_imbalance"] is not None
                and following["midpoint_return_bps"] is not None
            )
        ),
        "depth_vs_next_absolute_midpoint_move_pearson": _pearson(
            (
                (current["total_depth_size"], abs(following["midpoint_change"]))
                for current, following in next_pairs
                if current["symbol"] == following["symbol"]
                and current["venue"] == following["venue"]
                and current["continuity_epoch"] == following["continuity_epoch"]
                and following["midpoint_change"] is not None
            )
        ),
        "correlations_are_descriptive_not_causal": True,
    }
    return rows, summary


def write_derived_dataset(
    capture_manifest_path: Path,
    output_directory: Path,
    *,
    depth_levels: int = 10,
    clock: Any = None,
) -> Path:
    """Write a new non-overwriting derived dataset and provenance manifest."""

    capture_manifest_path = capture_manifest_path.resolve()
    output_directory = output_directory.resolve()
    manifest, snapshots = load_verified_capture(capture_manifest_path)
    rows, summary = derive_metrics(snapshots, depth_levels=depth_levels)
    output_directory.mkdir(parents=True, exist_ok=False)
    metrics_path = output_directory / "order_book_metrics.ndjson"
    derived_manifest_path = output_directory / "manifest.json"
    with metrics_path.open("x", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(metrics_path, 0o600)
    now = (clock or (lambda: dt.datetime.now(UTC)))().astimezone(UTC)
    derived_manifest = {
        "schema_version": "1.0",
        "purpose": "order_book_derived_research",
        "provider": manifest["provider"],
        "venue": manifest["venue"],
        "is_consolidated": False,
        "symbols": manifest["symbols"],
        "generated_at": now.isoformat(),
        "source_manifest_path": os.path.relpath(
            capture_manifest_path, output_directory
        ),
        "source_manifest_sha256": sha256_file(capture_manifest_path),
        "source_normalized_sha256": manifest["normalized_sha256"],
        "metrics_path": metrics_path.name,
        "metrics_sha256": sha256_file(metrics_path),
        "depth_levels": depth_levels,
        "inferred_event_warning": (
            "added/removed size is inferred from adjacent snapshots, not exchange order events"
        ),
        "summary": summary,
    }
    with derived_manifest_path.open("x", encoding="utf-8") as handle:
        json.dump(derived_manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(derived_manifest_path, 0o600)
    return derived_manifest_path
