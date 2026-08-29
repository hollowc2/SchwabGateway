"""Validate and catalog order-book evidence without deleting or rewriting captures."""

from __future__ import annotations

import datetime as dt
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from schwab_gateway.order_book_analysis import sha256_file

UTC = dt.timezone.utc


def _safe_capture_path(manifest_path: Path, value: Any) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        return None
    return manifest_path.parent / relative


def _evidence_status(manifest_path: Path, manifest: dict[str, Any]) -> tuple[str, list[str]]:
    failures: list[str] = []
    for path_field, hash_field in (
        ("raw_path", "raw_sha256"),
        ("normalized_path", "normalized_sha256"),
        ("connection_events_path", "connection_events_sha256"),
    ):
        if path_field not in manifest and path_field == "connection_events_path":
            continue
        evidence_path = _safe_capture_path(manifest_path, manifest.get(path_field))
        if evidence_path is None or not evidence_path.is_file():
            failures.append(f"{path_field}:missing_or_unsafe")
        elif sha256_file(evidence_path) != manifest.get(hash_field):
            failures.append(f"{path_field}:hash_mismatch")
    return ("verified" if not failures else "invalid"), failures


def build_catalog_document(
    evidence_root: Path,
    *,
    archive_after_days: int = 30,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Scan capture manifests and produce a non-destructive retention plan."""

    if archive_after_days < 1:
        raise ValueError("archive-after days must be positive")
    root = evidence_root.resolve()
    if not root.is_dir():
        raise ValueError("evidence root must be an existing directory")
    generated_at = (now or dt.datetime.now(UTC)).astimezone(UTC)
    entries: list[dict[str, Any]] = []
    for manifest_path in sorted(root.rglob("manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if manifest.get("purpose") != "order_book_research":
            continue
        status, failures = _evidence_status(manifest_path, manifest)
        try:
            ended_at = dt.datetime.fromisoformat(manifest["ended_at"])
            age_days = max((generated_at - ended_at.astimezone(UTC)).total_seconds(), 0) / 86400
        except (KeyError, TypeError, ValueError):
            age_days = None
            status = "invalid"
            failures.append("ended_at:invalid")
        retention_action = (
            "archive_copy_then_verify"
            if age_days is not None and age_days >= archive_after_days
            else "keep_hot"
        )
        entries.append(
            {
                "capture_id": manifest_path.parent.name,
                "manifest_path": os.path.relpath(manifest_path, root),
                "manifest_sha256": sha256_file(manifest_path),
                "provider": manifest.get("provider"),
                "venue": manifest.get("venue"),
                "is_consolidated": manifest.get("is_consolidated"),
                "symbols": manifest.get("symbols"),
                "started_at": manifest.get("started_at"),
                "ended_at": manifest.get("ended_at"),
                "raw_frame_count": manifest.get("raw_frame_count"),
                "normalized_snapshot_count": manifest.get("normalized_snapshot_count"),
                "evidence_status": status,
                "validation_failures": failures,
                "age_days": age_days,
                "retention_action": retention_action,
            }
        )
    return {
        "schema_version": "1.0",
        "purpose": "order_book_evidence_catalog",
        "generated_at": generated_at.isoformat(),
        "evidence_root": str(root),
        "capture_count": len(entries),
        "verified_capture_count": sum(
            entry["evidence_status"] == "verified" for entry in entries
        ),
        "retention_policy": {
            "archive_after_days": archive_after_days,
            "raw_deletion": "never_automatic",
            "archive_rule": "copy, verify hashes, then request separate deletion approval",
            "this_catalog_mutates_captures": False,
        },
        "captures": entries,
    }


def write_catalog(
    evidence_root: Path,
    output_path: Path,
    *,
    archive_after_days: int = 30,
    now: dt.datetime | None = None,
) -> Path:
    """Atomically refresh the mutable catalog index; capture files remain untouched."""

    document = build_catalog_document(
        evidence_root, archive_after_days=archive_after_days, now=now
    )
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_name, 0o600)
        os.replace(temporary_name, output_path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
    return output_path
