"""Durable, owner-only evidence files shared by the research capture and export tools."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

_HASH_CHUNK_BYTES = 1024 * 1024


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_private_json(path: Path, document: Any) -> None:
    """Create ``path`` exclusively as sorted, indented JSON, fsync it, then make it 0600."""
    with path.open("x", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def write_private_ndjson(path: Path, rows: Iterable[Any]) -> None:
    """Create ``path`` exclusively as compact sorted JSON lines, fsync it, then make it 0600."""
    with path.open("x", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)
