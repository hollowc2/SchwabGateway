"""Issue one capability-scoped internal gateway key into a new digest-only file."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sys
from pathlib import Path

from schwab_gateway.auth import (
    KNOWN_CAPABILITIES,
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
)

KEY_BYTES = 32


def generate_key() -> str:
    return secrets.token_urlsafe(KEY_BYTES)


def key_digest(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def build_client_entry(
    application_id: str,
    capability: str,
    priority: PriorityClass,
) -> tuple[dict[str, object], str]:
    if capability not in KNOWN_CAPABILITIES:
        raise ValueError("unsupported gateway capability")
    plaintext = generate_key()
    entry: dict[str, object] = {
        "id": application_id,
        "key_sha256": key_digest(plaintext),
        "capabilities": [capability],
        "priority_class": priority.value,
    }
    # Validate the exact metadata before it is ever written.
    InternalPrincipal(
        client_id=application_id,
        key_sha256=str(entry["key_sha256"]),
        capabilities=frozenset({capability}),
        priority_class=priority,
    )
    return entry, plaintext


def build_keys_document(
    application_id: str,
    capability: str,
    priority: PriorityClass,
) -> tuple[dict[str, object], str]:
    entry, plaintext = build_client_entry(application_id, capability, priority)
    return {"version": 1, "clients": [entry]}, plaintext


def append_keys_document(
    existing_input: Path,
    application_id: str,
    capability: str,
    priority: PriorityClass,
) -> tuple[dict[str, object], str]:
    _, existing = InternalKeyAuthenticator.load_file(existing_input)
    if application_id in {client["id"] for client in existing["clients"]}:
        raise ValueError(f"gateway application ID already exists: {application_id}")
    entry, plaintext = build_client_entry(application_id, capability, priority)
    document: dict[str, object] = {
        "version": existing["version"],
        "clients": [dict(client) for client in existing["clients"]] + [entry],
    }
    # This catches a duplicate digest even though an actual collision is vanishingly rare.
    InternalKeyAuthenticator.from_document(document)
    return document, plaintext


def write_private_json(path: Path, document: dict[str, object]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--existing-input", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--application-id", required=True)
    parser.add_argument(
        "--capability", required=True, choices=sorted(KNOWN_CAPABILITIES)
    )
    parser.add_argument(
        "--priority", required=True, choices=[item.value for item in PriorityClass]
    )
    args = parser.parse_args(argv)

    if args.output.exists():
        parser.error("output path already exists; choose a new output path")
    priority = PriorityClass(args.priority)
    try:
        if args.existing_input is None:
            document, plaintext = build_keys_document(
                args.application_id, args.capability, priority
            )
        else:
            document, plaintext = append_keys_document(
                args.existing_input, args.application_id, args.capability, priority
            )
        write_private_json(args.output, document)
        InternalKeyAuthenticator.from_file(args.output)
    except (OSError, ValueError) as exc:
        args.output.unlink(missing_ok=True)
        parser.error(str(exc))

    sys.stdout.write("Distribute this new key now; it cannot be recovered.\n\n")
    sys.stdout.write(f"{args.application_id}: {plaintext}\n")
    sys.stdout.write(
        f"\nAdded 1 new digest; wrote {len(document['clients'])} total digest(s) "
        "at mode 0600.\n"
    )


if __name__ == "__main__":
    main()
