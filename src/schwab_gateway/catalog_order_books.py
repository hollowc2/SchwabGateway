"""Build a verified catalog and non-destructive retention plan for order-book evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

from schwab_gateway.order_book_catalog import write_catalog


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--archive-after-days", type=int, default=30)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.evidence_root.is_absolute() or not args.output.is_absolute():
        parser.error("evidence root and output must be absolute")
    try:
        catalog = write_catalog(
            args.evidence_root,
            args.output,
            archive_after_days=args.archive_after_days,
        )
    except (OSError, ValueError) as exc:
        parser.exit(1, f"order-book catalog failed: {exc}\n")
    print(catalog)


if __name__ == "__main__":
    main()
