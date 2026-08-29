"""Build a traceable derived dataset from one verified order-book capture."""

from __future__ import annotations

import argparse
from pathlib import Path

from schwab_gateway.order_book_analysis import (
    OrderBookAnalysisError,
    write_derived_dataset,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-manifest", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--depth-levels", type=int, default=10)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.capture_manifest.is_absolute() or not args.output_directory.is_absolute():
        parser.error("capture manifest and output directory must be absolute")
    try:
        derived_manifest = write_derived_dataset(
            args.capture_manifest,
            args.output_directory,
            depth_levels=args.depth_levels,
        )
    except (OrderBookAnalysisError, OSError, ValueError) as exc:
        parser.exit(1, f"order-book derivation failed: {exc}\n")
    print(derived_manifest)


if __name__ == "__main__":
    main()
