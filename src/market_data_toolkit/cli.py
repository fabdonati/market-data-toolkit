from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from market_data_toolkit.features import FeatureRow, compute_features
from market_data_toolkit.io import REQUIRED_COLUMNS, export_dataset, load_data
from market_data_toolkit.normalize import normalize_bars
from market_data_toolkit.validation import summarize_dataset


def main() -> None:
    parser = argparse.ArgumentParser(prog="mdtk", description="Market data toolkit CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="Normalize a CSV dataset.")
    ingest.add_argument("source", type=Path)
    ingest.add_argument("--output", type=Path, required=True)

    validate = subparsers.add_parser("validate", help="Validate a CSV dataset.")
    validate.add_argument("source", type=Path)

    features = subparsers.add_parser("features", help="Compute features from a CSV dataset.")
    features.add_argument("source", type=Path)
    features.add_argument("--output", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "ingest":
        _run_ingest(args.source, args.output)
    elif args.command == "validate":
        _run_validate(args.source)
    else:
        _run_features(args.source, args.output)


def _run_ingest(source: Path, output: Path) -> None:
    export_dataset(output, normalize_bars(load_data(source)))
    print(f"Wrote normalized dataset to {output}")


def _run_validate(source: Path) -> None:
    bars = load_data(source)
    normalized = normalize_bars(bars)
    summary = summarize_dataset(bars, normalized)
    print(
        f"Validated {summary.row_count} rows across {summary.symbol_count} symbols. "
        f"Required columns: {', '.join(REQUIRED_COLUMNS)}. "
        f"Normalized row count: {summary.normalized_row_count}. "
        f"Window: {summary.start_timestamp} -> {summary.end_timestamp}."
    )


def _run_features(source: Path, output: Path) -> None:
    feature_rows = compute_features(normalize_bars(load_data(source)))
    _write_feature_rows(output, feature_rows)
    print(f"Wrote {len(feature_rows)} feature rows to {output}")


def _write_feature_rows(output: Path, rows: Sequence[FeatureRow]) -> None:
    if not rows:
        raise ValueError("No rows available to export.")

    first_row = asdict(rows[0])
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(first_row.keys()))
        writer.writeheader()
        writer.writerow(first_row)
        for row in rows[1:]:
            writer.writerow(asdict(row))


if __name__ == "__main__":
    main()
