from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from market_data_toolkit.features import FeatureRow, compute_features
from market_data_toolkit.ibkr import fetch_ibkr_historical_data, load_ibkr_historical_data
from market_data_toolkit.io import REQUIRED_COLUMNS, export_dataset, load_data
from market_data_toolkit.normalize import normalize_bars
from market_data_toolkit.validation import summarize_dataset


def main() -> None:
    parser = argparse.ArgumentParser(prog="mdtk", description="Market data toolkit CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="Normalize a CSV dataset.")
    ingest.add_argument("source", type=Path)
    ingest.add_argument("--output", type=Path, required=True)

    ibkr_ingest = subparsers.add_parser(
        "ibkr-ingest",
        help="Normalize an IBKR historical-data CSV export into the toolkit format.",
    )
    ibkr_ingest.add_argument("source", type=Path)
    ibkr_ingest.add_argument("--output", type=Path, required=True)
    ibkr_ingest.add_argument("--symbol", type=str)

    ibkr_fetch = subparsers.add_parser(
        "ibkr-fetch",
        help="Fetch IBKR historical bars directly from TWS / IB Gateway.",
    )
    ibkr_fetch.add_argument("--symbols", required=True, help="Comma-separated symbol list.")
    ibkr_fetch.add_argument("--output", type=Path, required=True)
    ibkr_fetch.add_argument("--host", default="127.0.0.1")
    ibkr_fetch.add_argument("--port", type=int, default=7497)
    ibkr_fetch.add_argument("--client-id", type=int, default=1)
    ibkr_fetch.add_argument("--exchange", default="SMART")
    ibkr_fetch.add_argument("--currency", default="USD")
    ibkr_fetch.add_argument("--duration", default="1 Y")
    ibkr_fetch.add_argument("--bar-size", default="1 day")
    ibkr_fetch.add_argument("--what-to-show", default="TRADES")
    ibkr_fetch.add_argument("--end-datetime", default="")
    ibkr_fetch.add_argument("--timeout-seconds", type=float, default=15.0)
    ibkr_fetch.add_argument("--include-extended-hours", action="store_true")

    validate = subparsers.add_parser("validate", help="Validate a CSV dataset.")
    validate.add_argument("source", type=Path)

    features = subparsers.add_parser("features", help="Compute features from a CSV dataset.")
    features.add_argument("source", type=Path)
    features.add_argument("--output", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "ingest":
        _run_ingest(args.source, args.output)
    elif args.command == "ibkr-ingest":
        _run_ibkr_ingest(args.source, args.output, args.symbol)
    elif args.command == "ibkr-fetch":
        _run_ibkr_fetch(args)
    elif args.command == "validate":
        _run_validate(args.source)
    else:
        _run_features(args.source, args.output)


def _run_ingest(source: Path, output: Path) -> None:
    export_dataset(output, normalize_bars(load_data(source)))
    print(f"Wrote normalized dataset to {output}")


def _run_ibkr_ingest(source: Path, output: Path, symbol: str | None) -> None:
    export_dataset(output, normalize_bars(load_ibkr_historical_data(source, symbol=symbol)))
    print(f"Wrote normalized IBKR dataset to {output}")


def _run_ibkr_fetch(args: argparse.Namespace) -> None:
    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    bars = fetch_ibkr_historical_data(
        symbols=symbols,
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        exchange=args.exchange,
        currency=args.currency,
        end_datetime=args.end_datetime,
        duration=args.duration,
        bar_size=args.bar_size,
        what_to_show=args.what_to_show,
        use_rth=not args.include_extended_hours,
        timeout_seconds=args.timeout_seconds,
    )
    export_dataset(args.output, bars)
    print(f"Wrote {len(bars)} fetched IBKR bars to {args.output}")


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
