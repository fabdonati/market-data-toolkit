from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from market_data_toolkit.models import Bar

REQUIRED_IBKR_COLUMNS = ("date", "open", "high", "low", "close", "volume")
OPTIONAL_IBKR_COLUMNS = ("average", "barcount", "symbol")


def load_ibkr_historical_data(path: str | Path, *, symbol: str | None = None) -> list[Bar]:
    csv_path = Path(path)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV file is missing a header row.")

        normalized_headers = {header.lower(): header for header in reader.fieldnames}
        missing = [column for column in REQUIRED_IBKR_COLUMNS if column not in normalized_headers]
        if missing:
            raise ValueError(f"IBKR CSV file is missing required columns: {', '.join(missing)}")

        bars: list[Bar] = []
        for raw_row in reader:
            row = {key.lower(): value for key, value in raw_row.items()}
            bars.append(_ibkr_row_to_bar(row, symbol=symbol))

    return bars


def _ibkr_row_to_bar(row: dict[str, str | None], *, symbol: str | None) -> Bar:
    resolved_symbol = row.get("symbol") or symbol
    if resolved_symbol is None or resolved_symbol == "":
        raise ValueError(
            "IBKR historical rows need a symbol column or an explicit symbol argument."
        )

    return Bar(
        symbol=resolved_symbol.upper(),
        timestamp=_parse_ibkr_timestamp(_required_value(row, "date")),
        open=float(_required_value(row, "open")),
        high=float(_required_value(row, "high")),
        low=float(_required_value(row, "low")),
        close=float(_required_value(row, "close")),
        volume=float(_required_value(row, "volume")),
    )


def _parse_ibkr_timestamp(value: str) -> datetime:
    candidates = (
        "%Y%m%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d",
        "%Y-%m-%d",
    )
    for candidate in candidates:
        try:
            return datetime.strptime(value, candidate)
        except ValueError:
            continue

    return datetime.fromisoformat(value)


def _required_value(row: dict[str, str | None], key: str) -> str:
    value = row.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required value for column '{key}'.")
    return value
