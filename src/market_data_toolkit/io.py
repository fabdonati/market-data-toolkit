from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable

from market_data_toolkit.models import Bar

REQUIRED_COLUMNS = ("symbol", "timestamp", "open", "high", "low", "close", "volume")


def load_data(path: str | Path) -> list[Bar]:
    csv_path = Path(path)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV file is missing a header row.")

        missing = [column for column in REQUIRED_COLUMNS if column not in reader.fieldnames]
        if missing:
            raise ValueError(f"CSV file is missing required columns: {', '.join(missing)}")

        bars: list[Bar] = []
        for row in reader:
            bars.append(_row_to_bar(row))

    return bars


def export_dataset(path: str | Path, bars: Iterable[Bar]) -> None:
    csv_path = Path(path)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(REQUIRED_COLUMNS)
        for bar in bars:
            writer.writerow(
                (
                    bar.symbol,
                    bar.timestamp.isoformat(),
                    f"{bar.open:.8f}",
                    f"{bar.high:.8f}",
                    f"{bar.low:.8f}",
                    f"{bar.close:.8f}",
                    f"{bar.volume:.8f}",
                )
            )


def _row_to_bar(row: dict[str, str | None]) -> Bar:
    timestamp_raw = _required_value(row, "timestamp")
    return Bar(
        symbol=_required_value(row, "symbol").upper(),
        timestamp=datetime.fromisoformat(timestamp_raw),
        open=float(_required_value(row, "open")),
        high=float(_required_value(row, "high")),
        low=float(_required_value(row, "low")),
        close=float(_required_value(row, "close")),
        volume=float(_required_value(row, "volume")),
    )


def _required_value(row: dict[str, str | None], key: str) -> str:
    value = row.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required value for column '{key}'.")
    return value

