from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from market_data_toolkit.models import Bar


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    row_count: int
    normalized_row_count: int
    symbol_count: int
    start_timestamp: datetime | None
    end_timestamp: datetime | None


def summarize_dataset(raw_bars: list[Bar], normalized_bars: list[Bar]) -> DatasetSummary:
    timestamps = [bar.timestamp for bar in normalized_bars]
    symbols = {bar.symbol for bar in normalized_bars}
    return DatasetSummary(
        row_count=len(raw_bars),
        normalized_row_count=len(normalized_bars),
        symbol_count=len(symbols),
        start_timestamp=min(timestamps, default=None),
        end_timestamp=max(timestamps, default=None),
    )
