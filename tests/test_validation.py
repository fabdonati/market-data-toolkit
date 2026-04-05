from __future__ import annotations

from datetime import datetime

from market_data_toolkit.models import Bar
from market_data_toolkit.validation import summarize_dataset


def test_summarize_dataset_reports_range_and_symbol_count() -> None:
    raw_bars = [
        Bar("AAPL", datetime.fromisoformat("2025-01-02T09:30:00"), 100, 101, 99, 100.5, 1000),
        Bar("MSFT", datetime.fromisoformat("2025-01-02T09:31:00"), 200, 201, 199, 200.5, 500),
    ]

    summary = summarize_dataset(raw_bars, raw_bars)

    assert summary.row_count == 2
    assert summary.normalized_row_count == 2
    assert summary.symbol_count == 2
    assert summary.start_timestamp == datetime.fromisoformat("2025-01-02T09:30:00")
    assert summary.end_timestamp == datetime.fromisoformat("2025-01-02T09:31:00")
