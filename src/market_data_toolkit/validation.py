from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from market_data_toolkit.models import Bar


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    row_count: int
    normalized_row_count: int
    symbol_count: int
    start_timestamp: datetime | None
    end_timestamp: datetime | None
    duplicate_minute_bucket_count: int
    daily_session_gap_count: int
    non_monotonic_symbols: tuple[str, ...]


def summarize_dataset(raw_bars: list[Bar], normalized_bars: list[Bar]) -> DatasetSummary:
    timestamps = [bar.timestamp for bar in normalized_bars]
    symbols = {bar.symbol for bar in normalized_bars}
    return DatasetSummary(
        row_count=len(raw_bars),
        normalized_row_count=len(normalized_bars),
        symbol_count=len(symbols),
        start_timestamp=min(timestamps, default=None),
        end_timestamp=max(timestamps, default=None),
        duplicate_minute_bucket_count=_count_duplicate_minute_buckets(raw_bars),
        daily_session_gap_count=_count_daily_session_gaps(normalized_bars),
        non_monotonic_symbols=_find_non_monotonic_symbols(raw_bars),
    )


def _count_duplicate_minute_buckets(bars: list[Bar]) -> int:
    seen: set[tuple[str, datetime]] = set()
    duplicates = 0
    for bar in bars:
        key = (bar.symbol.upper(), bar.timestamp.replace(second=0, microsecond=0))
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return duplicates


def _find_non_monotonic_symbols(bars: list[Bar]) -> tuple[str, ...]:
    last_seen: dict[str, datetime] = {}
    non_monotonic: set[str] = set()
    for bar in bars:
        previous = last_seen.get(bar.symbol)
        if previous is not None and bar.timestamp < previous:
            non_monotonic.add(bar.symbol)
        last_seen[bar.symbol] = bar.timestamp
    return tuple(sorted(non_monotonic))


def _count_daily_session_gaps(bars: list[Bar]) -> int:
    if not bars or not _looks_like_daily_dataset(bars):
        return 0

    missing_sessions = 0
    bars_by_symbol: dict[str, list[Bar]] = {}
    for bar in sorted(bars, key=lambda item: (item.symbol, item.timestamp)):
        bars_by_symbol.setdefault(bar.symbol, []).append(bar)

    for symbol_bars in bars_by_symbol.values():
        for previous, current in zip(symbol_bars, symbol_bars[1:], strict=False):
            missing_sessions += _missing_business_days(
                previous.timestamp.date(),
                current.timestamp.date(),
            )

    return missing_sessions


def _looks_like_daily_dataset(bars: list[Bar]) -> bool:
    return all(bar.timestamp.time() == time(0, 0) for bar in bars)


def _missing_business_days(previous: date, current: date) -> int:
    if current <= previous:
        return 0

    missing = 0
    probe = previous
    while True:
        probe = _next_business_day(probe)
        if probe >= current:
            break
        missing += 1
    return missing


def _next_business_day(day: date) -> date:
    probe = day + timedelta(days=1)
    while probe.weekday() >= 5:
        probe += timedelta(days=1)
    return probe
