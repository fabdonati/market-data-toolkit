from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable

from market_data_toolkit.models import Bar


def normalize_bars(bars: Iterable[Bar]) -> list[Bar]:
    grouped: dict[tuple[str, datetime], Bar] = {}
    for bar in bars:
        key = (bar.symbol.upper(), bar.timestamp.replace(second=0, microsecond=0))
        if key in grouped:
            grouped[key] = _merge_bars(grouped[key], bar)
        else:
            grouped[key] = Bar(
                symbol=bar.symbol.upper(),
                timestamp=key[1],
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )

    return sorted(grouped.values(), key=lambda bar: (bar.symbol, bar.timestamp))


def resample_bars(bars: Iterable[Bar], interval_minutes: int) -> list[Bar]:
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive.")

    buckets: dict[tuple[str, datetime], list[Bar]] = defaultdict(list)
    for bar in sorted(bars, key=lambda item: (item.symbol, item.timestamp)):
        bucket_time = _bucket_timestamp(bar.timestamp, interval_minutes)
        buckets[(bar.symbol, bucket_time)].append(bar)

    resampled: list[Bar] = []
    for (symbol, bucket_time), bucket_bars in sorted(buckets.items()):
        first = bucket_bars[0]
        last = bucket_bars[-1]
        resampled.append(
            Bar(
                symbol=symbol,
                timestamp=bucket_time,
                open=first.open,
                high=max(item.high for item in bucket_bars),
                low=min(item.low for item in bucket_bars),
                close=last.close,
                volume=sum(item.volume for item in bucket_bars),
            )
        )

    return resampled


def _bucket_timestamp(timestamp: datetime, interval_minutes: int) -> datetime:
    truncated = timestamp.replace(second=0, microsecond=0)
    minute_offset = truncated.minute % interval_minutes
    return truncated - timedelta(minutes=minute_offset)


def _merge_bars(left: Bar, right: Bar) -> Bar:
    return Bar(
        symbol=left.symbol,
        timestamp=left.timestamp,
        open=left.open,
        high=max(left.high, right.high),
        low=min(left.low, right.low),
        close=right.close,
        volume=left.volume + right.volume,
    )
