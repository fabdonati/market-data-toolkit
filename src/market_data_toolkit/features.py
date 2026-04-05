from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import fmean

from market_data_toolkit.models import Bar


@dataclass(frozen=True, slots=True)
class FeatureRow:
    symbol: str
    timestamp: str
    close: float
    log_return: float | None
    range_pct: float
    rolling_mean_3: float | None
    volume_zscore_3: float | None


def compute_features(bars: list[Bar]) -> list[FeatureRow]:
    sorted_bars = sorted(bars, key=lambda item: (item.symbol, item.timestamp))
    features: list[FeatureRow] = []
    history: dict[str, list[Bar]] = {}

    for bar in sorted_bars:
        symbol_history = history.setdefault(bar.symbol, [])
        prev_bar = symbol_history[-1] if symbol_history else None
        window = symbol_history[-2:] + [bar]

        rolling_mean = fmean(item.close for item in window) if len(window) == 3 else None
        volume_zscore = _volume_zscore(window) if len(window) == 3 else None
        log_return = None if prev_bar is None else (bar.close / prev_bar.close) - 1.0

        features.append(
            FeatureRow(
                symbol=bar.symbol,
                timestamp=bar.timestamp.isoformat(),
                close=bar.close,
                log_return=log_return,
                range_pct=(bar.high - bar.low) / bar.open,
                rolling_mean_3=rolling_mean,
                volume_zscore_3=volume_zscore,
            )
        )
        symbol_history.append(bar)

    return features


def _volume_zscore(window: list[Bar]) -> float | None:
    mean_volume = fmean(item.volume for item in window)
    centered = [item.volume - mean_volume for item in window]
    variance = sum(value * value for value in centered) / len(centered)
    if variance == 0:
        return None
    return centered[-1] / sqrt(variance)
