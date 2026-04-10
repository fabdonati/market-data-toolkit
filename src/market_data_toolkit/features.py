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
    gap_return: float | None
    range_pct: float
    rolling_mean_3: float | None
    rolling_volatility_3: float | None
    volume_ratio_3: float | None
    volume_zscore_3: float | None
    drawdown_pct: float


def compute_features(bars: list[Bar]) -> list[FeatureRow]:
    sorted_bars = sorted(bars, key=lambda item: (item.symbol, item.timestamp))
    features: list[FeatureRow] = []
    history: dict[str, list[Bar]] = {}
    return_history: dict[str, list[float]] = {}
    running_peak: dict[str, float] = {}

    for bar in sorted_bars:
        symbol_history = history.setdefault(bar.symbol, [])
        symbol_returns = return_history.setdefault(bar.symbol, [])
        prev_bar = symbol_history[-1] if symbol_history else None
        window = symbol_history[-2:] + [bar]

        rolling_mean = fmean(item.close for item in window) if len(window) == 3 else None
        volume_zscore = _volume_zscore(window) if len(window) == 3 else None
        log_return = None if prev_bar is None else (bar.close / prev_bar.close) - 1.0
        gap_return = None if prev_bar is None else (bar.open / prev_bar.close) - 1.0
        recent_returns = symbol_returns[-2:] + [log_return] if log_return is not None else []
        rolling_volatility = (
            _rolling_volatility(recent_returns) if len(recent_returns) == 3 else None
        )
        volume_ratio = _volume_ratio(window) if len(window) == 3 else None
        peak_close = max(running_peak.get(bar.symbol, bar.close), bar.close)
        running_peak[bar.symbol] = peak_close
        drawdown_pct = (bar.close / peak_close) - 1.0

        features.append(
            FeatureRow(
                symbol=bar.symbol,
                timestamp=bar.timestamp.isoformat(),
                close=bar.close,
                log_return=log_return,
                gap_return=gap_return,
                range_pct=(bar.high - bar.low) / bar.open,
                rolling_mean_3=rolling_mean,
                rolling_volatility_3=rolling_volatility,
                volume_ratio_3=volume_ratio,
                volume_zscore_3=volume_zscore,
                drawdown_pct=drawdown_pct,
            )
        )
        symbol_history.append(bar)
        if log_return is not None:
            symbol_returns.append(log_return)

    return features


def _volume_zscore(window: list[Bar]) -> float | None:
    mean_volume = fmean(item.volume for item in window)
    centered = [item.volume - mean_volume for item in window]
    variance = sum(value * value for value in centered) / len(centered)
    if variance == 0:
        return None
    return centered[-1] / sqrt(variance)


def _volume_ratio(window: list[Bar]) -> float | None:
    mean_volume = fmean(item.volume for item in window)
    if mean_volume == 0:
        return None
    return window[-1].volume / mean_volume


def _rolling_volatility(returns: list[float]) -> float | None:
    if len(returns) != 3:
        return None
    mean_return = fmean(returns)
    variance = sum((value - mean_return) ** 2 for value in returns) / len(returns)
    return sqrt(variance)
