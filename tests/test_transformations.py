from __future__ import annotations

from datetime import datetime

import pytest

from market_data_toolkit.features import compute_features
from market_data_toolkit.models import Bar
from market_data_toolkit.normalize import normalize_bars, resample_bars


@pytest.fixture
def sample_bars() -> list[Bar]:
    return [
        Bar("AAPL", datetime.fromisoformat("2025-01-02T09:30:12"), 100, 101, 99, 100.5, 1000),
        Bar("AAPL", datetime.fromisoformat("2025-01-02T09:30:55"), 100.5, 102, 100, 101.25, 1200),
        Bar("AAPL", datetime.fromisoformat("2025-01-02T09:31:00"), 101.25, 103, 101, 102, 800),
        Bar("AAPL", datetime.fromisoformat("2025-01-02T09:32:00"), 102, 104, 101.5, 103, 1600),
    ]


def test_normalize_bars_merges_duplicate_minutes(sample_bars: list[Bar]) -> None:
    normalized = normalize_bars(sample_bars)

    assert len(normalized) == 3
    assert normalized[0].timestamp == datetime.fromisoformat("2025-01-02T09:30:00")
    assert normalized[0].high == pytest.approx(102.0)
    assert normalized[0].close == pytest.approx(101.25)
    assert normalized[0].volume == pytest.approx(2200.0)


def test_resample_bars_builds_2_minute_buckets(sample_bars: list[Bar]) -> None:
    normalized = normalize_bars(sample_bars)
    resampled = resample_bars(normalized, interval_minutes=2)

    assert len(resampled) == 2
    assert resampled[0].timestamp == datetime.fromisoformat("2025-01-02T09:30:00")
    assert resampled[0].open == pytest.approx(100.0)
    assert resampled[0].close == pytest.approx(102.0)
    assert resampled[0].volume == pytest.approx(3000.0)


def test_normalize_bars_uses_latest_close_for_out_of_order_ticks() -> None:
    bars = [
        Bar("AAPL", datetime.fromisoformat("2025-01-02T09:30:40"), 100.5, 102, 100, 101.25, 1200),
        Bar("AAPL", datetime.fromisoformat("2025-01-02T09:30:05"), 100, 101, 99, 100.5, 1000),
    ]

    normalized = normalize_bars(bars)

    assert normalized[0].close == pytest.approx(101.25)


def test_compute_features_generates_rolling_fields(sample_bars: list[Bar]) -> None:
    normalized = normalize_bars(sample_bars)
    feature_rows = compute_features(normalized)

    assert feature_rows[0].log_return is None
    assert feature_rows[2].rolling_mean_3 == pytest.approx((101.25 + 102 + 103) / 3)
    assert feature_rows[2].range_pct == pytest.approx((104 - 101.5) / 102)
    assert feature_rows[2].volume_zscore_3 is not None
