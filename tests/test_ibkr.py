from __future__ import annotations

from pathlib import Path

import pytest

from market_data_toolkit.ibkr import load_ibkr_historical_data


def test_load_ibkr_historical_data_with_explicit_symbol(tmp_path: Path) -> None:
    source = tmp_path / "ibkr.csv"
    source.write_text(
        "\n".join(
            [
                "Date,Open,High,Low,Close,Volume,Average,BarCount",
                "20250102 09:30:00,100,101,99,100.5,1000,100.2,45",
                "20250102 09:31:00,100.5,102,100,101.25,1200,101.1,50",
            ]
        ),
        encoding="utf-8",
    )

    bars = load_ibkr_historical_data(source, symbol="aapl")

    assert len(bars) == 2
    assert bars[0].symbol == "AAPL"
    assert bars[0].timestamp.isoformat() == "2025-01-02T09:30:00"
    assert bars[1].close == pytest.approx(101.25)


def test_load_ibkr_historical_data_uses_symbol_column_when_available(tmp_path: Path) -> None:
    source = tmp_path / "ibkr.csv"
    source.write_text(
        "\n".join(
            [
                "date,symbol,open,high,low,close,volume",
                "2025-01-02 09:30:00,msft,200,201,199,200.5,500",
            ]
        ),
        encoding="utf-8",
    )

    bars = load_ibkr_historical_data(source)

    assert len(bars) == 1
    assert bars[0].symbol == "MSFT"
    assert bars[0].volume == pytest.approx(500.0)


def test_load_ibkr_historical_data_requires_symbol_information(tmp_path: Path) -> None:
    source = tmp_path / "ibkr.csv"
    source.write_text(
        "\n".join(
            [
                "Date,Open,High,Low,Close,Volume",
                "20250102,100,101,99,100.5,1000",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="need a symbol column or an explicit symbol argument"):
        load_ibkr_historical_data(source)
