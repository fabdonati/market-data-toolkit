from __future__ import annotations

from pathlib import Path

import pytest

from market_data_toolkit.io import export_dataset, load_data


def test_load_data_reads_csv_rows(tmp_path: Path) -> None:
    source = tmp_path / "bars.csv"
    source.write_text(
        "\n".join(
            [
                "symbol,timestamp,open,high,low,close,volume",
                "AAPL,2025-01-02T09:30:00,100,101,99,100.5,1000",
                "AAPL,2025-01-02T09:31:00,100.5,102,100,101.25,1200",
            ]
        ),
        encoding="utf-8",
    )

    bars = load_data(source)

    assert len(bars) == 2
    assert bars[0].symbol == "AAPL"
    assert bars[1].close == pytest.approx(101.25)


def test_export_dataset_round_trips_bars(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text(
        "\n".join(
            [
                "symbol,timestamp,open,high,low,close,volume",
                "msft,2025-01-02T09:30:00,10,11,9,10.5,200",
            ]
        ),
        encoding="utf-8",
    )

    exported = tmp_path / "output.csv"
    export_dataset(exported, load_data(source))

    round_tripped = load_data(exported)

    assert round_tripped[0].symbol == "MSFT"
    assert round_tripped[0].volume == pytest.approx(200.0)

