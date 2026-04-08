from __future__ import annotations

from pathlib import Path

import pytest

from market_data_toolkit.io import combine_datasets


def test_combine_datasets_merges_and_sorts_rows(tmp_path: Path) -> None:
    left = tmp_path / "aapl.csv"
    right = tmp_path / "msft.csv"
    left.write_text(
        "\n".join(
            [
                "symbol,timestamp,open,high,low,close,volume",
                "AAPL,2025-01-03T00:00:00,100,102,99,101,1000",
                "AAPL,2025-01-02T00:00:00,99,101,98,100,900",
            ]
        ),
        encoding="utf-8",
    )
    right.write_text(
        "\n".join(
            [
                "symbol,timestamp,open,high,low,close,volume",
                "MSFT,2025-01-02T00:00:00,200,201,199,200.5,500",
            ]
        ),
        encoding="utf-8",
    )

    combined = combine_datasets([right, left])

    assert [bar.symbol for bar in combined] == ["AAPL", "AAPL", "MSFT"]
    assert combined[0].timestamp.isoformat() == "2025-01-02T00:00:00"
    assert combined[-1].symbol == "MSFT"


def test_combine_datasets_rejects_duplicate_rows(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    contents = "\n".join(
        [
            "symbol,timestamp,open,high,low,close,volume",
            "AAPL,2025-01-02T00:00:00,99,101,98,100,900",
        ]
    )
    first.write_text(contents, encoding="utf-8")
    second.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate row detected"):
        combine_datasets([first, second])
