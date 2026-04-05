from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path


def _write_input_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "symbol,timestamp,open,high,low,close,volume",
                "aapl,2025-01-02T09:30:12,100,101,99,100.5,1000",
                "aapl,2025-01-02T09:30:55,100.5,102,100,101.25,1200",
                "aapl,2025-01-02T09:31:00,101.25,103,101,102,800",
            ]
        ),
        encoding="utf-8",
    )


def test_validate_command_reports_counts(tmp_path: Path) -> None:
    source = tmp_path / "bars.csv"
    _write_input_csv(source)

    result = subprocess.run(
        [sys.executable, "-m", "market_data_toolkit.cli", "validate", str(source)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Validated 3 rows across 1 symbols" in result.stdout
    assert "Normalized row count: 2" in result.stdout
    assert "Window: 2025-01-02 09:30:00 -> 2025-01-02 09:31:00" in result.stdout



def test_features_command_writes_feature_csv(tmp_path: Path) -> None:
    source = tmp_path / "bars.csv"
    output = tmp_path / "features.csv"
    _write_input_csv(source)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "market_data_toolkit.cli",
            "features",
            str(source),
            "--output",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    with output.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert rows[0]["symbol"] == "AAPL"
    assert rows[1]["rolling_mean_3"] == ""


def test_ingest_command_writes_normalized_rows(tmp_path: Path) -> None:
    source = tmp_path / "bars.csv"
    output = tmp_path / "normalized.csv"
    _write_input_csv(source)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "market_data_toolkit.cli",
            "ingest",
            str(source),
            "--output",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    with output.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert rows[0]["timestamp"] == "2025-01-02T09:30:00"
