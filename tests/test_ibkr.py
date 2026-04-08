from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from market_data_toolkit.ibkr import fetch_ibkr_historical_data, load_ibkr_historical_data


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


class _FakeHistoricalClient:
    def __init__(self) -> None:
        self.bars: list[object] = []
        self.completed = threading.Event()
        self.ready = threading.Event()
        self.failure: Exception | None = None
        self.connected = False

    def connect(self, host: str, port: int, client_id: int) -> None:
        self.connected = True
        self.ready.set()

    def run(self) -> None:
        return None

    def disconnect(self) -> None:
        self.connected = False

    def req_historical_data(
        self,
        *,
        req_id: int,
        symbol: str,
        exchange: str,
        currency: str,
        end_datetime: str,
        duration: str,
        bar_size: str,
        what_to_show: str,
        use_rth: bool,
    ) -> None:
        del req_id, exchange, currency, end_datetime, duration, bar_size, what_to_show, use_rth
        self.bars[:] = [
            SimpleNamespace(
                date="20250102",
                open=100.0 if symbol == "AAPL" else 200.0,
                high=101.0 if symbol == "AAPL" else 201.0,
                low=99.0 if symbol == "AAPL" else 199.0,
                close=100.5 if symbol == "AAPL" else 200.5,
                volume=1000 if symbol == "AAPL" else 500,
            )
        ]
        self.completed.set()


def test_fetch_ibkr_historical_data_converts_client_bars() -> None:
    bars = fetch_ibkr_historical_data(
        symbols=["AAPL", "MSFT"],
        timeout_seconds=1.0,
        client_factory=_FakeHistoricalClient,
    )

    assert len(bars) == 2
    assert bars[0].symbol == "AAPL"
    assert bars[1].symbol == "MSFT"
    assert bars[0].timestamp.isoformat() == "2025-01-02T00:00:00"


def test_fetch_ibkr_historical_data_rejects_empty_symbol_lists() -> None:
    with pytest.raises(ValueError, match="symbols must not be empty"):
        fetch_ibkr_historical_data(symbols=[], client_factory=_FakeHistoricalClient)


class _NeverReadyHistoricalClient(_FakeHistoricalClient):
    def connect(self, host: str, port: int, client_id: int) -> None:
        self.connected = True


def test_fetch_ibkr_historical_data_times_out_if_handshake_never_arrives() -> None:
    with pytest.raises(TimeoutError, match="IBKR API handshake"):
        fetch_ibkr_historical_data(
            symbols=["AAPL"],
            timeout_seconds=0.01,
            client_factory=_NeverReadyHistoricalClient,
        )
