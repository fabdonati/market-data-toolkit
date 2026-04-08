from __future__ import annotations

import csv
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol

from market_data_toolkit.models import Bar
from market_data_toolkit.normalize import normalize_bars

REQUIRED_IBKR_COLUMNS = ("date", "open", "high", "low", "close", "volume")
OPTIONAL_IBKR_COLUMNS = ("average", "barcount", "symbol")


class HistoricalDataClient(Protocol):
    bars: list[Any]
    completed: threading.Event
    ready: threading.Event
    failure: Exception | None

    def connect(self, host: str, port: int, client_id: int) -> None:
        ...

    def run(self) -> None:
        ...

    def disconnect(self) -> None:
        ...

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
        ...


def load_ibkr_historical_data(path: str | Path, *, symbol: str | None = None) -> list[Bar]:
    csv_path = Path(path)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV file is missing a header row.")

        normalized_headers = {header.lower(): header for header in reader.fieldnames}
        missing = [column for column in REQUIRED_IBKR_COLUMNS if column not in normalized_headers]
        if missing:
            raise ValueError(f"IBKR CSV file is missing required columns: {', '.join(missing)}")

        bars: list[Bar] = []
        for raw_row in reader:
            row = {key.lower(): value for key, value in raw_row.items()}
            bars.append(_ibkr_row_to_bar(row, symbol=symbol))

    return bars


def _ibkr_row_to_bar(row: dict[str, str | None], *, symbol: str | None) -> Bar:
    resolved_symbol = row.get("symbol") or symbol
    if resolved_symbol is None or resolved_symbol == "":
        raise ValueError(
            "IBKR historical rows need a symbol column or an explicit symbol argument."
        )

    return Bar(
        symbol=resolved_symbol.upper(),
        timestamp=_parse_ibkr_timestamp(_required_value(row, "date")),
        open=float(_required_value(row, "open")),
        high=float(_required_value(row, "high")),
        low=float(_required_value(row, "low")),
        close=float(_required_value(row, "close")),
        volume=float(_required_value(row, "volume")),
    )


def _parse_ibkr_timestamp(value: str) -> datetime:
    candidates = (
        "%Y%m%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d",
        "%Y-%m-%d",
    )
    for candidate in candidates:
        try:
            return datetime.strptime(value, candidate)
        except ValueError:
            continue

    return datetime.fromisoformat(value)


def _required_value(row: dict[str, str | None], key: str) -> str:
    value = row.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required value for column '{key}'.")
    return value


def fetch_ibkr_historical_data(
    *,
    symbols: list[str],
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 1,
    exchange: str = "SMART",
    currency: str = "USD",
    end_datetime: str = "",
    duration: str = "1 Y",
    bar_size: str = "1 day",
    what_to_show: str = "TRADES",
    use_rth: bool = True,
    timeout_seconds: float = 15.0,
    client_factory: Callable[[], HistoricalDataClient] | None = None,
) -> list[Bar]:
    if not symbols:
        raise ValueError("symbols must not be empty")

    factory = client_factory or _create_historical_data_client
    client = factory()
    client.connect(host, port, client_id)
    runner = threading.Thread(target=client.run, daemon=True)
    runner.start()

    try:
        if not client.ready.wait(timeout_seconds):
            raise TimeoutError(
                "Timed out waiting for the IBKR API handshake. "
                "Check that IB Gateway / TWS is logged in, API access is enabled, "
                "and the host/port/client id are correct."
            )

        fetched_bars: list[Bar] = []
        for req_id, symbol in enumerate(symbols, start=1):
            client.completed.clear()
            client.failure = None
            client.bars.clear()
            client.req_historical_data(
                req_id=req_id,
                symbol=symbol,
                exchange=exchange,
                currency=currency,
                end_datetime=end_datetime,
                duration=duration,
                bar_size=bar_size,
                what_to_show=what_to_show,
                use_rth=use_rth,
            )

            if not client.completed.wait(timeout_seconds):
                raise TimeoutError(
                    f"Timed out waiting for IBKR historical data for {symbol}. "
                    "The API session connected, but no historical response arrived."
                )
            if client.failure is not None:
                raise RuntimeError(
                    f"IBKR historical data request for {symbol} failed."
                ) from client.failure

            fetched_bars.extend(_api_bars_to_bars(client.bars, symbol))

        return normalize_bars(fetched_bars)
    finally:
        client.disconnect()


def _api_bars_to_bars(api_bars: list[Any], symbol: str) -> list[Bar]:
    bars: list[Bar] = []
    for api_bar in api_bars:
        bars.append(
            Bar(
                symbol=symbol.upper(),
                timestamp=_parse_ibkr_timestamp(str(api_bar.date)),
                open=float(api_bar.open),
                high=float(api_bar.high),
                low=float(api_bar.low),
                close=float(api_bar.close),
                volume=float(api_bar.volume),
            )
        )
    return bars


def _create_historical_data_client() -> HistoricalDataClient:
    try:
        from ibapi.client import EClient  # type: ignore[import-untyped]
        from ibapi.contract import Contract  # type: ignore[import-untyped]
        from ibapi.wrapper import EWrapper  # type: ignore[import-untyped]
    except ImportError as error:
        raise ImportError(
            "IBKR fetching requires the optional dependency group: pip install -e '.[ibkr,dev]'"
        ) from error

    class _HistoricalDataClient(EWrapper, EClient):  # type: ignore[misc]
        def __init__(self) -> None:
            EClient.__init__(self, self)
            self.bars: list[Any] = []
            self.completed = threading.Event()
            self.ready = threading.Event()
            self.failure: Exception | None = None

        def nextValidId(self, orderId: int) -> None:  # noqa: N802
            del orderId
            self.ready.set()

        def historicalData(self, reqId: int, bar: Any) -> None:  # noqa: N802
            self.bars.append(bar)

        def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:  # noqa: N802
            self.completed.set()

        def error(self, reqId: int, errorCode: int, errorString: str) -> None:  # noqa: N802
            if errorCode in {2104, 2106, 2158}:
                return
            self.failure = RuntimeError(f"IBKR error {errorCode}: {errorString}")
            self.completed.set()

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
            contract = Contract()
            contract.symbol = symbol
            contract.secType = "STK"
            contract.exchange = exchange
            contract.currency = currency

            self.reqHistoricalData(
                req_id,
                contract,
                end_datetime,
                duration,
                bar_size,
                what_to_show,
                1 if use_rth else 0,
                1,
                False,
                [],
            )

    return _HistoricalDataClient()
