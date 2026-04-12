# market-data-toolkit

`market-data-toolkit` is a lightweight Python package for ingesting, validating, normalizing,
and enriching OHLCV-style bar data.

## Features

- Load OHLCV data from CSV into typed bar objects
- Normalize duplicate timestamps into canonical minute bars
- Resample bars into coarser intervals
- Generate derived features such as returns, gap returns, rolling means, rolling volatility, volume ratios, and drawdown
- Validate duplicate minute buckets, daily session gaps, and non-monotonic symbol ordering from the command line

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

## CLI usage

Validate a dataset:

```bash
mdtk validate data/bars.csv
```

Validation reports row counts plus:

- duplicate minute buckets that would collapse during normalization
- obvious missing business-day gaps for daily datasets
- symbols whose raw rows arrive out of timestamp order

Normalize a dataset:

```bash
mdtk ingest data/raw.csv --output data/normalized.csv
```

Compute features:

```bash
mdtk features data/raw.csv --output data/features.csv
```

Convert an IBKR historical-data export into the toolkit schema:

```bash
mdtk ibkr-ingest data/ibkr_aapl.csv --symbol AAPL --output data/normalized.csv
```

If the IBKR export already includes a `symbol` column, `--symbol` is optional.
If it does not, treat the file as a single-symbol export and run the import once per symbol file.

Fetch IBKR historical bars directly from TWS / IB Gateway:

```bash
python -m pip install -e ".[ibkr,dev]"
mdtk ibkr-fetch --symbols AAPL,MSFT --duration "1 Y" --bar-size "1 day" --output data/portfolio.csv
```

This requires:

- TWS or IB Gateway running locally
- API access enabled
- the correct host / port / client id for your IBKR session

Combine multiple normalized datasets into one portfolio-ready CSV:

```bash
mdtk combine data/aapl.csv data/msft.csv data/nvda.csv --output data/portfolio.csv
```

The combined output is sorted by `symbol` and `timestamp`, and exact duplicate rows are rejected.

## End-to-end example

A committed pipeline example lives under
[`examples/portfolio_pipeline`](/Users/fabrizio/Dev/Finance/market-data-toolkit/examples/portfolio_pipeline/README.md).

It shows a complete workflow:

1. ingest two IBKR-style historical exports
2. combine the normalized symbol files
3. validate the resulting portfolio dataset
4. compute derived features
5. hand the combined dataset to `backtest-lab`

## How to read the outputs

The main CLI outputs are meant to answer three different research questions:

- `validate`
  - answers whether the raw file is structurally safe to use
  - duplicate minute buckets indicate rows that would collapse during normalization
  - daily session gaps indicate missing business-day coverage in daily data
  - non-monotonic symbols indicate symbol-level ordering problems in the raw feed
- `combine`
  - answers whether separate symbol files have been merged into one portfolio-ready dataset
  - the output should be sorted by `symbol` and `timestamp`, which is the shape expected by `backtest-lab`
- `features`
  - answers whether the dataset is now analysis-ready rather than just normalized
  - `gap_return` captures open-to-prior-close dislocations
  - `rolling_volatility_3` gives a short-horizon realized-volatility proxy
  - `volume_ratio_3` compares current volume to recent average participation
  - `drawdown_pct` tracks how far price has fallen from its running peak

If you want a quick visual check after feature generation, the feature CSV is already plot-ready.
For example, this command plots close, rolling volatility, and drawdown from the exported file:

```bash
python - <<'PY'
import csv
from pathlib import Path

source = Path("examples/portfolio_pipeline/features.csv")
with source.open(newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle))

for row in rows[:5]:
    print(
        row["symbol"],
        row["timestamp"],
        row["close"],
        row["rolling_volatility_3"],
        row["drawdown_pct"],
    )
PY
```

That is not a chart yet, but it is the exact shape a notebook or plotting script would consume.

## Package usage

```python
from market_data_toolkit import compute_features, load_data, normalize_bars

bars = load_data("data/raw.csv")
normalized = normalize_bars(bars)
features = compute_features(normalized)
```

## Project layout

- `src/market_data_toolkit/` package source
- `tests/` regression-style test coverage
- `examples/portfolio_pipeline/` committed end-to-end data workflow example
- `docs/architecture.md` design notes and data assumptions

## Limitations

- v0.1.0 is focused on CSV-based OHLCV data
- Feature generation is intentionally simple and transparent
- Validation includes lightweight daily-session gap checks, not a full exchange calendar
- Corporate actions are not included yet
- The IBKR adapter currently targets historical bar exports, not streaming API callbacks
- Direct IBKR fetching currently assumes stock contracts (`secType=STK`) on the selected exchange
