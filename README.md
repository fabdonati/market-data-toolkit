# market-data-toolkit

`market-data-toolkit` is a lightweight Python package for ingesting, validating, normalizing,
and enriching OHLCV-style bar data.

## Why this project

Most market-data workflows start with small utility code that quietly becomes critical:
parsing vendor exports, standardizing timestamps, resampling bars, and generating features
for downstream models or backtests. This project packages those tasks into a typed API and
a small CLI.

## Features

- Load OHLCV data from CSV into typed bar objects
- Normalize duplicate timestamps into canonical minute bars
- Resample bars into coarser intervals
- Generate simple derived features such as returns and rolling means
- Validate and export datasets from the command line

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

Normalize a dataset:

```bash
mdtk ingest data/raw.csv --output data/normalized.csv
```

Compute features:

```bash
mdtk features data/raw.csv --output data/features.csv
```

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
- `docs/architecture.md` design notes and data assumptions

## Limitations

- v0.1.0 is focused on CSV-based OHLCV data
- Feature generation is intentionally simple and transparent
- Corporate actions and trading-calendar logic are not included yet
