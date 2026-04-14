# Architecture Notes

## Design goals

- Keep the data model small and explicit
- Support both importable Python APIs and shell-friendly workflows
- Prefer deterministic transforms over opaque helper utilities

## Data model

The core unit is a `Bar`, a typed dataclass representing one OHLCV observation:

- `symbol`
- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`

The loader is strict about required columns so failures happen close to the data source instead
of later in the pipeline.

## Transform pipeline

The expected workflow is:

1. `load_data()` to parse a CSV source
2. `normalize_bars()` to standardize symbol casing and merge duplicate timestamps
3. `resample_bars()` when a coarser interval is needed
4. `compute_features()` to derive downstream analytics fields such as returns, gap returns, volatility, and drawdown

## Timestamp assumptions

- Timestamps are parsed with Python's ISO-8601 handling
- Duplicate timestamps are merged at minute granularity
- Resampling currently uses timestamp floor bucketing
- Daily-session validation uses business-day gaps, not a full exchange calendar

## CLI design

The CLI mirrors the package API rather than inventing a separate execution model:

- `mdtk validate`
- `mdtk ingest`
- `mdtk features`
- `mdtk ibkr-ingest`
- `mdtk combine`

That keeps it easy to verify behavior in tests and reuse the same logic from notebooks or
other packages.

## Broker-specific adapters

Vendor-specific loaders should convert raw source files into the same generic `Bar` schema used
everywhere else in the repo. The current IBKR adapter assumes a historical-bar CSV with the
standard bar fields:

- `Date`
- `Open`
- `High`
- `Low`
- `Close`
- `Volume`

Optional columns such as `Average`, `BarCount`, and `symbol` are ignored unless symbol
information is needed during import.

For direct historical fetches, the toolkit follows the same principle: request broker-specific
data through IBKR's API, convert the returned bars into the generic `Bar` model, and export the
result as a normalized CSV that downstream tools can consume without broker-specific logic.

When working with multiple symbols, the toolkit keeps each normalized dataset schema-identical so
they can be merged deterministically into a single portfolio-ready file for downstream research.

## Validation and feature scope

The current validation and reporting layer includes:

- duplicate minute-bucket detection highlights rows that collapse during normalization
- non-monotonic symbol detection flags raw input ordering problems
- daily-session gap detection highlights obvious missing business days in daily datasets

Feature generation stays transparent and table-friendly:

- simple returns
- gap returns
- rolling mean
- rolling volatility
- volume ratio and volume z-score
- drawdown from the running peak
