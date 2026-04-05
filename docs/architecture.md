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

The loader is intentionally strict about required columns so failures happen close to the
data source instead of later in the pipeline.

## Transform pipeline

The expected workflow is:

1. `load_data()` to parse a CSV source
2. `normalize_bars()` to standardize symbol casing and merge duplicate timestamps
3. `resample_bars()` when a coarser interval is needed
4. `compute_features()` to derive downstream analytics fields

## Timestamp assumptions

- Timestamps are parsed with Python's ISO-8601 handling
- Duplicate timestamps are merged at minute granularity
- Resampling currently uses timestamp floor bucketing

## CLI design

The CLI mirrors the package API rather than inventing a separate execution model:

- `mdtk validate`
- `mdtk ingest`
- `mdtk features`

That keeps it easy to verify behavior in tests and reuse the same logic from notebooks or
other packages.
