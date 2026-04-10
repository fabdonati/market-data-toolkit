# Changelog

## 0.1.0

- Added CSV ingestion and strict column validation for OHLCV datasets
- Added normalization and resampling helpers for minute-bar workflows
- Added feature engineering utilities and CLI commands for validation and exports
- Added regression-style test coverage and CI checks
- Fixed duplicate-bar close selection for out-of-order input rows

## Unreleased

- Expanded validation summaries with duplicate minute-bucket counts, daily session-gap detection, and non-monotonic symbol checks
- Added richer derived features including gap return, rolling volatility, volume ratio, and drawdown
- Added a committed end-to-end portfolio pipeline example built from IBKR-style CSV inputs
