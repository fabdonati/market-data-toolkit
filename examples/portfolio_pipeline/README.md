# Portfolio Pipeline Example

This example shows a small daily-bar workflow that starts from IBKR-style CSV exports and ends
with a portfolio-ready dataset that `backtest-lab` can consume directly.

From the repo root:

```bash
python -m market_data_toolkit.cli ibkr-ingest examples/portfolio_pipeline/aapl_ibkr.csv --symbol AAPL --output examples/portfolio_pipeline/aapl_normalized.csv
python -m market_data_toolkit.cli ibkr-ingest examples/portfolio_pipeline/msft_ibkr.csv --symbol MSFT --output examples/portfolio_pipeline/msft_normalized.csv
python -m market_data_toolkit.cli combine examples/portfolio_pipeline/aapl_normalized.csv examples/portfolio_pipeline/msft_normalized.csv --output examples/portfolio_pipeline/portfolio.csv
python -m market_data_toolkit.cli validate examples/portfolio_pipeline/portfolio.csv
python -m market_data_toolkit.cli features examples/portfolio_pipeline/portfolio.csv --output examples/portfolio_pipeline/features.csv
```

The combined output should match `expected_portfolio.csv`.

To hand the data to `backtest-lab`, run from that repo:

```bash
python -m backtest_lab.cli /Users/fabrizio/Dev/Finance/market-data-toolkit/examples/portfolio_pipeline/portfolio.csv \
  --input-format market-data-toolkit \
  --strategy moving-average \
  --short-window 2 \
  --long-window 3
```
