# CryptBot Market Regime Engine

Research-first crypto market regime, strategy validation, event-driven backtesting and paper
trading platform. It is designed to test whether an edge exists, not to manufacture an attractive
backtest.

Current implementation provides tested foundations across Phase 0–9:

- normalized market data contracts, validation and checkpointed ingestion primitives;
- causal feature generation and feature quality/availability reports;
- deterministic rules, reproducible GMM baseline and explainable ensemble regimes;
- trend, mean-reversion, flash-crash reversal, funding-extreme and relative-strength strategies;
- conservative sizing, exposure/loss/drawdown limits, circuit breaker and kill switch;
- causal event-driven fills with fees, spread, slippage, impact, partial/unfilled orders, funding,
  margin, stop/TP and liquidation;
- purged/embargoed validation, rolling/anchored walk-forward, Monte Carlo, bootstrap, PBO,
  Deflated Sharpe, Reality Check and parameter stability;
- Markdown/CSV/JSON research reports and an isolated paper broker with optional Discord adapter.
- typed live-execution contracts, 16-check preflight, idempotency, hard order limits, kill switch and
  durable audit integration, with only a permanently disabled execution adapter shipped.

Concrete live execution is not implemented and remains disabled.
Concrete exchange ingestion, production WebSocket operation and representative market validation
are also still required; see [review readiness](docs/review-readiness.md) for the exact boundary.

Venue policy is account- and terms-based rather than domestic-only. Priority public adapters cover
Hyperliquid, Aster, Bitget and MEXC, with domestic public adapters for GMO Coin, bitbank and
bitFlyer. Every venue starts execution-disabled; BTCC is pending verification, while Bybit and
Binance Global execution are forbidden for the Japan-resident profile. See the
[venue policy and emergency runbook](docs/venue-policy.md).

## Setup

```bash
uv sync --all-extras
uv run app validate-config
uv run pytest --cov
```

The repository pins Python 3.12 through `.python-version` and declares `<3.13` to keep local,
Docker and CI behavior aligned.
`configs/default.yaml` is the checked-in baseline; explicit constructor values, `APP_` environment
variables and `.env` override it, in that order. Secrets must stay in the environment or `.env`.

If an editable-install path is not recognized by the local Python build, prefix development
commands with `PYTHONPATH=src`.

For the PostgreSQL smoke environment, copy `.env.example` to `.env`, replace
`CRYPTBOT_DB_PASSWORD`, then run `docker compose up --build`. Compose injects the same database
credential into PostgreSQL and the application and executes the read-only health check.

## Reproducible mechanical smoke run

These fixtures verify mechanics only; they are not evidence of profitability.

```bash
uv run app run-backtest tests/fixtures/backtest_events.json
uv run app run-walk-forward \
  --observations 100 --train 40 --validation 10 --test 10 --purge 2 --embargo 2
uv run app run-monte-carlo tests/fixtures/net_returns.csv --simulations 1000
uv run app generate-report tests/fixtures/equity.csv \
  --trades-path tests/fixtures/trades.csv
uv run app paper-trade tests/fixtures/paper_quotes.csv --quantity 0.1
uv run app live-preflight  # intentionally exits 2 with the disabled adapter
uv run app venue-status
uv run app venue-capabilities hyperliquid
uv run app public-data-smoke hyperliquid  # public metadata only
```

The generated research report deliberately returns `INSUFFICIENT_EVIDENCE` until external,
untouched OOS and all acceptance-gate evidence is supplied.

## Safety

- Paper mode and dry-run are on by default.
- Live mode requires multiple explicit interlocks but has no concrete live adapter.
- Exchange data and execution capabilities are separate and disabled independently.
- Missing observations remain missing; they are never silently converted to zero.
- `.env` is ignored, secrets are redacted, and withdrawal permissions are never required.

See [implementation status](docs/implementation-status.md), [architecture](docs/architecture.md),
[backtesting methodology](docs/backtesting-methodology.md), and
[paper trading](docs/paper-trading.md). Phase 9 safety behavior is documented in the
[live execution runbook](docs/live-trading-interface.md).
