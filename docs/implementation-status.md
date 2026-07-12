# Implementation status after Phase 9

## Completed

Phase 5 implements five independent strategy hypotheses. Every strategy either emits an auditable
signal or an explicit gate/rejection reason. UNKNOWN, low-confidence and low-quality regimes cannot
open risk. Risk management covers sizing, daily/weekly losses, drawdown, gross/symbol/exchange
exposure, leverage, position count, losing streaks, data/API/WebSocket/spread/divergence failures,
cooldown, circuit breaker and manual kill switch.

Phase 6 implements a stable event queue and preserves signal, order creation, submission and fill
times. A fill requires a strictly later market event. It supports market/limit, GTC/IOC/FOK,
post-only/reduce-only, fees, funding, spread, slippage, impact, tick/lot/minimum notional, partial and
unfilled orders, margin, long/short accounting, stop/TP and liquidation.

Phase 7 implements chronological train/validation/OOS splits, purge/embargo, rolling/anchored
walk-forward, seeded block bootstrap and Monte Carlo, CSCV/PBO, Deflated Sharpe, a bootstrap Reality
Check, parameter plateaus, requested performance metrics, stratified results and Markdown/CSV/JSON
artifacts. Missing evidence cannot produce a passing acceptance verdict.

Phase 8 implements an in-memory isolated paper broker. Orders fill only on later quotes and visible
liquidity participation is capped. Data-quality failure, sequence gaps, stream disconnects and the
kill switch halt new risk. Discord is optional and disabled without an HTTPS webhook. No live
execution method is called.

Phase 9 implements typed order/acknowledgement/position contracts, a permanently disabled adapter,
a 16-check fail-closed preflight with a short TTL, per-order allowlists and limits, risk-decision
identity/size binding, serialized idempotent submission, adapter protocol checks, global open-order
limits, kill-switch cancel-all, position-bounded reduce-only contingency actions and durable
sanitized audit events. No concrete venue execution adapter is included.

## Verification snapshot

- 140 automated tests pass on Python 3.12, including async integration and Hypothesis invariants.
- Overall line coverage: 95.03%.
- Risk coverage: 100.00%.
- Execution/backtest coverage: 98.94%.
- Regime coverage: 95.30%.
- Phase 9 live-interface coverage: 99.00% and enforced at 95% in CI.
- Paper execution coverage: 98.57% and enforced at 95% in CI.
- `ruff check`, `ruff format --check`, strict `mypy`, Alembic upgrade/check and Bandit pass.
- Mechanical fixture: two causal fills and final equity 1000.166610 from 1000 initial cash.
- Paper fixture: one later-event fill, final equity 999.991838776, zero live orders.
- Monte Carlo fixture: 0% ruin and 44.5% terminal-loss frequency at 1,000 simulations.
- Research fixture verdict: `INSUFFICIENT_EVIDENCE`.

These values test software behavior only. The tiny fixtures were not sampled from a representative
market history and must not be cited as a trading edge.

## Remaining before an edge can be evaluated

- Select an exchange/data vendor that the operator is currently permitted to use.
- Implement and contract-test that concrete public-data adapter.
- Acquire licensed, point-in-time OHLCV, funding, OI, liquidation and book data with survivorship
  and outage records.
- Freeze hypotheses and parameter grids before observing final results.
- Run the complete OOS, cost-stress, leave-one-period/asset-out, walk-forward and overfitting suite.
- Verify 100/300/1,000 capital feasibility against actual venue minimums and historical fee tiers.
- Operate paper trading through multiple market regimes and reconcile every virtual fill.

Until those items are complete, the correct conclusion is **no demonstrated reproducible edge and
not ready for live trading**.

See `review-readiness.md` for the precise implemented/partial/deferred boundary. Phase numbering in
this repository means a safe tested foundation exists for that phase; it does not mean external
venue integration or empirical edge validation has been completed.
