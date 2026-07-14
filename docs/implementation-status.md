# Implementation status through Research Data Operations Phase R2.1

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

Phase R1 connects immutable raw events and quarantine records to point-in-time datasets, measured
data quality, causal features, regimes, the three in-scope strategies, portfolio risk, event-driven
fills, walk-forward OOS, cost stress, overfitting diagnostics, capital feasibility, acceptance and
hash-verified research artifacts under one run identity. Liquidation, Whale and all other strategy
hypotheses are explicitly deferred. The primary and challenger venues remain data-only for this
phase; live execution remains disabled and no concrete execution adapter is introduced.

Phase R2 connects the existing public REST and WebSocket adapter contracts to explicit production
and experimental recording stores. Production membership requires a current LIVE_VERIFIED
capability; other implemented feeds are retained only as experimental observations. Raw responses,
normalized events, quarantine records, restart checkpoints, point-in-time instrument rules and
ordered daily snapshot membership have separate durable identities. Finalized membership is
immutable, hash-verifiable and reproducible, and database foreign keys bind research runs and
artifacts to the exact data snapshot and strategy-scoped frozen hypothesis. Collection remains
opt-in (`collection_enabled: false` by default), and live execution remains off.

Phase R2.1 makes collection stream-scoped by venue, instrument, event type and channel. REST
pollers, continuous WebSocket readers, raw persistence and checkpoint writes are separate workers;
restart cursors preserve timestamps, trade IDs and order-book connection/sequence recovery state.
Order-book events cannot become research-eligible until the adapter's snapshot/delta reconciler has
completed a continuous replay. Gap, stale and unsynchronized observations remain durable but are
quarantined from production membership. Instrument-rule fields without endpoint/hash/time evidence
remain UNKNOWN, so capital and cost evidence fail closed. Finalized snapshots explicitly distinguish
research-eligible from control-only or incomplete datasets, and database triggers protect finalized
rows as well as membership. Collection and live execution both remain disabled by default.

## Verification snapshot

Test counts and coverage values are intentionally not copied into this document. CI generates
`artifacts/build-verification/latest.json` from the JUnit and coverage outputs and uploads the whole
`artifacts/build-verification/` directory as the `build-verification-<commit SHA>` CI artifact.
That commit-bound artifact is the sole source for current verification counts and percentages.

The checked-in tests and their generated artifacts validate software behavior only. Fixture data is
not representative market history and must not be cited as evidence of a trading edge.

## Remaining before an edge can be evaluated

- Select an exchange/data vendor that the operator is currently permitted to use.
- Operate the opt-in R2 collector against licensed production feeds and retain its daily verified
  snapshot manifests.
- Acquire licensed, point-in-time OHLCV, funding, OI, liquidation and book data with survivorship
  and outage records.
- Freeze production hypotheses and parameter grids before observing final results.
- Run the R1 OOS, stress, leave-one-out and overfitting suite on the licensed snapshot.
- Verify capital feasibility against actual venue minimums and historical fee tiers.
- Operate paper trading through multiple market regimes and reconcile every virtual fill.

Until those items are complete, the correct conclusion is **no demonstrated reproducible edge and
not ready for live trading**.

See `review-readiness.md` for the precise implemented/partial/deferred boundary. Phase numbering in
this repository means a safe tested foundation exists for that phase; it does not mean external
venue integration or empirical edge validation has been completed.
