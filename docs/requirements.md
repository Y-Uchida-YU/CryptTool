# Requirements and acceptance criteria

## Scope (Phases 0–9)

The deliverable covers configuration, structured logging, persistence schema, read/execution adapter boundaries, REST/backfill and streaming primitives, validation, causal features, deterministic and statistical regime classification, ensemble output, five strategies, risk management, event-driven backtesting, research validation, reporting, paper execution, typed live-execution contracts and fail-closed safety controls, CLI, Docker and CI. A concrete exchange execution adapter and production historical dataset remain intentionally outside the enabled system.

## Functional requirements

- Assets: BTC, ETH, SOL; timeframes: 1m, 5m, 15m, 1h, 4h, 1d.
- Preserve UTC timestamps, exchange/source, symbol, market type and observation time.
- Never replace unavailable market observations with zero.
- Store rejected observations and every correction reason in an immutable audit trail.
- Separate public market data from account/execution permissions.
- Emit multi-label regimes with primary, secondary, confidence, evidence, snapshot, duration, model/config version, and data quality.
- Only closed bars enter bar-based research. Reference distributions are shifted one observation.

## Non-functional requirements

Python 3.12, reproducible dependency lock, deterministic model seeds, typed domain boundaries, idempotent natural keys, JSON logs, secret redaction, and configurable exchange disablement. Domain code does not import SQLAlchemy or exchange clients.

## Acceptance criteria

Phase 0 documents contain the 14 requested design outputs. Phase 1 commands validate conservative settings and reject unsafe live settings. Phase 2 detects duplicate/gap/order/invalid observations and persists accepted records idempotently. Phase 3 hand-calculated tests cover returns, ATR, volatility, z-score, OI, funding, CVD, imbalance, basis and liquidation ratios, including causality. Phase 4 boundary tests cover simultaneous regimes, insufficient data, UNKNOWN and evidence-derived confidence. `ruff`, `mypy`, and tests must pass; coverage targets are enforced progressively as later execution modules arrive.

## Roadmap

1. Phase 0: documents and threat/data availability assessment.
2. Phase 1: safe runtime foundation and ports/adapters.
3. Phase 2: concrete public-data adapters, checkpointed ingestion, validation and storage.
4. Phase 3: causal feature computation and quality report.
5. Phase 4: rules, GMM baseline and evidence-quality ensemble contract.
6. Phase 5: five strategies, regime gates and risk-first sizing/halts.
7. Phase 6: event backtester, conservative execution and portfolio accounting.
8. Phase 7: leakage-controlled validation, overfitting diagnostics and reports.
9. Phase 8: isolated paper broker, virtual fills, notifications and periodic reports.
10. Phase 9: guarded live interface, disabled adapter, preflight and contingency controls; no venue implementation.
