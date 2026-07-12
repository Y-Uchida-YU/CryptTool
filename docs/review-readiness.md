# Review readiness and known limits

## Review conclusion

The repository is ready for architecture and software-safety review, but it is not a completed
trading system and contains no evidence of a profitable edge. Phase labels mean the corresponding
domain foundation and safety boundary exist; they do not imply production venue integration.

## Implemented and verified

- Fail-fast configuration with paper/dry-run defaults and multi-interlock live refusal.
- Typed market, strategy, risk, execution, portfolio and audit contracts.
- Causal feature calculations with missing-value propagation and availability/quality reporting.
- Explainable deterministic regimes, deterministic GMM baseline and an ensemble output contract.
- Five regime-gated research strategies.
- Conservative risk sizing, exposure/loss/drawdown breakers and kill switch.
- Event-driven backtest mechanics for delayed orders, book-limited partial fills, fees, funding,
  slippage, impact, stops, take-profit and liquidation.
- Purged/embargoed splits, walk-forward, resampling and overfitting diagnostics.
- Isolated quote-replay paper broker and a notification interface.
- Live execution contracts with no concrete adapter, short-lived preflight approval, RiskDecision
  binding, concurrency-safe idempotency, global limits and position-bounded reduction.
- PostgreSQL-aware CI migration check, Docker health smoke path and enforced coverage thresholds.

## Partial or intentionally deferred

- No concrete public-data exchange adapter is selected or enabled. The backfill CLI therefore
  refuses or reports that deployment composition is required.
- No venue WebSocket reconnect/reconciliation loop is shipped. Only the contracts and ingestion
  quality/checkpoint primitives exist.
- The database schema currently persists normalized OHLCV and audit events, not every listed raw
  market-data family. Production raw payload retention and quarantine tables remain to be designed
  alongside the selected data source and its licensing constraints.
- Statistical model comparison infrastructure is not an empirical comparison. GMM is the only
  implemented baseline; HMM, change-point and Markov-switching approaches remain challengers.
- Paper trading is deterministic quote replay, not a continuously deployed real-time service.
  Operational scheduling for daily/weekly reports is deployment work.
- The fixtures validate mechanics only. No representative historical dataset, untouched OOS
  period, cost calibration, walk-forward campaign or prolonged paper run has been supplied.
- No concrete execution adapter or command capable of placing a real order is included.

## Required before production consideration

1. Select a legally and contractually usable data venue for the operator and freeze its capability
   matrix.
2. Implement public REST/WebSocket adapters with recorded contract fixtures, gap recovery and raw
   payload provenance.
3. Extend migrations and retention policy for the selected funding, OI, trade, book and liquidation
   feeds.
4. Acquire point-in-time data, freeze hypotheses and run the complete untouched OOS and stress
   protocol without excluding adverse periods.
5. Run paper trading through multiple regimes and reconcile every order, fill, funding event and
   outage.
6. Treat any future concrete execution adapter as a separately reviewed project. Live mode must
   remain disabled until all acceptance evidence is durably recorded and independently approved.
