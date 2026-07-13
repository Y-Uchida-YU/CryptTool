# Research Pipeline R1

R1 is an offline, fail-closed research path. It never enables live execution and does not compose
an execution adapter. One immutable `ResearchRunIdentity` binds the commit, canonical config hash,
data snapshot, frozen hypothesis and strategy version to every generated artifact.

The only in-scope strategy IDs are `funding_carry`, `cross_venue_basis` and
`btc_sol_relative_strength`. Liquidation, whale and all other strategy IDs are reported as
`DEFERRED`. Hyperliquid, Bitget, Aster and MEXC are primary data sources; dYdX, Paradex and Lighter
are data-only challengers.

Run a migrated database and then execute:

```text
app run-research-pipeline --config CONFIG
app run-walk-forward-backtest --config CONFIG
app generate-research-report --run-id RUN_ID
```

The config must provide the commit SHA, snapshot ID, cutoff, frozen hypothesis, explicit venue,
instrument and event-type universes, raw events, instrument rules and walk-forward sizes. Raw event
payload strings are hashed and stored unchanged. Values become eligible only when
`available_at <= decision_time`; events past the snapshot cutoff are excluded and normalization is
fit separately on each walk-forward training window.

Every completed run writes a hash-verified manifest plus Markdown, JSON, walk-forward CSV, stress
CSV, trade Parquet and rejection Parquet artifacts below `artifacts/research/<run_id>/`. A missing
diagnostic produces `INSUFFICIENT_EVIDENCE`; it never defaults to `PASS`.
