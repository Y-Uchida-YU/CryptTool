# R10: independent BTC / SOL funding carry observation

Base: `bdebc28f14be9e9ebaae7152e6f5b35a2212de09` (R9).
No public API observation run is part of this PR.

## Scope and invariants

The shadow allowlist is BTC and SOL, with any non-empty unique subset permitted.
The collector and evaluator instrument sets must match. BTC-only callers remain
supported; the canonical default remains BTC. The strict/research requirement is
not broadened, and adding SOL does not promote any capability.

The evaluator's arithmetic is unchanged. Funding sign, interval normalization,
leg selection, VWAP, fees, slippage, basis, one-hour horizon, timing thresholds,
net-edge threshold and candidate-only experimental semantics remain unchanged.
The existing five saved BTC smoke observations still exercise exact candidate ID,
net-edge and break-even regression assertions.

## Runtime and persistence

`FundingCarryShadowRuntime` owns one `FundingCarryShadowInstrumentRuntime` per
configured instrument. Each owns its latest funding/books, book state machine,
invalid reasons, pair identity, cursor positions, timing and coverage state.
`FundingCarryShadowInputSource` remains a compatibility name for a single runtime.

Cursor keys remain `(run_id, venue, instrument, event_stream)`: eight cursors for
BTC+SOL. Migration 0021 changes checkpoint keys to `(run_id, instrument)`. Existing
R9 checkpoints are assigned to BTC without modifying their evidence. Durable
candidate metrics get an instrument dimension; the empty instrument denotes the
legacy aggregate. R9 aggregate counters are also backfilled into the BTC view.
Downgrade refuses to collapse independent evidence; archival is required first.

Each instrument receives one bounded batch per refresh, independent of the other
instrument's backlog. At the default 1,000 events per instrument, each of its four
streams has a reserved 250-event quota. Unused capacity is not borrowed: maximum
2,000 events across BTC+SOL per refresh. Within each stream, ordering remains
`available_at, event_id`. Payload queries stay bounded and use the existing R9
index. Actual backlog uses filtered SQL COUNT, not payload materialization.

Processing commits each instrument independently after normalization, book state
application and latest-input update. A failed instrument returns no batch; its
peer still progresses. Cursor and processed/coverage state roll back on failure.
Delta source identities are reconstructed from the snapshot-backed boundary on
restart and rollback. Candidate identity includes instrument already and is not
changed; source-pair identity now explicitly includes instrument too.

## Metrics

Artifacts expose `by_instrument` for input, pairing, dedup and book-state metrics.
Input metrics include per-stream lag/backlog/status and per-instrument event
counts, coverage and status. Lag is the maximum **per-stream** cursor-to-DB-tail
distance, so a busy book cannot hide stalled funding. Streams with no queued
input do not cause false cursor stalls. Any degraded instrument degrades global
status. A processing failure also degrades that instrument without reusing its
previous batch.

- `shadow_input_backlog_event_count`: actual unseen events (filtered DB count).
- `shadow_input_backlog_stream_count`: streams with at least one unseen event.
- `shadow_input_backlog_estimate`: legacy alias for pending stream count.

Coverage is the first-to-last matched-input span divided by elapsed observation
time, not the proportion of successful ticks. Start time is persisted across
restart. Aggregate coverage is the minimum instrument ratio, not a union that
could hide one stopped instrument. As in R9, this endpoint-span metric does not
measure outages inside that span; per-stream stalls and lag complement it.

Candidate, timing-input and metric records identify instrument. Atomic artifact
finalization now includes `funding-edge-timeseries.csv` and
`economics-timeseries.csv`, each with an explicit instrument column. CSV economics
use existing `entry_basis_cost` and `economics_notional` names and include rejected
records whose economics were calculated. These are diagnostic exports only.

## Configuration and safety

`configs/funding-carry-cross-sectional-shadow.yaml` selects BTC/SOL,
Hyperliquid/Bitget, funding/snapshot/delta inputs and notional 10. It keeps
funding age/skew 40/30 seconds, book age/skew 30/5 seconds and both taker fees
0.0006. The one-hour evaluator horizon and zero minimum edge are unchanged.
Nothing starts automatically and the example must be given an isolated durable
database/state directory before a separately authorized observation run.

Shadow worker allowlist remains collector supervision and shadow evaluation.
No SnapshotFinalizer, research, paper signal/order/fill, promotion or execution
worker is added. Fixture integration asserts all safety counters zero, strict
paper NOT_READY, live OFF, credentials absent and no execution adapter.

## Validation

New offline tests cover memory and SQL repositories, independent cursors,
cross-instrument rejection, gaps/reconnects, fairness in both directions,
failure/rollback isolation, restart recovery, metrics and worst-state status,
deterministic evaluations, durable dedup, artifact safety, migration preservation,
and actual CLI wiring with mocked public adapters (BTC/SOL and SOL-only).
Existing safety tests remain unchanged. The synthetic R9 backlog fixture now
honors venue/event-type scope because R10 requests per-stream backlog counts;
the 226,708-event tail assertion remains intact.

Run `uv run ruff format --check .`, `uv run ruff check .`, `uv run mypy src`,
`uv run bandit -r src`, and `uv run pytest`. CI additionally enforces coverage
and upgrades/checks the schema against PostgreSQL. Keep the PR Draft until all
checks are green; no live smoke, certification or long-duration run is required
or authorized by this PR.

Local validation (2026-09-26): 634 tests passed, including 33 new parametrized
R10 cases. Combined line/branch coverage 85.57%; all safety-critical coverage
thresholds passed. Ruff format/check, strict mypy and Bandit passed. No external
API run or certification was performed.
