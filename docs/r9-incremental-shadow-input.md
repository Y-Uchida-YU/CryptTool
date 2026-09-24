# R9 Incremental Shadow Input Evidence

## Confirmed root cause

The R8 source called `list_experimental_events()` on every 10-second refresh. PostgreSQL
returned every ORM row including the TOAST-backed raw payload; Python then materialized,
filtered, sorted, deserialized, and checked an ever-growing processed-event set again.

The saved six-hour run contained 226,708 scoped events. Of these, 221,415 were Bitget
order-book deltas with an average raw payload of about 41 KB (about 8.7 GB in aggregate).
The source stopped advancing after 44,247 observed events while collection continued to the
table tail. The primary root cause is the full-table/TOAST payload scan (A); the secondary
root cause is ORM/Python materialization (B). JSON deserialization (C), sorting (D), and the
processed-event set (E) amplified the same unbounded scan. Evidence did not indicate a DB
connection fault (F) or an independent scheduler fault (G).

Old per-refresh complexity was `O(N log N)` time and `O(N * payload_size)` memory, repeated
as `N` grew. The incremental path uses four indexed keyset streams and returns at most the
configured batch size: `O(log N + K)` query work and `O(K)` materialization, where `K=1000`.
The small merge sort is bounded to at most 1,000 rows.

## Batch-size rationale

The saved run averaged about 308 scoped events per 30-second collector cycle. A 1,000-event
batch therefore provides about 3.2x cadence headroom while bounding an average raw-payload
working set near 41 MB before deserialization. The measured Python peak was lower because
rows are divided across four keyset streams and released batch by batch.

## Saved-evidence benchmark

The benchmark used the completed six-hour database without external API access or raw-row
mutation. The composite benchmark index was created inside a transaction and rolled back.

- Old full scan: six-hour operational runtime did not reach the tail; last observed count
  44,247. Samples were 0.581 s for 1,000 rows and 2.679 s for 10,000 rows before the
  unbounded materialization became impractical.
- Incremental: 226,708/226,708 rows; 311.054 s wall time; batch latency p50 331.259 ms,
  p95 408.900 ms, max 557.726 ms; peak Python memory 23,461,419 bytes.
- Milestones: 44,247 at 52.950 s; 100,000 at 135.993 s; 200,000 at 275.752 s.
- Final cursor: `2026-08-10T04:56:12.293892+00:00` /
  `bitget-4f9921153036a2da0cdfdcfc9657fd4833fd011190057a534d1b25bada165a34`.
- Final lag: 0 seconds.

Machine-readable results are in `artifacts/r9-incremental-shadow-benchmark.json`.

## Recovery and health semantics

Cursor identity is `(run_id, venue, instrument, event_stream)` and ordering is
`(available_at, event_id)`. Funding and order-book state is processed before the cursor and
checkpoint are committed atomically. Failed processing leaves the cursor unchanged, so
delivery is at least once and existing source-pair/candidate identities retain dedup control.

Bitget book levels, snapshot boundary, connection epoch, and sequence are checkpointed.
On restart, applied delta identities are recovered from the snapshot boundary through the
durable order-book cursor before new deltas are accepted. Delta-only initialization remains
invalid.

Observation coverage is measured separately from collector uptime. If scoped DB input keeps
growing while any venue/stream cursor exceeds the derived stall window (two collector-plus-
refresh cycles; 80 seconds at the baseline cadence), Shadow Runtime is `DEGRADED` while all
execution systems remain disabled.
