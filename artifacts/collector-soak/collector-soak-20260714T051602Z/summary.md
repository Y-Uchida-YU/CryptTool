# Collector Soak Result: collector-soak-20260714T051602Z

## Verdict

**FAIL — merge not recommended.** Live execution remained OFF.

The process is no longer running and persisted data covers approximately six hours, from 2026-07-14 05:16:02Z through 11:16:03.955137Z. SQLite integrity is valid, all 74 checkpoint rows are committed, and no WAL or SHM file remains.

The run cannot pass the Merge Gate. Four Bitget and four Hyperliquid order-book streams finished in `degraded` state with `recovery_required=true`. The legacy runner also exited without writing its expected health artifact. Because this run predates Collector Run Registry, lease renewal, and resource instrumentation, several required zero-value assertions cannot be established and are recorded as unavailable rather than inferred.

## Result

| Metric | Result |
| --- | ---: |
| Configured duration | 21,600 seconds |
| Observed duration | 21,601.955137 seconds |
| Production events | 36 |
| Experimental events | 1,961,023 |
| Quarantine events | 0 |
| Persisted collection failures | 0 |
| Final checkpoints | 74 |
| Maximum non-control checkpoint lag | 109.45698 seconds |
| Permanently degraded streams | 8 |
| Minimum inferred disconnects/reconnects | 636 / 636 |
| Remaining application leases | 0 (legacy run acquired none) |
| Live execution | OFF |

## Merge Gate

| Condition | Result |
| --- | --- |
| Process crash = 0 | **UNPROVEN** |
| Unhandled exception = 0 | **UNPROVEN** |
| Lease renewal failure = 0 | **NOT AVAILABLE — legacy run** |
| Remaining lease = 0 | PASS — no application lease was acquired and no process/DB holder remains |
| Permanently degraded stream = 0 | **FAIL — 8** |
| Checkpoint regression = 0 | **UNPROVEN** |
| CI Green | Pending for this artifact commit |
| Live Execution OFF | PASS |

## Minimum follow-up fix

No new soak was run. Before a future validation run, fix order-book recovery so a reconnect only returns to synchronized delivery after snapshot bootstrap and contiguous delta replay completes. Run the already-implemented instrumented supervisor so final status, lease renewal, exception, queue, DB latency, RSS, task, and checkpoint-regression evidence is persisted even when shutdown fails.

Evidence source: `/tmp/crypttool-accelerated-live-soak-d96567d.db` and `artifacts/collector-soak/process-audit-20260714.json` on the validation host.
