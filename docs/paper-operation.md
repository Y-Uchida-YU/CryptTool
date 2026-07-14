# Continuous Research & Paper Operation

R3 runs public market-data collection, snapshot/research scheduling, eligibility, signal observation, simulated paper execution, risk, attribution, and reporting under one durable run ID.

## Safety defaults

- `live_trading: false`
- `live.enabled: false`
- `paper_trading: true`
- `continuous_paper.observation_only: true`
- no execution adapter or trading credential is used
- PostgreSQL is mandatory outside tests/local smoke

Start with `app start-paper-operation --config configs/paper-operation.yaml`. The service restores cash ledgers, positions, open paper orders, eligibility, and run state from PostgreSQL after restart. Request graceful shutdown with `app stop-paper-operation --run-id RUN_ID`.

Docker Compose uses the `paper` profile. The sample LaunchAgent and Windows Task Scheduler definitions restart the process after host or process failure. Supply database credentials through the runtime environment; do not write passwords or Discord webhook URLs into checked-in files.

Daily reports are written to `artifacts/operations/YYYY-MM-DD/`. Promotion output is advisory only; R3 cannot enable live execution.
