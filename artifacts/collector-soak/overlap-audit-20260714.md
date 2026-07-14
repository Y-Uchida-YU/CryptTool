# Collector overlap audit — 2026-07-14

- `collector-soak-20260714T025719Z` and `collector-soak-20260714T025935Z` used different SQLite files, but ran identical public venue/instrument/channel workloads concurrently and used the legacy, non-namespaced checkpoint layout.
- Their database records are not combined and are not represented as two independent long-duration acceptance results.
- Both processes received `SIGINT` and completed graceful collector shutdown. No `SIGKILL` was used.
- Exact task counts immediately before shutdown were not captured by the legacy runner and are explicitly recorded as unavailable.
- The retained run is `collector-soak-20260714T051602Z`, using `/tmp/crypttool-accelerated-live-soak-d96567d.db`, Hyperliquid/Bitget, and BTC/ETH/SOL/HYPE for six hours.
- Live execution remained OFF.
