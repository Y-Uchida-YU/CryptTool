# Order Book Recovery Live Validation

- Run ID: `orderbook-recovery-20260714T120525Z`
- Validated commit: `47da05bcc6201d1cba528021465893e708c3af5a`
- Live execution: **OFF**
- Verdict: **PASS**

## Stage 1 — Venue-specific forced reconnect

Hyperliquid BTC and Bitget BTC were each observed for at least 30 minutes after three forced reconnects.

| Venue | Semantics | Forced recovery | Continuous messages | Final state | Recovery required |
| --- | --- | ---: | ---: | --- | --- |
| Hyperliquid | snapshot only | 3/3 | 336 | SYNCHRONIZED | false |
| Bitget | snapshot and delta | 3/3 | 18,537 | SYNCHRONIZED | false |

All six injected disconnects were client-initiated shutdowns. No exception, stale timeout, server close, or permanently degraded stream was observed.

## Stage 2 — All target streams

Hyperliquid and Bitget BTC/ETH/SOL/HYPE were run concurrently for 60 minutes.

| Venue | Instrument | Messages | Final state | Recovery required | Permanently degraded |
| --- | --- | ---: | --- | --- | ---: |
| Hyperliquid | BTC | 671 | SYNCHRONIZED | false | 0 |
| Hyperliquid | ETH | 671 | SYNCHRONIZED | false | 0 |
| Hyperliquid | SOL | 671 | SYNCHRONIZED | false | 0 |
| Hyperliquid | HYPE | 671 | SYNCHRONIZED | false | 0 |
| Bitget | BTC | 37,062 | SYNCHRONIZED | false | 0 |
| Bitget | ETH | 37,278 | SYNCHRONIZED | false | 0 |
| Bitget | SOL | 38,490 | SYNCHRONIZED | false | 0 |
| Bitget | HYPE | 39,491 | SYNCHRONIZED | false | 0 |

Each connection sent and received 179 application heartbeats. The only disconnect was the expected client shutdown at test completion. There were no unhandled exceptions.

## Merge gate

- All eight order-book streams synchronized: PASS
- Permanently degraded streams: 0
- Recovery-required streams: 0
- Forced reconnect recovery: PASS (6/6 total)
- Unhandled exceptions: 0
- Lease renewal failures: 0
- Checkpoint regressions: 0
- Live execution: OFF

