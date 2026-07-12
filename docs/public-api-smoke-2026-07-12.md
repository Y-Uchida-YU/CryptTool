# Priority-1 public API smoke — 2026-07-12

This is a public-data connectivity record, not execution eligibility and not evidence of an edge.
No credential, private endpoint, order or location-evasion mechanism was used.

| Venue | Market metadata | REST book | Funding | OI | Public WS book |
|---|---:|---:|---:|---:|---:|
| Hyperliquid | 232 markets | pass | 13 records | pass | pass, 20x20 |
| Aster | 524 markets | pass | 1,000 records | pass | pass, 20x20 |
| Bitget | 697 markets | pass | 100 records | pass | pass, 500x500 |
| MEXC | 968 markets | pass | pass | unavailable (403) | pass |

The counts are transient observations and must not be treated as stable capability constants.
MEXC OI is marked unavailable in the operator environment and raises
`CapabilityUnavailableError`; the system did not retry through a VPN, proxy or alternate region.

Repeat metadata checks with `app public-data-smoke VENUE`. WebSocket checks intentionally remain a
separate operational probe because CI must not depend on external venues. A passing public probe
does not alter `execution_enabled`, `operator_account_verified`, withdrawal verification or the
minimum execution-smoke gate.
