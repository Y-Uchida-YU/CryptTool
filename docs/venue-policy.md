# Venue eligibility, capability and exit policy

## Policy

The operator is assumed to reside in Japan. A venue is not limited to a Japan-registered exchange,
but it must be usable by the operator's real account without VPN, false address, third-party
identity or any other location-control evasion. Eligibility is mutable operational state, not a
hard-coded legal conclusion. Terms are rechecked at least every 30 days and every material account,
KYC, jurisdiction or product change creates an audit event in production.

All configured venues start with `execution_enabled: false`. New exposure requires `ENABLED`, a
verified operator account, verified execution API, available withdrawals, a passed minimum-size
execution smoke test, fresh terms review and no location evasion. `EXIT_ONLY` permits only a
position-bounded reduce/close path. Bybit and Binance Global execution are explicitly forbidden for
the Japan-resident profile. BTCC remains `PENDING_VERIFICATION` because the complete private
real-time/update and permission-separation conditions have not been verified from the current
official material.

This is an engineering eligibility control, not legal advice. The operator must record the actual
account result; absence of Japan from a public prohibited-country list is not account verification.

## Priority-1 capability snapshot

The checked-in matrices are dated 2026-07-12 and are exercised through
`app venue-capabilities VENUE`. Unsupported calls raise `CapabilityUnavailableError`; they never
return an empty success payload.

| Capability | Hyperliquid | Aster | Bitget | MEXC |
|---|---:|---:|---:|---:|
| Spot | yes | not asserted | yes | not asserted by contract adapter |
| Perpetual | yes | yes | yes | yes |
| Funding history | yes | yes | yes | yes |
| Open interest | yes | yes | yes | no (official endpoint returned 403 in operator environment) |
| Book snapshot/delta | yes/yes | yes/yes | yes/yes | yes/yes |
| Trades | yes | yes | yes | yes |
| Mark/index | yes/yes | yes/yes | yes/yes | yes/yes |
| Private WebSocket | yes | yes | yes | yes |
| Reduce-only | yes | yes | yes | yes |
| FOK | not asserted | yes | yes | yes |
| Subaccounts | yes | not asserted | yes | not asserted |

Sources: [Hyperliquid Info](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint),
[Hyperliquid Exchange](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint),
[Aster API](https://docs.asterdex.com/product/aster-pro/api/api-documentation),
[Bitget API](https://www.bitget.com/api-doc/common/intro), and
[MEXC Contract API](https://mexcdevelop.github.io/apidocs/contract_v1_en/).

## Cross-venue identity and time

`CanonicalInstrument` and `VenueInstrumentMapping` compare base, quote, settlement, kind, inverse
flag, multiplier, funding interval and index composition. BTC/USDT, BTC/USDC, inverse BTC/USD and
BTC/JPY are never merged by ticker alone; neither are 1000PEPE and PEPE.

Each cross-venue observation carries exchange, receive, availability and monotonic timestamps plus
a clock-offset estimate. `VenueClock` compares NTP offset with venue server time and rejects books
whose availability/skew exceeds tolerance. Cross-venue basis uses executable depth VWAP, not a
displayed last/mid price.

## Non-atomic leg policy

`LegExecutionMachine` tracks first submission, partial/final fill, hedge, unwind and halt. It
enforces first-leg timeout, maximum naked exposure, emergency hedge venue and an explicit unwind
policy. It never assumes CEX/DEX atomicity. A naked-exposure breach halts new work and requires
reconciliation before retry.

## DEX emergency exit runbook

1. Trip the kill switch and stop new exposure.
2. Compare multiple RPCs, oracle/index values, chain progress, stablecoin price, wallet nonce and
   gas balance. Record disagreements; do not silently choose the favorable source.
3. If the frontend is unavailable but the official API/contract path and chain are healthy, use the
   separately tested reduce-only contract/API procedure. Never deploy an unreviewed signing script
   during the incident.
4. If chain, oracle or consensus is degraded, use the pre-approved emergency hedge venue within the
   naked-exposure budget; otherwise unwind the reachable leg.
5. Reconcile position, fills, wallet transfers and nonce from independent read paths. Keep the venue
   `EXIT_ONLY` or `DISABLED` until a fresh terms/account/contract/smoke review passes.

DEX monitoring covers smart-contract, oracle, bridge, chain, consensus, RPC, depeg, nonce, gas and
frontend risks. CEX monitoring covers deposits/withdrawals, maintenance, API, mark/index, ADL,
insurance fund, account, KYC and jurisdiction changes. Whale wallet observations are auxiliary
regime-confidence, crowding and liquidation-risk features only; they cannot emit copy trades.

## Research strategy order

The implementation order is Cross-Venue Funding Arbitrage, Spot-Perp Carry, Cross-Venue Basis Mean
Reversion, Relative Strength Market Neutral, Liquidation Exhaustion, Flash Crash Reversal, then
Maker Strategy. Maker Strategy is deliberately deferred until venue-specific queue position,
cancel latency, adverse-selection and fee/rebate evidence is available.
