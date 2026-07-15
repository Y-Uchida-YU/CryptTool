# R4 Market Data Capability Certification

R4 certifies public market data separately for each venue, capability, and canonical instrument.
It never enables live execution and it never promotes a capability from a matrix edit. Collection
uses an isolated database and stores all untrusted observations as experimental events until the
certification, audit, artifact, commit, adapter version, and expiry gates agree.

## Official contract sources

- [Hyperliquid REST](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
  uses `POST https://api.hyperliquid.xyz/info` with
  `metaAndAssetCtxs`, `fundingHistory`, and `candleSnapshot`. Its public WebSocket is
  `wss://api.hyperliquid.xyz/ws`; `trades` and `l2Book` are snapshot feeds. Hyperliquid funding is
  paid [hourly](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding).
- [Bitget REST](https://www.bitget.com/api-doc/classic/contract/market/Get-Current-Funding-Rate)
  uses the `/api/v2/mix/market` current/history funding, open-interest, symbol-price,
  and candles endpoints. Its public WebSocket is `wss://ws.bitget.com/v2/ws/public`. The full
  `books` channel starts with a snapshot and then sends updates. Funding intervals are read from
  `fundingRateInterval` instead of assuming that every instrument uses eight hours.

The endpoint, request/schema contract, units, timestamp semantics, fixture hash, normalization test
node, measured tolerances, and evidence hashes are included in each certification artifact.

## Safety and promotion

`MarketDataCertificationService` may issue PASS, FAIL, or INSUFFICIENT_EVIDENCE. Only
`CapabilityPromotionService` can create an instrument-scoped `TrustedCapabilityRecord`, and only
after the stored certification and evidence gates match. An expired or adapter-mismatched record
cannot admit a production event. Historical experimental events are not rewritten as production
events.

Funding Carry snapshots use `StrategyDataRequirement`; their membership is limited to the two
required venues and Tier 1 event types. Missing Order Book data therefore does not make a Funding
Carry snapshot ineligible, while missing per-venue Tier 1 history does.

Strict Paper remains an operator decision. R4 can return `READY_FOR_OPERATOR_APPROVAL`, but
`require_operator_approval` prevents automatic activation. The default configuration remains
Observation-only with live execution disabled.

## Commands

```console
app run-market-data-certification --config configs/market-data-certification.yaml
app verify-market-data-certification --certification-id ID
app capability-certification-status
```

The 30-minute command is intentionally a long-running process. Start it once, record its Run ID
and PID, and inspect it only after the operator asks for completion review.
