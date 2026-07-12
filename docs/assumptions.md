# Assumptions and defaults

- Jurisdiction and exchange terms can change. The operator must confirm eligibility before enabling an adapter. Data access does not imply permission to trade.
- Bybit is configured disabled and considered a public-data research candidate only; no execution implementation is supplied. Binance public market-data endpoints are a data-only candidate, not a required dependency. Kraken/Coinbase and Japanese registered venues require separate capability adapters because derivative fields differ.
- PostgreSQL 16 is production storage; SQLite is the zero-dependency development default.
- Observation timestamps represent event/exchange time in UTC; ingestion time is separate. Naive timestamps are rejected.
- Bars are usable only after closure. No intra-bar high/low is visible to a close-time signal before the close event.
- Statistical fitting uses only a completed training window. Thirty observations is only a software minimum; research acceptance requires substantially more and full regime coverage.
- Initial capital scenarios are 100, 300 and 1,000 USDT-equivalent. Base currency conversion and tax reporting are outside Phase 0–4.
- Paper mode on, dry-run on, live mode off, leverage <=1, risk/trade 0.25%, daily 1%, weekly 3%, drawdown stop 8%, positions 3, losing-streak stop 5.
- Missing required inputs yield `NaN`, reduced quality or `UNKNOWN`; never zero.
- Confidence is an evidence strength multiplied by data quality, discounted when no fitted statistical model is available. It is not a probability of profit.

## Unresolved items

Venue eligibility, symbol mappings, historical liquidation licensing, reliable predicted-funding history, order-book retention depth, production data vendor, fee tiers, tax lot method, deployment region, recovery objectives, and human kill-switch operator remain deployment decisions in configuration/runbooks.
