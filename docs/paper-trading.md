# Phase 8 paper trading

Paper trading is the only enabled trading mode. The paper broker accepts market and limit orders,
but evaluates them only on a later quote. Fill quantity is capped by visible touch liquidity and a
configurable participation fraction. Market fills pay spread, adverse slippage and taker fees;
limit orders can remain unfilled or partially filled. Funding changes cash at an explicit event.

Low-quality, stale, duplicated/out-of-order data and the kill switch prevent new fills. Gross
exposure is capped at 1x. Every request, rejection, fill, fee, slippage, funding and account snapshot
is available for structured audit. Daily and seven-day reports disclose activity and costs.

Discord is an optional notification adapter. No webhook means a null adapter and no outbound
traffic. Live execution adapters are not used by the paper service, and Phase 8 does not enable
live trading.
