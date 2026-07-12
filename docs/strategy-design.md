# Phase 5 strategy design

Strategies emit desired exposure and evidence, never orders. A strategy is active only when the
current primary/secondary regime is explicitly allowed, confidence and data quality pass their
gates, and no disqualifying regime is present. Risk management can always reduce or reject the
requested exposure.

- Trend following requires TREND_UP or TREND_DOWN plus aligned slope, breakout and optional
  volume/OI confirmation. Position size is volatility-adjusted.
- Mean reversion requires RANGE and rejects strong trend, flash crash, liquidation cascade and
  low-quality liquidity. Entry combines return z-score and band/VWAP displacement.
- Flash-crash reversal requires the rule engine's multi-stage FLASH_CRASH classification, then a
  recovery phase: spread normalization, book/liquidity recovery and selling deceleration/CVD
  improvement. Detection alone is not an entry.
- Funding extreme waits for price/OI/basis/CVD evidence that crowding is unwinding. Extreme
  funding alone is insufficient.
- Relative strength uses volatility-adjusted return, beta/correlation, drawdown, liquidity,
  funding cost and pair-spread evidence. Cointegration is a prerequisite for mean-reverting pair
  exposure, not inferred from a single price ratio.

Every signal records strategy version, observation time, regime, confidence, desired side,
strength, invalidation reason and input evidence. UNKNOWN or insufficient confidence produces a
flat/disabled decision.
