# Backtesting methodology

The later engine is an event queue ordered by `(available_at, priority, sequence)`. A closed market event updates state; feature calculation consumes only data available at that instant; a signal is emitted after calculation delay; risk accepts/rejects; order submission occurs after network delay; fills consume later quote/trade/book events. Bar-only fallback fills no earlier than next bar open with adverse spread/slippage and probabilistic/volume-capped fill. Close-at-signal execution is forbidden.

Costs comprise maker/taker fee, funding at actual settlement while position is held, half/full spread by order type, depth-walk slippage, nonlinear impact, latency drift, partial/unfilled opportunity cost, tick/lot rounding and minimum notional. Parameters are venue- and time-dependent and stress-tested at fee 1.5x and slippage 2x. Liquidation uses mark price, maintenance margin, fees and funding; 1x remains default.

Chronology uses train/validation/test, purged and embargoed selection, anchored and rolling walk-forward, then untouched OOS. Reports disclose all search trials and evaluate symbol, venue, timeframe and regime strata. Monte Carlo resamples trades in blocks and perturbs fills/costs; bootstrap confidence intervals accompany metrics. Parameter surfaces must show broad stability rather than isolated maxima.

Metrics include return/CAGR, volatility, Sharpe/Sortino/Calmar, drawdown, PF, win/payoff/expectancy, trade distribution/holding/turnover, costs, exposure, streaks, VaR/CVaR, ulcer/recovery, ruin probability and executable trade count at 100/300/1,000 USDT. Overfitting diagnostics include Deflated Sharpe, PBO/CSCV, White Reality Check or stationary-bootstrap equivalent, feature and regime stability.

Acceptance requires positive OOS, positive majority of walk-forward windows, resilience to cost stress and leave-one-period/asset-out analysis, acceptable drawdown/ruin, stable neighborhoods and executable minimum orders. Failure is reported as no demonstrated edge, not tuned away.
