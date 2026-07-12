# Risk management and threat analysis

Risk precedence is data safety -> operational safety -> portfolio risk -> signal. Default action is no new position.

Hard gates: invalid/stale data, unresolved sequence gap, disconnected stream, REST/WS mismatch, abnormal cross-venue price, abnormal spread, UNKNOWN regime, insufficient confidence, daily/weekly loss, 8% drawdown, leverage >1, exposure, position count, losing streak or kill switch. Existing positions follow a separate reduce-only contingency path; data failure must not blindly market-close into dislocation.

Sizing candidates are fixed fractional, volatility target, ATR maximum-loss, and risk parity. The final requested size is the minimum allowed by per-trade risk, asset/venue/gross exposure, available cash and market minimums. If rounding to lot size violates risk or minimum notional, reject. Full Kelly is prohibited.

Threats include leaked credentials, withdrawal permission, replay/duplicate messages, clock drift, stale DNS/API endpoints, manipulated prints, book desynchronization, database partial writes, model artifact replacement, configuration drift and operator error. Controls include environment secrets, redaction, least privilege, hashes/versioning, idempotency, NTP monitoring, transactional checkpoints, allowlisted endpoints and two-factor live enablement. Phase 0–4 contains no execution implementation.

Daily and seven-day losses use realized plus conservative marked unrealized PnL. Drawdown uses high-water equity. Circuit breakers require manual acknowledgment or configured cooldown plus healthy-data observation window; they do not auto-reset solely with elapsed time.
