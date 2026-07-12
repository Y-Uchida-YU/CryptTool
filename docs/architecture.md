# Architecture

## Boundaries

`domain` contains immutable market observations, feature definitions and regime contracts. `adapters` translate venue/storage protocols. `services` orchestrate ingestion and pure engines. `infrastructure` owns SQL/logging. `cli` is a composition boundary. Execution ports cannot be reached from ingestion ports.

Data flow: REST backfill / WebSocket -> adapter normalization -> validation/quarantine -> idempotent persistence -> causal feature engine -> quality gate -> rules and frozen statistical model -> ensemble -> audit event. Any fatal quality/risk condition ends in `UNKNOWN` and blocks downstream strategy activation.

The Phase 9 execution boundary is one-way and fail-closed: Strategy Signal -> RiskDecision ->
LivePreflightReport -> LiveExecutionGateway -> ExecutionAdapter. Only the disabled adapter ships.
The gateway owns idempotency, allowlists, hard limits and audit events; venue signing and transport
must remain inside a future adapter. Kill-switch cancellation and reduce-only contingency actions
are separate from new-exposure authorization.

## Reliability

The ingestion contracts require REST retries to be bounded and limited to transient failures. Rate
limiting precedes calls, and checkpoints advance only after successful validation and persistence.
A future concrete WebSocket adapter must implement heartbeat, sequence tracking, reconnect with
jitter, snapshot/delta reconciliation and REST recovery. Those venue-specific loops are not shipped
today. Natural keys prevent normalized OHLCV duplicates; production raw-payload retention remains a
deployment requirement rather than an implemented repository feature.

## Exchange comparison (design-time)

Official APIs show differing capabilities: Binance publishes a separate public market-data endpoint and spot streams; Bybit V5 publishes derivative OI, funding, mark/index, ticker and public WebSockets. These sources justify capability negotiation rather than a common lowest-denominator implementation. Terms/availability are operational checks, so neither venue is enabled by default. See the official [Binance spot API repository](https://github.com/binance/binance-spot-api-docs), [Bybit OI endpoint](https://bybit-exchange.github.io/docs/v5/market/open-interest), [Bybit instrument metadata](https://bybit-exchange.github.io/docs/v5/market/instrument), and [Bybit public ticker](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker).

## Technology decisions

Pydantic Settings provides fail-fast configuration; pandas/numpy implement inspectable vector features; scikit-learn supplies a deterministic GMM baseline; SQLAlchemy/Alembic isolate persistence; httpx/websockets/tenacity cover transport; structlog produces auditable JSON. A custom later event engine is favored over opaque vector backtesting because timing and fills must be testable.

## Regime definitions

Trend and range derive from volatility-normalized slope/breakout evidence. Volatility uses lagged rolling distributions. Funding and OI extremes use lagged z-scores/percentiles. Squeezes require signed abnormal return plus liquidation evidence; flash crash additionally requires OI contraction and spread stress. Spot/perp leadership requires synchronized venue-quality data. RISK_OFF requires cross-asset breadth/correlation and is deferred until cross-sectional features exist. Labels are non-exclusive. UNKNOWN is mandatory under insufficient quality/evidence.

Statistical candidates are evaluated by rolling likelihood, dwell time, transition rate, seed/window stability, interpretability and OOS utility. GMM is implemented first because its probabilities and deterministic fitting are inspectable; HMM, change-point and Markov-switching models remain challengers. Cluster IDs are not semantic regimes until mapped using training-only summaries.
