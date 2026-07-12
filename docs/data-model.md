# Data model

Normalized fact tables use `(exchange, symbol, market_type, timestamp[, timeframe|trade_id|sequence])` natural keys: `markets`, `ohlcv`, `trades`, `order_book_snapshots`, `order_book_levels`, `funding_rates`, `open_interest`, `liquidations`, `tickers`, and `long_short_ratios`. Decimal price/quantity fields avoid binary storage error. Every fact carries event time, received time, source endpoint, raw-object hash and quality status.

Derived tables are `feature_sets(feature_time, available_at, feature_version, values_json, quality_score)`, `regime_results`, `model_registry(training_start/end, input_schema_hash, artifact_hash, seed)`, `checkpoints`, and immutable `audit_events`. Raw payloads live in partitioned object storage or `raw_events`; corrections create a new normalized version plus audit event and never overwrite raw input.

PostgreSQL partitions high-volume facts monthly by event time; BRIN indexes serve time scans and B-tree indexes serve natural keys. Retention is configurable by data class. Migrations are forward-only in production and checked by CI. `OHLCVRow` and `AuditEvent` are the Phase 1 executable schema nucleus; other tables are added with concrete adapters to avoid premature schemas.

Feature availability time is explicit: OHLCV features become available after bar close plus configured data delay; funding at publication time; OI at exchange timestamp plus observed latency; order-book features at sequence application time. Joins are backward as-of joins only.
## Cross-venue additions

Every normalized timed observation carries `exchange_timestamp`, `received_at`, `available_at`,
`local_monotonic_time` and an optional `clock_offset_estimate`. Migration
`0002_cross_venue_clock` adds those provenance columns to OHLCV persistence. `CanonicalAsset`,
`CanonicalInstrument` and `VenueInstrumentMapping` prevent ticker-only cross-venue joins by checking
quote, settlement, contract multiplier, inverse flag, funding interval and index composition.
