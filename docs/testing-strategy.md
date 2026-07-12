# Testing strategy

Unit tests use hand-computable fixtures for normalization, timezone handling, duplicate/gap/order detection, rates and every initial feature. Boundary cases evaluate threshold below/equal/above, simultaneous regimes, NaN/inf, poor quality, UNKNOWN and confidence monotonicity. Causality tests mutate future rows and require all prior feature values to remain identical.

Async contract tests simulate 429/timeouts, bounded retry, WebSocket disconnect/sequence gap, checkpoint failure and idempotent persistence. Adapter fixtures are recorded and scrubbed; live APIs are not used in CI. PostgreSQL integration tests validate constraints, transaction rollback and migrations.

Hypothesis invariants for later execution include fill <= order quantity, fee >=0, conservation of cash/position, close-to-zero position, no future event access and event-id idempotency. Regression fixtures pin known feature/regime outputs by version.

Coverage targets are overall 85%, risk/execution 95%, regime 90%. Branch coverage and mutation-resistant assertions matter more than line count. CI executes ruff check/format, strict mypy, pytest/coverage, migration check, Bandit and Gitleaks on Python 3.12.
