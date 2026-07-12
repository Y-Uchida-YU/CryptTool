# Phase 9 live execution interface and safety runbook

## Status

Phase 9 supplies contracts and safety controls only. `DisabledExecutionAdapter` is the only shipped
execution adapter. It raises on every order, cancel, position and account method, and its health
check is always false. Therefore this repository cannot send a real order.

Adding a venue adapter later is a separate reviewed change. Data access never implies execution
permission, and an adapter must be independently disabled when jurisdiction, terms or operational
conditions change.

## Startup preflight

Every check must pass simultaneously:

1. production environment;
2. both live flags explicitly enabled;
3. both paper flags explicitly disabled;
4. dry-run explicitly disabled;
5. exact configuration confirmation;
6. non-persisted runtime confirmation;
7. matching, execution-enabled concrete adapter;
8. adapter health check;
9. API credentials supplied through the environment;
10. withdrawal permission explicitly confirmed disabled;
11. live data-quality threshold met;
12. market-data WebSocket connected;
13. clock skew within limit;
14. kill switch clear;
15. paper validation accepted;
16. untouched out-of-sample validation accepted.

`app live-preflight` uses the shipped disabled adapter context and therefore returns exit code 2.
The runtime confirmation is never stored in `.env` or configuration files.

## Per-order controls

The gateway requires a prior approved preflight report, an allowed exchange and symbol, an
unexpired request, a passing RiskDecision, notional below the configured maximum, open-order
capacity, rate-limit capacity and a healthy adapter. Each order has an idempotency key; replaying it
returns the original receipt without calling the adapter again. Adapter receipt/request mismatches
fail closed.

Every preview, rejection, adapter error, acknowledgement, replay, cancellation, close and kill
switch decision creates a sanitized, versioned audit event. `SqlExecutionAuditSink` persists those
events transactionally in `audit_events` without API credentials.

## Kill switch and contingency path

Activating the kill switch immediately blocks new exposure and attempts `cancel_all_orders` when a
preflight-approved concrete adapter exists. Reduce-only orders, explicit cancellation and position
close remain available so that a halt does not trap existing risk. Any cancel/close failure is
surfaced as requiring manual intervention; it is not reported as successful.

Incident response order:

1. activate kill switch and record a human reason;
2. verify cancel-all acknowledgements against the venue UI;
3. reconcile open orders and positions through an independent read path;
4. close or reduce only when current market data and venue status are trustworthy;
5. rotate credentials if compromise is suspected;
6. preserve audit/database logs and configuration/model hashes;
7. require a new preflight and explicit operator confirmation before any restart.

## Conditions before implementing a concrete adapter

- Current legal/terms eligibility confirmed for the operator.
- API key has trading/read permissions only and no withdrawal permission.
- Sandbox/testnet contract tests cover signing, timestamp drift, retries and idempotency.
- Paper and OOS acceptance evidence is durable, reviewed and not derived from fixture data.
- Position/open-order reconciliation and independent emergency procedures are demonstrated.
- The adapter change receives separate review; enabling it is not bundled with strategy tuning.
