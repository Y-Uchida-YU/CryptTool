import json
import logging
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session

from app.adapters.notifications.discord import DiscordWebhookNotificationAdapter
from app.domain.execution.live_models import ExecutionAuditEvent
from app.infrastructure.database.audit import SqlExecutionAuditSink
from app.infrastructure.database.models import AuditEvent, Base
from app.infrastructure.database.session import build_engine
from app.infrastructure.logging.configure import configure_logging, redact_secrets


def test_database_schema_and_audit_round_trip() -> None:
    engine = build_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    assert {"audit_events", "ohlcv"} <= set(inspect(engine).get_table_names())
    event = AuditEvent(
        occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
        event_type="risk_decision",
        entity_id="signal-1",
        payload_json=json.dumps({"allowed": False}),
        model_version="risk-1",
        config_version="config-1",
    )
    with Session(engine) as session:
        session.add(event)
        session.commit()
        stored = session.scalar(select(AuditEvent))
        assert stored is not None and stored.event_type == "risk_decision"
    engine.dispose()


def test_sql_execution_audit_sink_persists_sanitized_decisions() -> None:
    engine = build_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    sink = SqlExecutionAuditSink(engine)
    sink.record(
        ExecutionAuditEvent(
            event_id="event-1",
            timestamp=datetime(2025, 1, 1, tzinfo=UTC),
            event_type="order_rejected",
            request_id="request-1",
            allowed=False,
            reason="risk rejected",
            model_version="phase9-test",
            config_version="config-test",
            details={"symbol": "BTC"},
        )
    )
    with Session(engine) as session:
        stored = session.scalar(select(AuditEvent))
        assert stored is not None
        assert stored.event_type == "order_rejected"
        assert "risk rejected" in stored.payload_json
    engine.dispose()


def test_structured_logging_redacts_secret_fields() -> None:
    event = redact_secrets(None, "event", {"api_key": "secret", "safe": "value"})
    assert event == {"api_key": "***REDACTED***", "safe": "value"}
    configure_logging("WARNING")
    assert logging.getLogger().level == logging.WARNING


@pytest.mark.asyncio
async def test_discord_adapter_uses_https_and_posts_without_real_network() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        DiscordWebhookNotificationAdapter("http://example.test/hook")

    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["body"] = json.loads(request.content)
        return httpx.Response(204)

    adapter = DiscordWebhookNotificationAdapter(
        "https://example.test/hook", transport=httpx.MockTransport(handler)
    )
    await adapter.send("Risk halt", "data quality", "warning")
    assert captured["method"] == "POST"
    assert captured["body"] == {
        "embeds": [
            {
                "title": "Risk halt",
                "description": "data quality",
                "footer": {"text": "warning"},
            }
        ]
    }
