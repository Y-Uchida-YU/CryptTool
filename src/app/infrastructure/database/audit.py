import json

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.domain.execution.live_models import ExecutionAuditEvent
from app.infrastructure.database.models import AuditEvent


class SqlExecutionAuditSink:
    """Durable append-only sink for sanitized Phase 9 decision events."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def record(self, event: ExecutionAuditEvent) -> None:
        payload = event.model_dump(mode="json")
        with Session(self.engine) as session:
            session.add(
                AuditEvent(
                    occurred_at=event.timestamp,
                    event_type=event.event_type,
                    entity_id=event.request_id,
                    payload_json=json.dumps(payload, sort_keys=True),
                    model_version=event.model_version,
                    config_version=event.config_version,
                )
            )
            session.commit()
