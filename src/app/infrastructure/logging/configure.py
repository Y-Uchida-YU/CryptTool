import logging
import re
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any

import structlog

SECRET_PATTERN = re.compile(r"(?i)(api[_-]?key|secret|token|password)")


def redact_secrets(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> Mapping[str, Any]:
    for key in list(event_dict):
        if SECRET_PATTERN.search(key):
            event_dict[key] = "***REDACTED***"
    return event_dict


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s")
    structlog.configure(
        processors=[
            redact_secrets,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
