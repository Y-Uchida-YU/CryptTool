import pytest
from pydantic import ValidationError

from app.config.settings import Settings


def test_safe_defaults() -> None:
    settings = Settings(_env_file=None)
    assert settings.paper_trading and settings.dry_run and not settings.live_trading
    assert settings.risk.leverage == 1


def test_live_requires_all_interlocks() -> None:
    with pytest.raises(ValidationError, match="live mode requires"):
        Settings(_env_file=None, live_trading=True)
