import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from app.config.settings import Settings


def test_safe_defaults() -> None:
    settings = Settings(_env_file=None)
    assert settings.paper_trading and settings.dry_run and not settings.live_trading
    assert settings.risk.leverage == 1


def test_live_requires_all_interlocks() -> None:
    with pytest.raises(ValidationError, match="live mode requires"):
        Settings(_env_file=None, live_trading=True)


def test_yaml_is_a_low_priority_runtime_source(tmp_path, monkeypatch) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(
        "symbols: [ADA]\nlive:\n  allowed_symbols: [ADA]\npaper:\n  initial_cash: 321\n",
        encoding="utf-8",
    )

    class FileSettings(Settings):
        model_config = SettingsConfigDict(
            **{
                **Settings.model_config,
                "yaml_file": config,
                "env_file": None,
            }
        )

    loaded = FileSettings(_env_file=None)
    assert loaded.symbols == ("ADA",)
    assert loaded.paper.initial_cash == 321
    monkeypatch.setenv("APP_SYMBOLS", '["BTC"]')
    monkeypatch.setenv("APP_LIVE__ALLOWED_SYMBOLS", '["BTC"]')
    overridden = FileSettings(_env_file=None)
    assert overridden.symbols == ("BTC",)
