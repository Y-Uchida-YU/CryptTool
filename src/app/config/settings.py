from datetime import UTC, datetime
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from app.domain.venues.models import VenueEligibilityStatus


class RiskSettings(BaseModel):
    leverage: float = Field(1.0, gt=0, le=1.0)
    max_risk_per_trade: float = Field(0.0025, gt=0, le=0.01)
    max_daily_loss: float = Field(0.01, gt=0, le=0.05)
    max_weekly_loss: float = Field(0.03, gt=0, le=0.10)
    max_drawdown: float = Field(0.08, gt=0, le=0.20)
    max_positions: int = Field(3, ge=1, le=20)
    max_consecutive_losses: int = Field(5, ge=1, le=20)
    max_gross_exposure: float = Field(1.0, gt=0, le=1.0)
    max_symbol_exposure: float = Field(0.35, gt=0, le=1.0)
    max_exchange_exposure: float = Field(0.50, gt=0, le=1.0)
    maximum_spread_bps: float = Field(50.0, gt=0, le=500)
    stale_data_seconds: int = Field(30, ge=1, le=3600)
    cooldown_seconds: int = Field(900, ge=0, le=86400)


class RegimeSettings(BaseModel):
    minimum_quality: float = Field(0.80, ge=0, le=1)
    minimum_confidence: float = Field(0.60, ge=0.5, le=1)
    z_extreme: float = Field(2.326, ge=1.5, le=4)
    z_severe: float = Field(3.0, ge=2, le=6)
    trend_slope_z: float = Field(1.0, ge=0.25, le=4)
    low_vol_z: float = Field(-1.0, ge=-4, le=-0.1)


class ExchangeSettings(BaseModel):
    name: str
    data_enabled: bool = True
    execution_enabled: bool = False
    rest_url: str | None = None
    ws_url: str | None = None


class VenueSettings(BaseModel):
    data_enabled: bool = False
    execution_enabled: bool = False
    eligibility_status: VenueEligibilityStatus = VenueEligibilityStatus.DATA_ONLY
    jurisdiction: str = "JP"
    terms_checked_at: datetime = Field(default_factory=lambda: datetime(1970, 1, 1, tzinfo=UTC))
    terms_version: str | None = None
    operator_account_verified: bool = False
    api_market_data_available: bool = False
    api_execution_available: bool = False
    deposits_available: bool = False
    withdrawals_available: bool = False
    execution_smoke_test_passed: bool = False
    requires_location_evasion: bool = False
    reason: str = "not verified"

    @field_validator("terms_checked_at")
    @classmethod
    def terms_checked_at_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("terms_checked_at must be timezone-aware")
        return value.astimezone(UTC)


class VenueRiskSettings(BaseModel):
    maximum_equity_fraction: float = Field(gt=0, le=1)


class BacktestSettings(BaseModel):
    initial_cash: float = Field(1000.0, gt=0)
    maker_fee_rate: float = Field(0.0002, ge=0, le=0.01)
    taker_fee_rate: float = Field(0.0006, ge=0, le=0.01)
    slippage_bps: float = Field(2.0, ge=0, le=100)
    market_impact_bps: float = Field(1.0, ge=0, le=100)
    signal_delay_ms: int = Field(50, ge=0, le=60000)
    submission_delay_ms: int = Field(200, ge=0, le=60000)
    maximum_participation: float = Field(0.10, gt=0, le=1)
    minimum_notional: float = Field(5.0, ge=0)


class ValidationSettings(BaseModel):
    training_observations: int = Field(1000, ge=100)
    validation_observations: int = Field(250, ge=30)
    test_observations: int = Field(250, ge=30)
    purge_observations: int = Field(5, ge=0)
    embargo_observations: int = Field(5, ge=0)
    monte_carlo_trials: int = Field(1000, ge=100, le=100000)
    random_seed: int = 17


class PaperTradingSettings(BaseModel):
    enabled: bool = True
    initial_cash: float = Field(1000.0, gt=0)
    fee_rate: float = Field(0.0006, ge=0, le=0.01)
    slippage_bps: float = Field(2.0, ge=0, le=100)
    maximum_participation: float = Field(0.10, gt=0, le=1)
    discord_webhook_url: SecretStr | None = None


class LiveTradingSettings(BaseModel):
    enabled: bool = False
    adapter_name: str = "disabled"
    allowed_symbols: tuple[str, ...] = ("BTC", "ETH", "SOL")
    maximum_order_notional: float = Field(100.0, gt=0, le=10000)
    maximum_open_orders: int = Field(3, ge=1, le=20)
    maximum_orders_per_minute: int = Field(5, ge=1, le=60)
    maximum_clock_skew_seconds: float = Field(2.0, ge=0, le=30)
    preflight_ttl_seconds: int = Field(30, ge=1, le=300)
    minimum_data_quality: float = Field(0.90, ge=0.8, le=1)
    withdrawal_permission_confirmed_disabled: bool = True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="APP_",
        env_nested_delimiter="__",
        env_file=".env",
        yaml_file="configs/default.yaml",
        yaml_file_encoding="utf-8",
        extra="ignore",
    )

    environment: Literal["development", "test", "production"] = "development"
    database_url: str = "sqlite+pysqlite:///cryptbot.db"
    symbols: tuple[str, ...] = ("BTC", "ETH", "SOL")
    timeframes: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h", "1d")
    paper_trading: bool = True
    live_trading: bool = False
    dry_run: bool = True
    live_confirmation: SecretStr | None = None
    exchange_api_key: SecretStr | None = None
    exchange_api_secret: SecretStr | None = None
    risk: RiskSettings = Field(default_factory=RiskSettings)  # type: ignore[arg-type]
    regime: RegimeSettings = Field(default_factory=RegimeSettings)  # type: ignore[arg-type]
    backtest: BacktestSettings = Field(default_factory=BacktestSettings)  # type: ignore[arg-type]
    validation: ValidationSettings = Field(default_factory=ValidationSettings)  # type: ignore[arg-type]
    paper: PaperTradingSettings = Field(default_factory=PaperTradingSettings)  # type: ignore[arg-type]
    live: LiveTradingSettings = Field(default_factory=LiveTradingSettings)  # type: ignore[arg-type]
    exchanges: tuple[ExchangeSettings, ...] = (
        ExchangeSettings(name="bybit_public", data_enabled=False),
    )
    venues: dict[str, VenueSettings] = Field(default_factory=dict)
    venue_risk: dict[str, VenueRiskSettings] = Field(default_factory=dict)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Explicit constructor values and environment secrets always override checked-in YAML.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )

    @model_validator(mode="after")
    def guard_live_mode(self) -> "Settings":
        confirmation = self.live_confirmation.get_secret_value() if self.live_confirmation else ""
        if self.live_trading:
            if self.paper_trading or self.dry_run or confirmation != "I_ACCEPT_LIVE_TRADING_RISK":
                raise ValueError(
                    "live mode requires paper=false, dry_run=false, and exact confirmation"
                )
            enabled_execution = [
                exchange for exchange in self.exchanges if exchange.execution_enabled
            ]
            if not enabled_execution:
                raise ValueError("live mode requires an explicitly execution-enabled exchange")
            if self.environment != "production":
                raise ValueError("live mode requires environment=production")
            if self.live.adapter_name == "disabled":
                raise ValueError("live mode requires a concrete execution adapter")
            if sum(exchange.name == self.live.adapter_name for exchange in enabled_execution) != 1:
                raise ValueError("live adapter must match exactly one execution-enabled exchange")
            if self.exchange_api_key is None or self.exchange_api_secret is None:
                raise ValueError("live mode requires API credentials from the environment")
            if not self.live.withdrawal_permission_confirmed_disabled:
                raise ValueError("live mode forbids API keys with withdrawal permission")
        if not self.symbols or any(not symbol.isalnum() for symbol in self.symbols):
            raise ValueError("symbols must be non-empty alphanumeric identifiers")
        if self.paper.enabled != self.paper_trading:
            raise ValueError("paper.enabled and paper_trading must agree")
        if self.live.enabled != self.live_trading:
            raise ValueError("live.enabled and live_trading must agree")
        if not self.live.allowed_symbols or any(
            symbol not in self.symbols for symbol in self.live.allowed_symbols
        ):
            raise ValueError("live allowed symbols must be a non-empty subset of symbols")
        forbidden_execution = {"bybit", "binance_global"}
        if any(
            name in forbidden_execution and venue.execution_enabled
            for name, venue in self.venues.items()
        ):
            raise ValueError("Bybit and Binance Global execution is forbidden for JP operators")
        if any(
            venue.requires_location_evasion and (venue.data_enabled or venue.execution_enabled)
            for venue in self.venues.values()
        ):
            raise ValueError("venues requiring location evasion must remain disabled")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
