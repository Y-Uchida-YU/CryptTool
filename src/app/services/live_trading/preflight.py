from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config.settings import Settings

RUNTIME_CONFIRMATION = "ENABLE_LIVE_EXECUTION_NOW"


class LivePreflightContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    operator_confirmation: str = ""
    adapter_name: str = "disabled"
    adapter_is_concrete: bool = False
    adapter_healthy: bool = False
    data_quality_score: float = Field(0, ge=0, le=1)
    websocket_connected: bool = False
    clock_skew_seconds: float = Field(999, ge=0)
    kill_switch_active: bool = True
    paper_validation_passed: bool = False
    out_of_sample_validation_passed: bool = False

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("preflight timestamp must be timezone-aware")
        return value.astimezone(UTC)


class PreflightCheck(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    passed: bool
    reason: str


class LivePreflightReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    approved: bool
    checks: tuple[PreflightCheck, ...]
    warning: str


def evaluate_live_preflight(
    settings: Settings, context: LivePreflightContext
) -> LivePreflightReport:
    configured_execution = tuple(
        exchange.name for exchange in settings.exchanges if exchange.execution_enabled
    )
    confirmation = (
        settings.live_confirmation.get_secret_value() if settings.live_confirmation else ""
    )
    checks = (
        PreflightCheck(
            name="production_environment",
            passed=settings.environment == "production",
            reason="environment must be production",
        ),
        PreflightCheck(
            name="live_enabled",
            passed=settings.live_trading and settings.live.enabled,
            reason="both live flags must be explicitly enabled",
        ),
        PreflightCheck(
            name="paper_disabled",
            passed=not settings.paper_trading and not settings.paper.enabled,
            reason="paper mode must be explicitly disabled",
        ),
        PreflightCheck(
            name="dry_run_disabled",
            passed=not settings.dry_run,
            reason="dry-run must be explicitly disabled",
        ),
        PreflightCheck(
            name="configuration_confirmation",
            passed=confirmation == "I_ACCEPT_LIVE_TRADING_RISK",
            reason="exact configuration confirmation is required",
        ),
        PreflightCheck(
            name="runtime_confirmation",
            passed=context.operator_confirmation == RUNTIME_CONFIRMATION,
            reason="exact runtime confirmation is required and is never stored",
        ),
        PreflightCheck(
            name="concrete_adapter",
            passed=(
                context.adapter_is_concrete
                and context.adapter_name != "disabled"
                and settings.live.adapter_name == context.adapter_name
                and context.adapter_name in configured_execution
            ),
            reason="a matching execution-enabled concrete adapter is required",
        ),
        PreflightCheck(
            name="adapter_health",
            passed=context.adapter_healthy,
            reason="execution adapter health check must pass",
        ),
        PreflightCheck(
            name="credentials_present",
            passed=settings.exchange_api_key is not None
            and settings.exchange_api_secret is not None,
            reason="credentials must come from the environment",
        ),
        PreflightCheck(
            name="withdrawal_permission_disabled",
            passed=settings.live.withdrawal_permission_confirmed_disabled,
            reason="withdrawal permission is forbidden",
        ),
        PreflightCheck(
            name="data_quality",
            passed=context.data_quality_score >= settings.live.minimum_data_quality,
            reason="live data quality must meet the stricter live threshold",
        ),
        PreflightCheck(
            name="websocket_connected",
            passed=context.websocket_connected,
            reason="market-data WebSocket must be connected",
        ),
        PreflightCheck(
            name="clock_synchronized",
            passed=context.clock_skew_seconds <= settings.live.maximum_clock_skew_seconds,
            reason="clock skew exceeds the configured maximum",
        ),
        PreflightCheck(
            name="kill_switch_clear",
            passed=not context.kill_switch_active,
            reason="kill switch must be clear at startup",
        ),
        PreflightCheck(
            name="paper_validation",
            passed=context.paper_validation_passed,
            reason="paper-trading acceptance must be recorded",
        ),
        PreflightCheck(
            name="out_of_sample_validation",
            passed=context.out_of_sample_validation_passed,
            reason="untouched OOS acceptance must be recorded",
        ),
    )
    return LivePreflightReport(
        timestamp=context.timestamp,
        approved=all(check.passed for check in checks),
        checks=checks,
        warning=(
            "LIVE EXECUTION APPROVED: real orders may be sent"
            if all(check.passed for check in checks)
            else "LIVE EXECUTION REFUSED: one or more safety checks failed"
        ),
    )
