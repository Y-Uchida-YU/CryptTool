from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import signal
import socket
import subprocess  # nosec B404
import tempfile
import time
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer
import yaml  # type: ignore[import-untyped]
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from app.adapters.exchanges.dex import (
    DydxMarketDataAdapter,
    LighterMarketDataAdapter,
    ParadexMarketDataAdapter,
)
from app.adapters.exchanges.domestic import (
    BitbankMarketDataAdapter,
    BitflyerMarketDataAdapter,
    GmoCoinMarketDataAdapter,
)
from app.adapters.exchanges.public import (
    AsterMarketDataAdapter,
    BitgetMarketDataAdapter,
    HyperliquidMarketDataAdapter,
    MexcMarketDataAdapter,
    PublicRestAdapter,
)
from app.config.settings import Settings
from app.domain.execution.models import InstrumentRules, MarketSnapshot, OrderType, TimeInForce
from app.domain.features.engine import FeatureEngine
from app.domain.market_data.models import OHLCV, Side
from app.domain.venues.trusted_capabilities import TrustedCapabilityRegistry
from app.infrastructure.database.session import build_engine
from app.services.backtest.engine import BacktestEngine
from app.services.backtest.events import FundingEvent, MarketEvent, SignalEvent
from app.services.capability_audit import CapabilityContractAuditor
from app.services.ingestion.quality import validate_ohlcv
from app.services.live_trading.preflight import LivePreflightContext, evaluate_live_preflight
from app.services.paper_trading.broker import PaperBroker
from app.services.paper_trading.models import PaperOrderRequest, PaperQuote
from app.services.regime_engine.ensemble import EnsembleRegimeEngine
from app.services.regime_engine.rules import DeterministicRuleEngine
from app.services.reporting.report import AcceptanceAssessment, generate_report
from app.services.research.accelerated_validation import (
    AcceleratedValidationArtifactWriter,
    FaultKind,
    FaultSchedule,
    HistoricalMarketEventReplay,
    HistoricalPublicDatasetLoader,
    run_start_stop_resource_test,
)
from app.services.research.collector_runs import (
    CollectorLeaseConflict,
    CollectorRunRecord,
    CollectorRunStatus,
    SQLCollectorLeaseRepository,
    collector_group_key,
)
from app.services.research.data_operations import (
    DataSnapshotService,
    PublicAdapterCollectorSource,
    ResearchMarketDataCollector,
    SnapshotEligibilityPolicy,
    TrustedResearchCapabilityGate,
    write_collector_health_report,
    write_snapshot_manifest,
)
from app.services.research.models import ResearchRunResult, canonical_sha256
from app.services.research.pipeline import ResearchPipeline
from app.services.research.report import ResearchArtifactWriter
from app.services.research.repository import (
    InMemoryResearchRepository,
    NamespacedResearchRepository,
    PostgreSQLResearchRepository,
)
from app.services.validation.resampling import monte_carlo_paths
from app.services.validation.splits import anchored_walk_forward, rolling_walk_forward
from app.services.venue_eligibility import eligibility_from_settings

app = typer.Typer(help="CryptBot research and market-regime CLI", no_args_is_help=True)


def _timestamp(value: Any) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        raise typer.BadParameter("timestamps must include a UTC offset")
    return parsed.to_pydatetime().astimezone(UTC)


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, default=_json_default, indent=2, sort_keys=True), encoding="utf-8"
    )
    temporary.replace(path)


def _settings_from_yaml(path: Path) -> Settings:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise typer.BadParameter("settings config must be a YAML mapping")
    return Settings(**payload)


@app.command("validate-config")
def validate_config() -> None:
    """Validate safety and runtime configuration."""
    settings = Settings()
    typer.echo(
        f"configuration valid: environment={settings.environment}, "
        f"paper={settings.paper_trading}, live={settings.live_trading}"
    )


@app.command("db-migrate")
def db_migrate() -> None:
    """Upgrade the configured database through Alembic."""
    command.upgrade(Config("alembic.ini"), "head")
    typer.echo("database schema is current")


@app.command("backfill")
def backfill() -> None:
    """Run configured REST backfill (requires a concrete enabled exchange adapter)."""
    settings = Settings()
    enabled = [exchange.name for exchange in settings.exchanges if exchange.data_enabled]
    if not enabled:
        typer.echo("backfill refused: no market-data adapter is enabled", err=True)
        raise typer.Exit(2)
    typer.echo(f"enabled adapters require deployment composition: {','.join(enabled)}")


@app.command("validate-data")
def validate_data(
    input_path: Annotated[Path, typer.Argument(exists=True, readable=True)],
) -> None:
    """Validate a normalized OHLCV CSV and print its quality result."""
    frame = pd.read_csv(input_path)
    required = {
        "exchange",
        "symbol",
        "timeframe",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise typer.BadParameter(f"missing columns: {','.join(missing)}")
    observations: list[OHLCV] = []
    invalid: list[dict[str, object]] = []
    for index, row in frame.iterrows():
        try:
            observations.append(
                OHLCV(
                    exchange=str(row["exchange"]),
                    symbol=str(row["symbol"]),
                    timeframe=str(row["timeframe"]),
                    timestamp=_timestamp(row["timestamp"]),
                    open=Decimal(str(row["open"])),
                    high=Decimal(str(row["high"])),
                    low=Decimal(str(row["low"])),
                    close=Decimal(str(row["close"])),
                    volume=Decimal(str(row["volume"])),
                )
            )
        except ValueError as exc:
            invalid.append({"row": str(index), "reason": str(exc)})
    result = validate_ohlcv(observations)
    payload = {
        "accepted": len(result.accepted),
        "rejected": len(result.rejected) + len(invalid),
        "quality_score": result.quality_score if not invalid else 0.0,
        "issues": [issue.model_dump(mode="json") for issue in result.issues],
        "invalid_rows": invalid,
    }
    typer.echo(json.dumps(payload, indent=2, default=_json_default))
    if invalid or result.quality_score == 0:
        raise typer.Exit(1)


@app.command("build-features")
def build_features(
    input_path: Annotated[Path, typer.Argument(exists=True, readable=True)],
    output_path: Annotated[Path, typer.Option()] = Path("reports/features.csv"),
    window: Annotated[int, typer.Option(min=2)] = 30,
) -> None:
    """Build causal features from a chronologically indexed market CSV."""
    frame = pd.read_csv(input_path)
    if "timestamp" in frame:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.set_index("timestamp")
    engine = FeatureEngine(window=window)
    output = engine.build(frame)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path)
    engine.quality_report(output).to_csv(output_path.with_suffix(".quality.csv"))
    engine.availability(frame).to_csv(output_path.with_suffix(".availability.csv"))
    typer.echo(f"features written: {output_path} ({len(output)} observations)")


@app.command("detect-regimes")
def detect_regimes(
    input_path: Annotated[Path, typer.Argument(exists=True, readable=True)],
    output_path: Annotated[Path, typer.Option()] = Path("reports/regime.json"),
    quality_score: Annotated[float, typer.Option(min=0.0, max=1.0)] = 1.0,
) -> None:
    """Detect the latest explainable regime from a feature CSV."""
    frame = pd.read_csv(input_path)
    if frame.empty:
        raise typer.BadParameter("feature CSV is empty")
    timestamp = _timestamp(frame.iloc[-1].get("timestamp", datetime.now(UTC).isoformat()))
    features: dict[str, float | None] = {}
    for key, value in frame.iloc[-1].items():
        if key == "timestamp":
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        features[str(key)] = numeric if np.isfinite(numeric) else None
    settings = Settings()
    detector = EnsembleRegimeEngine(DeterministicRuleEngine(settings.regime))
    result = detector.detect(features, timestamp, quality_score)
    _write_json(output_path, result.model_dump(mode="python"))
    typer.echo(f"{result.primary_regime.value} confidence={result.confidence:.3f}")


def _load_backtest(path: Path) -> tuple[BacktestEngine, list[Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rules: dict[tuple[str, str], InstrumentRules] = {}
    for item in payload["rules"]:
        rules[(item["exchange"], item["symbol"])] = InstrumentRules(
            tick_size=Decimal(str(item["tick_size"])),
            lot_size=Decimal(str(item["lot_size"])),
            minimum_notional=Decimal(str(item["minimum_notional"])),
            maker_fee_rate=Decimal(str(item.get("maker_fee_rate", "0.0002"))),
            taker_fee_rate=Decimal(str(item.get("taker_fee_rate", "0.0006"))),
            maintenance_margin_rate=Decimal(str(item.get("maintenance_margin_rate", "0.005"))),
        )
    engine = BacktestEngine(Decimal(str(payload["initial_cash"])), rules)
    events: list[Any] = []
    for item in payload["events"]:
        timestamp = _timestamp(item["timestamp"])
        kind = item["type"]
        if kind == "market":
            events.append(
                MarketEvent(
                    MarketSnapshot(
                        exchange=item["exchange"],
                        symbol=item["symbol"],
                        timestamp=timestamp,
                        bid=Decimal(str(item["bid"])),
                        ask=Decimal(str(item["ask"])),
                        bid_quantity=Decimal(str(item["bid_quantity"])),
                        ask_quantity=Decimal(str(item["ask_quantity"])),
                        last_price=Decimal(str(item["last_price"]))
                        if "last_price" in item
                        else None,
                        trade_quantity=Decimal(str(item["trade_quantity"]))
                        if "trade_quantity" in item
                        else None,
                        mark_price=Decimal(str(item["mark_price"]))
                        if "mark_price" in item
                        else None,
                    )
                )
            )
        elif kind == "signal":
            events.append(
                SignalEvent(
                    timestamp=timestamp,
                    signal_id=item["signal_id"],
                    exchange=item["exchange"],
                    symbol=item["symbol"],
                    side=Side(item["side"]),
                    quantity=Decimal(str(item["quantity"])),
                    order_type=OrderType(item.get("order_type", "market")),
                    time_in_force=TimeInForce(item.get("time_in_force", "gtc")),
                    calculation_delay=timedelta(
                        milliseconds=int(item.get("calculation_delay_ms", 0))
                    ),
                    submission_delay=timedelta(
                        milliseconds=int(item.get("submission_delay_ms", 0))
                    ),
                    reduce_only=bool(item.get("reduce_only", False)),
                    stop_loss=Decimal(str(item["stop_loss"])) if "stop_loss" in item else None,
                    take_profit=Decimal(str(item["take_profit"]))
                    if "take_profit" in item
                    else None,
                )
            )
        elif kind == "funding":
            events.append(
                FundingEvent(
                    timestamp=timestamp,
                    exchange=item["exchange"],
                    symbol=item["symbol"],
                    rate=Decimal(str(item["rate"])),
                    mark_price=Decimal(str(item["mark_price"])),
                )
            )
        else:
            raise typer.BadParameter(f"unsupported backtest event type: {kind}")
    return engine, events


@app.command("run-backtest")
def run_backtest(
    input_path: Annotated[Path, typer.Argument(exists=True, readable=True)],
    output_path: Annotated[Path, typer.Option()] = Path("reports/backtest-result.json"),
) -> None:
    """Run an event-driven backtest from an explicit JSON event ledger."""
    engine, events = _load_backtest(input_path)
    engine.add_events(events)
    result = engine.run()
    payload = {
        "processed_events": result.processed_events,
        "final_cash": result.final_cash,
        "final_equity": result.final_equity,
        "orders": [asdict(order) for order in result.orders],
        "fills": [asdict(fill) for fill in result.fills],
        "funding": [asdict(record) for record in result.funding],
        "liquidations": [asdict(item) for item in result.liquidations],
        "rejected_signals": [asdict(item) for item in result.rejected_signals],
        "snapshots": [asdict(item) for item in result.snapshots],
    }
    _write_json(output_path, payload)
    typer.echo(
        f"backtest complete: fills={len(result.fills)}, final_equity={result.final_equity}; "
        "mechanical validation only"
    )


@app.command("generate-walk-forward-windows")
def generate_walk_forward_windows(
    observations: Annotated[int, typer.Option(min=1)],
    train: Annotated[int, typer.Option(min=1)],
    validation: Annotated[int, typer.Option(min=0)],
    test: Annotated[int, typer.Option(min=1)],
    purge: Annotated[int, typer.Option(min=0)] = 0,
    embargo: Annotated[int, typer.Option(min=0)] = 0,
    output_path: Annotated[Path, typer.Option()] = Path("reports/walk-forward-windows.json"),
) -> None:
    """Generate both rolling and anchored walk-forward windows."""
    kwargs = {
        "n_samples": observations,
        "train_size": train,
        "validation_size": validation,
        "out_of_sample_size": test,
        "purge_size": purge,
        "embargo_size": embargo,
    }
    rolling = rolling_walk_forward(**kwargs)
    anchored = anchored_walk_forward(**kwargs)

    def summarize(window: Any) -> dict[str, object]:
        return {
            "number": window.number,
            "train": [int(window.train[0]), int(window.train[-1])],
            "validation": (
                [int(window.validation[0]), int(window.validation[-1])]
                if len(window.validation)
                else []
            ),
            "out_of_sample": [int(window.out_of_sample[0]), int(window.out_of_sample[-1])],
            "anchored": window.anchored,
        }

    _write_json(
        output_path,
        {
            "rolling": [summarize(item) for item in rolling],
            "anchored": [summarize(item) for item in anchored],
        },
    )
    typer.echo(f"walk-forward windows: rolling={len(rolling)}, anchored={len(anchored)}")


@app.command("run-monte-carlo")
def run_monte_carlo(
    input_path: Annotated[Path, typer.Argument(exists=True, readable=True)],
    column: Annotated[str, typer.Option()] = "return",
    initial_capital: Annotated[float, typer.Option(min=0.01)] = 1000.0,
    simulations: Annotated[int, typer.Option(min=100)] = 1000,
    output_path: Annotated[Path, typer.Option()] = Path("reports/monte-carlo.json"),
) -> None:
    """Run fixed-seed block Monte Carlo on net returns from CSV."""
    frame = pd.read_csv(input_path)
    if column not in frame:
        raise typer.BadParameter(f"return column not found: {column}")
    result = monte_carlo_paths(
        np.asarray(frame[column].astype(float).to_numpy(), dtype=np.float64),
        initial_capital=initial_capital,
        n_simulations=simulations,
        seed=Settings().validation.random_seed,
    )
    _write_json(output_path, result.to_dict())
    typer.echo(
        f"Monte Carlo complete: ruin={result.ruin_probability:.3%}, "
        f"loss={result.probability_of_loss:.3%}"
    )


@app.command("generate-report")
def generate_report_command(
    equity_path: Annotated[Path, typer.Argument(exists=True, readable=True)],
    trades_path: Annotated[Path | None, typer.Option(exists=True, readable=True)] = None,
    output_directory: Annotated[Path, typer.Option()] = Path("reports/research"),
) -> None:
    """Generate Markdown, JSON and CSV research reports without filtering losses."""
    equity_frame = pd.read_csv(equity_path)
    if not {"timestamp", "equity"} <= set(equity_frame):
        raise typer.BadParameter("equity CSV requires timestamp,equity")
    index = pd.to_datetime(equity_frame["timestamp"], utc=True)
    equity = pd.Series(equity_frame["equity"].astype(float).to_numpy(), index=index)
    trades = pd.read_csv(trades_path) if trades_path else None
    assessment = AcceptanceAssessment(overall="INSUFFICIENT_EVIDENCE", checks=())
    artifacts = generate_report(
        output_directory,
        equity_curve=equity,
        trades=trades,
        assessment=assessment,
        metadata={"live_trading": False, "source": str(equity_path)},
    )
    typer.echo(f"report generated: {artifacts.markdown}; verdict={assessment.overall}")


def _run_research(config_path: Path) -> ResearchRunResult:
    settings = Settings()
    if settings.live_trading:
        raise typer.BadParameter("research pipeline requires live execution to remain disabled")
    engine = build_engine(settings.database_url)
    try:
        repository = PostgreSQLResearchRepository(engine)
        pipeline = ResearchPipeline(repository)
        return pipeline.run(ResearchPipeline.load_config(config_path))
    finally:
        engine.dispose()


@app.command("run-research-pipeline")
def run_research_pipeline(
    config: Annotated[Path, typer.Option("--config", exists=True, readable=True)],
) -> None:
    """Run the immutable raw-data-to-acceptance research pipeline."""
    result = _run_research(config)
    typer.echo(
        f"run_id={result.identity.run_id} snapshot={result.identity.data_snapshot_id} "
        f"verdict={result.acceptance_result.verdict.value} "
        f"manifest={result.artifact_manifest_path} live_execution=OFF"
    )


@app.command("run-walk-forward-backtest")
def run_walk_forward_backtest(
    config: Annotated[Path, typer.Option("--config", exists=True, readable=True)],
) -> None:
    """Execute strategy selection and untouched OOS windows from a research config."""
    result = _run_research(config)
    walk_forward = result.walk_forward_result
    typer.echo(
        f"run_id={result.identity.run_id} windows={len(walk_forward.windows)} "
        f"oos_observations={len(walk_forward.combined_oos_returns)} "
        f"content_sha256={walk_forward.content_sha256}"
    )


@app.command("generate-research-report")
def generate_research_report(
    run_id: Annotated[str, typer.Option("--run-id", min=1)],
) -> None:
    """Verify and expose the reproducible artifact set for an existing run."""
    manifest_path = Path("artifacts/research") / run_id / "manifest.json"
    if not manifest_path.is_file():
        raise typer.BadParameter(f"unknown research run: {run_id}")
    manifest = ResearchArtifactWriter.verify(manifest_path)
    files = manifest["files"]
    if not isinstance(files, dict):
        raise typer.BadParameter("invalid research manifest files")
    typer.echo(
        f"report={manifest_path} snapshot={manifest['data_snapshot_id']} "
        f"files={len(files)} hashes=verified"
    )


def _research_data_adapter(name: str) -> PublicRestAdapter:
    adapters: dict[str, type[PublicRestAdapter]] = {
        "hyperliquid": HyperliquidMarketDataAdapter,
        "bitget": BitgetMarketDataAdapter,
        "aster": AsterMarketDataAdapter,
        "mexc": MexcMarketDataAdapter,
        "dydx": DydxMarketDataAdapter,
        "paradex": ParadexMarketDataAdapter,
        "lighter": LighterMarketDataAdapter,
    }
    try:
        return adapters[name]()
    except KeyError as exc:
        raise typer.BadParameter(f"unsupported R2 research venue: {name}") from exc


async def _run_accelerated_validation(
    *,
    settings: Settings,
    days: int,
    commit_sha: str,
    seed: int,
    maximum_queue_depth: int,
    live_soak_artifact: Path | None,
    research_config: Path | None,
) -> Path:
    if settings.live_trading or settings.live.enabled:
        raise typer.BadParameter("accelerated validation requires live execution to remain OFF")
    _require_nonproduction_database_isolation(settings, run_mode="accelerated_validation")
    requested_venues = tuple(
        venue for venue in settings.research_collection.venues if venue in {"hyperliquid", "bitget"}
    ) or ("hyperliquid", "bitget")
    adapters = tuple(_research_data_adapter(venue) for venue in requested_venues)
    sources = tuple(PublicAdapterCollectorSource(adapter, adapter.venue) for adapter in adapters)
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    try:
        dataset = await HistoricalPublicDatasetLoader(sources).load(
            start=start,
            end=end,
            instruments=settings.research_collection.instruments,
        )
    finally:
        await asyncio.gather(*(source.close() for source in sources), return_exceptions=True)
    if len(dataset.events) <= len(tuple(FaultKind)):
        raise typer.BadParameter("historical public dataset has too few events for fault coverage")
    engine = build_engine(settings.database_url)
    try:
        repository = PostgreSQLResearchRepository(engine)
        replay = await HistoricalMarketEventReplay(repository=repository).replay(
            events=dataset.stream(),
            speed=None,
            maximum_queue_depth=maximum_queue_depth,
            fault_schedule=FaultSchedule.deterministic(event_count=len(dataset.events), seed=seed),
        )
        sample_event = dataset.events[0]

        async def lifecycle(_: int) -> None:
            memory_repository = InMemoryResearchRepository()

            async def one_event() -> Any:
                yield sample_event

            await HistoricalMarketEventReplay(
                repository=memory_repository, restart_percentages=()
            ).replay(
                events=one_event(),
                speed=None,
                maximum_queue_depth=1,
                fault_schedule=None,
            )

        pool = getattr(engine, "pool", None)
        checked_out = getattr(pool, "checkedout", None)
        resources, resource_analysis = await run_start_stop_resource_test(
            iterations=100,
            cycle=lifecycle,
            database_connections=(checked_out if callable(checked_out) else lambda: 0),
        )
    finally:
        engine.dispose()
    live_soak_status = "INSUFFICIENT_EVIDENCE"
    if live_soak_artifact is not None and live_soak_artifact.is_file():
        soak = json.loads(live_soak_artifact.read_text(encoding="utf-8"))
        configured = float(soak.get("configured_duration_hours", 0))
        live_soak_status = "PASS" if configured >= 6 and soak.get("completed_at") else "FAIL"
    research_verdict = "INSUFFICIENT_EVIDENCE"
    if research_config is not None:
        research_verdict = _run_research(research_config).acceptance_result.verdict.value
    unresolved_values = {
        detail
        for item in dataset.coverage
        if item.status != "AVAILABLE" and (detail := item.detail) is not None
    }
    if live_soak_status != "PASS":
        unresolved_values.add("6-hour live soak is incomplete")
    if research_verdict != "PASS":
        unresolved_values.add("research pipeline evidence is incomplete")
    unresolved = tuple(sorted(unresolved_values))
    run_id = f"accelerated-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{commit_sha[:8]}"
    return AcceleratedValidationArtifactWriter.write(
        root=Path("artifacts/accelerated-validation"),
        run_id=run_id,
        commit_sha=commit_sha,
        replay=replay,
        resources=resources,
        resource_leak_detected=not resource_analysis.bounded,
        resource_analysis=resource_analysis,
        live_soak_status=live_soak_status,
        research_pipeline_verdict=research_verdict,
        unresolved_items=unresolved,
        dataset_coverage=dataset.coverage,
    )


@app.command("run-accelerated-validation")
def run_accelerated_validation(
    config: Annotated[Path, typer.Option("--config", exists=True, readable=True)],
    commit_sha: Annotated[str, typer.Option("--commit-sha", min=7)],
    days: Annotated[int, typer.Option("--days", min=30, max=90)] = 30,
    seed: Annotated[int, typer.Option("--seed")] = 20260714,
    maximum_queue_depth: Annotated[int, typer.Option("--maximum-queue-depth", min=1)] = 4096,
    live_soak_artifact: Annotated[
        Path | None, typer.Option("--live-soak-artifact", exists=True, readable=True)
    ] = None,
    research_config: Annotated[
        Path | None, typer.Option("--research-config", exists=True, readable=True)
    ] = None,
) -> None:
    """Run historical replay, deterministic faults, restarts and 100 lifecycle cycles."""
    manifest = asyncio.run(
        _run_accelerated_validation(
            settings=_settings_from_yaml(config),
            days=days,
            commit_sha=commit_sha,
            seed=seed,
            maximum_queue_depth=maximum_queue_depth,
            live_soak_artifact=live_soak_artifact,
            research_config=research_config,
        )
    )
    typer.echo(f"manifest={manifest} live_execution=OFF")


async def _run_clean_historical_replay(
    *, settings: Settings, days: int, commit_sha: str, maximum_queue_depth: int
) -> Path:
    if settings.live_trading or settings.live.enabled:
        raise typer.BadParameter("clean replay requires live execution to remain OFF")
    _require_nonproduction_database_isolation(settings, run_mode="clean_replay")
    venues = tuple(
        venue for venue in settings.research_collection.venues if venue in {"hyperliquid", "bitget"}
    ) or ("hyperliquid", "bitget")
    adapters = tuple(_research_data_adapter(venue) for venue in venues)
    sources = tuple(PublicAdapterCollectorSource(adapter, adapter.venue) for adapter in adapters)
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    run_id = f"clean-replay-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{commit_sha[:8]}"
    try:
        dataset = await HistoricalPublicDatasetLoader(sources).load(
            start=start,
            end=end,
            instruments=settings.research_collection.instruments,
        )
    finally:
        await asyncio.gather(*(source.close() for source in sources), return_exceptions=True)
    if not dataset.events:
        raise typer.BadParameter("clean historical dataset contains no available events")
    engine = build_engine(settings.database_url)
    database_identity, schema_name = _database_identity(engine)
    started = time.perf_counter()
    try:
        base_repository = PostgreSQLResearchRepository(engine)
        repository = NamespacedResearchRepository(
            base_repository, checkpoint_namespace=f"clean-replay:{run_id}"
        )
        replay = await HistoricalMarketEventReplay(
            repository=repository,
            restart_percentages=(),
            snapshot_prefix="clean",
        ).replay(
            events=dataset.stream(),
            speed=None,
            maximum_queue_depth=maximum_queue_depth,
            fault_schedule=None,
        )
        verified = DataSnapshotService(base_repository).verify(replay.snapshot_manifest.snapshot_id)
    finally:
        engine.dispose()
    if replay.fault_results or replay.restart_results:
        raise RuntimeError("clean replay unexpectedly contains fault or restart injection")
    directory = Path("artifacts/clean-replay") / run_id
    directory.mkdir(parents=True, exist_ok=False)
    payload = {
        "run_id": run_id,
        "commit_sha": commit_sha,
        "database_identity": database_identity,
        "schema_name": schema_name,
        "checkpoint_namespace": f"clean-replay:{run_id}",
        "fault_injection": "disabled",
        "restart_injection": "disabled",
        "requested_start": start,
        "requested_end": end,
        "event_count": replay.input_events,
        "elapsed_seconds": time.perf_counter() - started,
        "effective_speed": replay.effective_speed,
        "event_loss": replay.event_loss,
        "unexpected_duplicates": replay.unexpected_duplicates,
        "snapshot_id": verified.snapshot_id,
        "snapshot_manifest_sha256": verified.manifest_sha256,
        "snapshot_eligibility_status": verified.eligibility_status,
        "snapshot_eligibility_reasons": verified.eligibility_reasons,
        "dataset_coverage": tuple(asdict(item) for item in dataset.coverage),
        "live_execution": "OFF",
    }
    evidence = directory / "result.json"
    evidence.write_text(
        json.dumps(payload, default=_json_default, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    write_snapshot_manifest(directory / "snapshot-manifest.json", verified)
    manifest = {
        "run_id": run_id,
        "files": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (evidence, directory / "snapshot-manifest.json")
        },
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return manifest_path


@app.command("run-clean-historical-replay")
def run_clean_historical_replay(
    config: Annotated[Path, typer.Option("--config", exists=True, readable=True)],
    commit_sha: Annotated[str, typer.Option("--commit-sha", min=7)],
    days: Annotated[int, typer.Option("--days", min=30, max=90)] = 30,
    maximum_queue_depth: Annotated[int, typer.Option("--maximum-queue-depth", min=1)] = 4096,
) -> None:
    """Build a fault-free, restart-free snapshot through the production collector path."""
    manifest = asyncio.run(
        _run_clean_historical_replay(
            settings=_settings_from_yaml(config),
            days=days,
            commit_sha=commit_sha,
            maximum_queue_depth=maximum_queue_depth,
        )
    )
    typer.echo(f"manifest={manifest} live_execution=OFF")


@app.command("collect-research-data")
def collect_research_data(
    config: Annotated[Path, typer.Option("--config", exists=True, readable=True)],
) -> None:
    """Collect public market data; unverified capabilities remain experimental."""
    settings = _settings_from_yaml(config)
    collection = settings.research_collection
    if settings.live_trading or settings.live.enabled:
        raise typer.BadParameter("research collection requires live execution to remain OFF")
    if not collection.collection_enabled:
        raise typer.BadParameter("collection_enabled=true is required")
    if not collection.venues:
        raise typer.BadParameter("at least one research collection venue is required")
    adapters = tuple(_research_data_adapter(name) for name in collection.venues)
    registry = TrustedCapabilityRegistry.from_artifacts(
        Path.cwd(), tuple(adapter.capabilities for adapter in adapters)
    )
    engine = build_engine(settings.database_url)
    started_at = datetime.now(UTC)
    run_id = f"collector-production-{started_at.strftime('%Y%m%dT%H%M%S%fZ')}"
    owner_id = f"{socket.gethostname()}:{os.getpid()}"
    process_started_at, command_sha256 = _process_identity(os.getpid())
    token_path, run_token_sha256 = _create_collector_run_token(run_id)
    database_identity, schema_name = _database_identity(engine)
    groups = _collector_groups(
        database_identity=database_identity,
        schema_name=schema_name,
        venues=collection.venues,
        instruments=collection.instruments,
        event_types=collection.event_types,
    )
    artifact_directory = Path("artifacts/collector-runs") / run_id
    lease_repository = SQLCollectorLeaseRepository(engine)
    run = CollectorRunRecord(
        run_id=run_id,
        collector_group=f"collector-set-{canonical_sha256(groups)[:24]}",
        owner_id=owner_id,
        commit_sha=_current_commit_sha(),
        config_path=str(config.resolve()),
        database_identity=database_identity,
        schema_name=schema_name,
        checkpoint_namespace="production",
        artifact_namespace=str(artifact_directory),
        venues=collection.venues,
        instruments=collection.instruments,
        event_types=collection.event_types,
        duration_seconds=None,
        pid=os.getpid(),
        process_started_at=process_started_at,
        hostname=socket.gethostname(),
        command_sha256=command_sha256,
        run_token_sha256=run_token_sha256,
        status=CollectorRunStatus.RUNNING,
        started_at=started_at,
        heartbeat_at=started_at,
    )
    acquired: list[str] = []
    supervisor_started = False
    try:
        lease_repository.save_run(run)
        for group in groups:
            lease_repository.acquire(group, run_id, owner_id, started_at + timedelta(seconds=90))
            acquired.append(group)
        collector = ResearchMarketDataCollector(
            repository=PostgreSQLResearchRepository(engine),
            sources=tuple(
                PublicAdapterCollectorSource(adapter, adapter.venue) for adapter in adapters
            ),
            capability_gate=TrustedResearchCapabilityGate(registry),
            instruments=collection.instruments,
            event_types=collection.event_types,
            collection_enabled=True,
            poll_interval_seconds=collection.poll_interval_seconds,
            maximum_cycles=collection.maximum_cycles,
            stale_after_seconds=collection.stale_after_seconds,
        )

        supervisor_started = True
        asyncio.run(
            _run_collector_with_lease(
                collector=collector,
                duration_seconds=10 * 365 * 24 * 3600,
                lease_repository=lease_repository,
                groups=groups,
                run=run,
            )
        )
        result = collector.result
        completed_at = datetime.now(UTC)
        write_collector_health_report(
            artifact_directory / "health.json",
            {
                "run_id": run_id,
                "status": CollectorRunStatus.COMPLETED,
                "health": result.health,
                "production_counts": result.production_counts,
                "experimental_counts": result.experimental_counts,
                "quarantine_count": result.quarantine_count,
                "checkpoint_namespace": "production",
                "live_execution": "OFF",
            },
        )
        lease_repository.save_run(
            replace(
                lease_repository.get_run(run_id) or run,
                status=CollectorRunStatus.COMPLETED,
                heartbeat_at=completed_at,
                stopped_at=completed_at,
                artifact_directory=str(artifact_directory),
            )
        )
        typer.echo(
            f"production={result.production_counts} experimental={result.experimental_counts} "
            f"quarantine={result.quarantine_count} run_id={run_id} live_execution=OFF"
        )
    except CollectorLeaseConflict as exc:
        canceled_at = datetime.now(UTC)
        lease_repository.save_run(
            replace(
                run,
                status=CollectorRunStatus.CANCELED_DUE_TO_OVERLAP,
                heartbeat_at=canceled_at,
                stopped_at=canceled_at,
                failure_reason=str(exc),
            )
        )
        raise typer.BadParameter(str(exc)) from exc
    except Exception as exc:
        failed_at = datetime.now(UTC)
        current = lease_repository.get_run(run_id) or run
        lease_repository.save_run(
            replace(
                current,
                status=CollectorRunStatus.FAILED,
                heartbeat_at=failed_at,
                stopped_at=failed_at,
                failure_reason=f"{type(exc).__name__}: {exc}",
            )
        )
        raise
    finally:
        release_failures: list[str] = []
        if not supervisor_started:
            for group in acquired:
                try:
                    lease_repository.release(group, run_id, owner_id)
                except Exception as exc:
                    release_failures.append(f"{group}:{type(exc).__name__}")
        if release_failures:
            typer.echo(f"collector lease release failures={release_failures}", err=True)
        token_path.unlink(missing_ok=True)
        engine.dispose()


async def _run_collector_with_lease(
    *,
    collector: ResearchMarketDataCollector,
    duration_seconds: float,
    lease_repository: SQLCollectorLeaseRepository,
    groups: tuple[str, ...],
    run: CollectorRunRecord,
    stop_request: asyncio.Event | None = None,
    lease_ttl_seconds: float = 90,
    renewal_timeout_seconds: float = 10,
) -> None:
    """Supervise collection and fail closed whenever lease ownership is uncertain."""
    loop = asyncio.get_running_loop()
    requested_stop = stop_request or asyncio.Event()
    renewal_shutdown = asyncio.Event()
    installed: list[signal.Signals] = []
    for item in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(item, requested_stop.set)
            installed.append(item)
        except (NotImplementedError, RuntimeError):
            pass
    collector_task = asyncio.create_task(collector.run(), name="research-collector-soak")
    timer = asyncio.create_task(asyncio.sleep(duration_seconds), name="collector-soak-timer")
    renewal = asyncio.create_task(
        _renew_collector_leases(
            repository=lease_repository,
            groups=groups,
            run=run,
            shutdown=renewal_shutdown,
            ttl_seconds=lease_ttl_seconds,
            renewal_timeout_seconds=renewal_timeout_seconds,
        ),
        name="collector-lease-renewal",
    )
    stop_task = asyncio.create_task(requested_stop.wait(), name="collector-stop-request")
    failure: BaseException | None = None
    try:
        done, _ = await asyncio.wait(
            {collector_task, timer, renewal, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if renewal in done:
            try:
                renewal.result()
            except BaseException as exc:
                failure = RuntimeError(
                    f"collector lease renewal failed: {type(exc).__name__}: {exc}"
                )
                failure.__cause__ = exc
            else:
                failure = RuntimeError("collector lease renewal stopped unexpectedly")
        elif collector_task in done:
            failure = collector_task.exception()
        collector.shutdown()
        results = await asyncio.gather(collector_task, return_exceptions=True)
        if failure is None and isinstance(results[0], BaseException):
            failure = results[0]
        if failure is not None:
            failed_at = datetime.now(UTC)
            try:
                current = await asyncio.wait_for(
                    asyncio.to_thread(lease_repository.get_run, run.run_id),
                    timeout=renewal_timeout_seconds,
                )
                await asyncio.wait_for(
                    asyncio.to_thread(
                        lease_repository.save_run,
                        replace(
                            current or run,
                            status=CollectorRunStatus.FAILED,
                            heartbeat_at=failed_at,
                            stopped_at=failed_at,
                            failure_reason=f"{type(failure).__name__}: {failure}",
                        ),
                    ),
                    timeout=renewal_timeout_seconds,
                )
            except Exception as status_exc:
                failure.add_note(
                    f"failed to persist terminal run status: "
                    f"{type(status_exc).__name__}: {status_exc}"
                )
            raise failure
    finally:
        collector.shutdown()
        await asyncio.gather(collector_task, return_exceptions=True)
        renewal_shutdown.set()
        timer.cancel()
        stop_task.cancel()
        await asyncio.gather(timer, stop_task, renewal, return_exceptions=True)
        release_failures: list[str] = []
        for group in groups:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        lease_repository.release,
                        group,
                        run.run_id,
                        run.owner_id,
                    ),
                    timeout=renewal_timeout_seconds,
                )
            except Exception as exc:
                release_failures.append(f"{group}:{type(exc).__name__}")
        if release_failures:
            release_failure = RuntimeError(
                f"collector lease release failed for {len(release_failures)} group(s)"
            )
            if failure is not None:
                failure.add_note(str(release_failure))
            else:
                failed_at = datetime.now(UTC)
                try:
                    current = await asyncio.wait_for(
                        asyncio.to_thread(lease_repository.get_run, run.run_id),
                        timeout=renewal_timeout_seconds,
                    )
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            lease_repository.save_run,
                            replace(
                                current or run,
                                status=CollectorRunStatus.FAILED,
                                heartbeat_at=failed_at,
                                stopped_at=failed_at,
                                failure_reason=str(release_failure),
                            ),
                        ),
                        timeout=renewal_timeout_seconds,
                    )
                except Exception as status_exc:
                    release_failure.add_note(
                        f"failed to persist terminal run status: {type(status_exc).__name__}"
                    )
                raise release_failure
        for item in installed:
            loop.remove_signal_handler(item)


def _current_commit_sha() -> str:
    head = Path(".git/HEAD")
    if not head.is_file():
        return "unknown"
    value = head.read_text(encoding="utf-8").strip()
    if value.startswith("ref: "):
        reference = Path(".git") / value.removeprefix("ref: ")
        if reference.is_file():
            return reference.read_text(encoding="utf-8").strip()
    return value


def _process_identity(pid: int) -> tuple[datetime, str]:
    completed = subprocess.run(  # nosec B603
        ("/bin/ps", "-p", str(pid), "-o", "lstart=", "-o", "command="),
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    output = completed.stdout.strip()
    if len(output) < 25:
        raise RuntimeError("collector process identity is unavailable")
    started_text = output[:24]
    command = output[24:].strip()
    local_zone = datetime.now().astimezone().tzinfo
    started_at = datetime.strptime(started_text, "%a %b %d %H:%M:%S %Y").replace(tzinfo=local_zone)
    if not command:
        raise RuntimeError("collector process command is unavailable")
    return started_at.astimezone(UTC), hashlib.sha256(command.encode()).hexdigest()


def _collector_token_path(run_id: str) -> Path:
    token_directory = Path(tempfile.gettempdir()) / "crypttool-collector-run-tokens"
    token_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    return token_directory / f"{hashlib.sha256(run_id.encode()).hexdigest()}.token"


def _create_collector_run_token(run_id: str) -> tuple[Path, str]:
    token = secrets.token_urlsafe(32)
    path = _collector_token_path(run_id)
    with path.open("x", encoding="utf-8") as token_file:
        token_file.write(token)
    path.chmod(0o600)
    return path, hashlib.sha256(token.encode()).hexdigest()


def _verify_collector_process_identity(run: CollectorRunRecord) -> None:
    if socket.gethostname() != run.hostname:
        raise typer.BadParameter("collector hostname identity mismatch")
    try:
        started_at, command_sha256 = _process_identity(run.pid)
    except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
        raise typer.BadParameter("collector process identity is unavailable") from exc
    if abs((started_at - run.process_started_at).total_seconds()) > 1:
        raise typer.BadParameter("collector process start time mismatch")
    if not secrets.compare_digest(command_sha256, run.command_sha256):
        raise typer.BadParameter("collector process command hash mismatch")
    token_path = _collector_token_path(run.run_id)
    try:
        token = token_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise typer.BadParameter("collector run token is unavailable") from exc
    if not secrets.compare_digest(hashlib.sha256(token.encode()).hexdigest(), run.run_token_sha256):
        raise typer.BadParameter("collector run token mismatch")


def _database_identity(engine: Any) -> tuple[str, str]:
    url = engine.url
    if url.get_backend_name() == "sqlite":
        database = Path(str(url.database)).expanduser().resolve()
        return f"sqlite:///{database}", "main"
    identity = url.render_as_string(hide_password=True)
    with engine.connect() as connection:
        schema = str(connection.scalar(text("SELECT current_schema()")) or "public")
    return identity, schema


def _require_nonproduction_database_isolation(settings: Settings, *, run_mode: str) -> None:
    if run_mode == "production":
        return
    if settings.production_database_url is None:
        raise typer.BadParameter(
            f"{run_mode} requires an explicit production_database_url for isolation"
        )
    run_engine = build_engine(settings.database_url)
    production_engine = build_engine(settings.production_database_url)
    try:
        run_identity = _database_identity(run_engine)
        production_identity = _database_identity(production_engine)
    finally:
        run_engine.dispose()
        production_engine.dispose()
    if run_identity == production_identity:
        raise typer.BadParameter(
            f"{run_mode} database/schema must be isolated from the production database"
        )


def _collector_groups(
    *,
    database_identity: str,
    schema_name: str,
    venues: tuple[str, ...],
    instruments: tuple[str, ...],
    event_types: tuple[str, ...],
) -> tuple[str, ...]:
    groups: set[str] = set()
    for venue in venues:
        for instrument in instruments:
            for requested_type in event_types:
                if requested_type in {"orderbook_snapshot", "orderbook_delta", "orderbook"}:
                    event_type, channel = "orderbook", "orderbook"
                elif requested_type == "trade":
                    event_type, channel = requested_type, "trades"
                elif requested_type in {"mark_price", "index_price"}:
                    event_type, channel = requested_type, "ticker"
                else:
                    event_type, channel = requested_type, "rest"
                groups.add(
                    collector_group_key(
                        database_identity=database_identity,
                        schema_name=schema_name,
                        venue=venue,
                        instrument=instrument,
                        event_type=event_type,
                        channel=channel,
                    )
                )
    return tuple(sorted(groups))


async def _renew_collector_leases(
    *,
    repository: SQLCollectorLeaseRepository,
    groups: tuple[str, ...],
    run: CollectorRunRecord,
    shutdown: asyncio.Event,
    ttl_seconds: float = 90,
    renewal_timeout_seconds: float = 10,
) -> None:
    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=ttl_seconds / 3)
            return
        except TimeoutError:
            now = datetime.now(UTC)
            for group in groups:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        repository.renew,
                        group,
                        run.run_id,
                        run.owner_id,
                        now + timedelta(seconds=ttl_seconds),
                    ),
                    timeout=renewal_timeout_seconds,
                )
            current = await asyncio.wait_for(
                asyncio.to_thread(repository.get_run, run.run_id),
                timeout=renewal_timeout_seconds,
            )
            if current is None:
                raise RuntimeError("collector run registry entry disappeared") from None
            await asyncio.wait_for(
                asyncio.to_thread(repository.save_run, replace(current, heartbeat_at=now)),
                timeout=renewal_timeout_seconds,
            )


@app.command("run-collector-soak")
def run_collector_soak(
    config: Annotated[Path, typer.Option("--config", exists=True, readable=True)],
    duration_hours: Annotated[float, typer.Option("--duration-hours", min=0.001)] = 24,
) -> None:
    """Run a bounded collector soak and persist operational health evidence."""
    settings = _settings_from_yaml(config)
    collection = settings.research_collection
    if settings.live_trading or settings.live.enabled:
        raise typer.BadParameter("collector soak requires live execution to remain OFF")
    if not collection.collection_enabled:
        raise typer.BadParameter("collection_enabled=true is required")
    if not collection.venues:
        raise typer.BadParameter("at least one research collection venue is required")
    _require_nonproduction_database_isolation(settings, run_mode="soak")
    adapters = tuple(_research_data_adapter(name) for name in collection.venues)
    registry = TrustedCapabilityRegistry.from_artifacts(
        Path.cwd(), tuple(adapter.capabilities for adapter in adapters)
    )
    engine = build_engine(settings.database_url)
    started_at = datetime.now(UTC)
    run_id = f"collector-soak-{started_at.strftime('%Y%m%dT%H%M%S%fZ')}"
    owner_id = f"{socket.gethostname()}:{os.getpid()}"
    process_started_at, command_sha256 = _process_identity(os.getpid())
    token_path, run_token_sha256 = _create_collector_run_token(run_id)
    checkpoint_namespace = f"soak:{run_id}"
    artifact_directory = Path("artifacts/collector-soak") / run_id
    lease_repository = SQLCollectorLeaseRepository(engine)
    database_identity, schema_name = _database_identity(engine)
    groups = _collector_groups(
        database_identity=database_identity,
        schema_name=schema_name,
        venues=collection.venues,
        instruments=collection.instruments,
        event_types=collection.event_types,
    )
    run = CollectorRunRecord(
        run_id=run_id,
        collector_group=f"collector-set-{canonical_sha256(groups)[:24]}",
        owner_id=owner_id,
        commit_sha=_current_commit_sha(),
        config_path=str(config.resolve()),
        database_identity=database_identity,
        schema_name=schema_name,
        checkpoint_namespace=checkpoint_namespace,
        artifact_namespace=str(artifact_directory),
        venues=collection.venues,
        instruments=collection.instruments,
        event_types=collection.event_types,
        duration_seconds=duration_hours * 3600,
        pid=os.getpid(),
        process_started_at=process_started_at,
        hostname=socket.gethostname(),
        command_sha256=command_sha256,
        run_token_sha256=run_token_sha256,
        status=CollectorRunStatus.RUNNING,
        started_at=started_at,
        heartbeat_at=started_at,
    )
    acquired: list[str] = []
    supervisor_started = False
    try:
        lease_repository.save_run(run)
        expires_at = started_at + timedelta(seconds=90)
        for group in groups:
            lease_repository.acquire(group, run_id, owner_id, expires_at)
            acquired.append(group)
        base_repository = PostgreSQLResearchRepository(engine)
        repository = NamespacedResearchRepository(
            base_repository, checkpoint_namespace=checkpoint_namespace
        )
        collector = ResearchMarketDataCollector(
            repository=repository,
            sources=tuple(
                PublicAdapterCollectorSource(adapter, adapter.venue) for adapter in adapters
            ),
            capability_gate=TrustedResearchCapabilityGate(registry),
            instruments=collection.instruments,
            event_types=collection.event_types,
            collection_enabled=True,
            poll_interval_seconds=collection.poll_interval_seconds,
            maximum_cycles=None,
            stale_after_seconds=collection.stale_after_seconds,
        )

        supervisor_started = True
        asyncio.run(
            _run_collector_with_lease(
                collector=collector,
                duration_seconds=duration_hours * 3600,
                lease_repository=lease_repository,
                groups=groups,
                run=run,
            )
        )
        completed_at = datetime.now(UTC)
        result = collector.result
        payload: dict[str, object] = {
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "configured_duration_hours": duration_hours,
            "production_counts": result.production_counts,
            "experimental_counts": result.experimental_counts,
            "quarantine_count": result.quarantine_count,
            "health": result.health,
            "live_execution": "OFF",
            "collector_run_id": run_id,
            "checkpoint_namespace": checkpoint_namespace,
            "artifact_namespace": str(artifact_directory),
            "database_identity": database_identity,
            "schema_name": schema_name,
            "lease_groups": groups,
        }
        path = artifact_directory / "health.json"
        write_collector_health_report(path, payload)
        current = lease_repository.get_run(run_id) or run
        lease_repository.save_run(
            replace(
                current,
                status=CollectorRunStatus.COMPLETED,
                heartbeat_at=completed_at,
                stopped_at=completed_at,
                artifact_directory=str(artifact_directory),
            )
        )
        typer.echo(f"run_id={run_id} health={path} live_execution=OFF")
    except CollectorLeaseConflict as exc:
        canceled_at = datetime.now(UTC)
        lease_repository.save_run(
            replace(
                run,
                status=CollectorRunStatus.CANCELED_DUE_TO_OVERLAP,
                heartbeat_at=canceled_at,
                stopped_at=canceled_at,
                artifact_directory=str(artifact_directory),
                failure_reason=str(exc),
            )
        )
        write_collector_health_report(
            artifact_directory / "health.json",
            {
                "run_id": run_id,
                "status": CollectorRunStatus.CANCELED_DUE_TO_OVERLAP,
                "reason": str(exc),
                "live_execution": "OFF",
            },
        )
        raise typer.BadParameter(str(exc)) from exc
    except Exception as exc:
        failed_at = datetime.now(UTC)
        current = lease_repository.get_run(run_id) or run
        lease_repository.save_run(
            replace(
                current,
                status=CollectorRunStatus.FAILED,
                heartbeat_at=failed_at,
                stopped_at=failed_at,
                failure_reason=f"{type(exc).__name__}: {exc}",
            )
        )
        raise
    finally:
        release_failures: list[str] = []
        if not supervisor_started:
            for group in acquired:
                try:
                    lease_repository.release(group, run_id, owner_id)
                except Exception as exc:
                    release_failures.append(f"{group}:{type(exc).__name__}")
        if release_failures:
            typer.echo(f"collector lease release failures={release_failures}", err=True)
        token_path.unlink(missing_ok=True)
        engine.dispose()


@app.command("generate-collector-health-report")
def generate_collector_health_report(
    run_id: Annotated[str, typer.Option("--run-id", min=1)],
) -> None:
    """Generate a concise Markdown report from persisted soak health metrics."""
    directory = Path("artifacts/collector-soak") / run_id
    source = directory / "health.json"
    if not source.is_file():
        raise typer.BadParameter(f"unknown collector soak run: {run_id}")
    payload = json.loads(source.read_text(encoding="utf-8"))
    health = payload.get("health", {})
    lines = [
        f"# Collector health: {run_id}",
        "",
        f"- Live execution: {payload.get('live_execution')}",
        f"- Production events: {payload.get('production_counts')}",
        f"- Experimental events: {payload.get('experimental_counts')}",
        f"- Quarantine count: {payload.get('quarantine_count')}",
        f"- Events by venue/type/instrument: {health.get('events_by_venue_type_instrument')}",
        f"- Disconnects: {health.get('disconnect_count')}",
        f"- Reconnects: {health.get('reconnect_count')}",
        f"- Sequence gaps: {health.get('sequence_gaps')}",
        f"- Snapshot recoveries: {health.get('snapshot_recoveries')}",
        f"- Stale duration seconds: {health.get('stale_duration_seconds')}",
        f"- Checkpoint lag seconds: {health.get('checkpoint_lag_seconds')}",
        f"- DB write latency average seconds: "
        f"{health.get('database_write_latency_average_seconds')}",
        f"- DB write latency peak seconds: {health.get('database_write_latency_peak_seconds')}",
        f"- Queue peak: {health.get('queue_peak')}",
        f"- Duplicate ratio: {health.get('duplicate_ratio')}",
        f"- RSS start/end/peak: {health.get('rss_start_bytes')}/"
        f"{health.get('rss_end_bytes')}/{health.get('rss_peak_bytes')}",
        f"- Task count start/end/peak: {health.get('task_count_start')}/"
        f"{health.get('task_count_end')}/{health.get('task_count_peak')}",
    ]
    report = directory / "summary.md"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    typer.echo(f"run_id={run_id} report={report}")


def _collector_run_payload(run: CollectorRunRecord) -> dict[str, object]:
    return {
        **asdict(run),
        "status": run.status.value,
        "live_execution": "OFF",
    }


@app.command("list-collector-runs")
def list_collector_runs(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    """List registered collector runs without exposing database credentials."""
    engine = build_engine(database_url or Settings().database_url)
    try:
        runs = SQLCollectorLeaseRepository(engine).list_runs()
        typer.echo(json.dumps([_collector_run_payload(run) for run in runs], default=_json_default))
    finally:
        engine.dispose()


@app.command("collector-run-status")
def collector_run_status(
    run_id: Annotated[str, typer.Option("--run-id", min=1)],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    """Return the durable registry state for one collector run."""
    engine = build_engine(database_url or Settings().database_url)
    try:
        run = SQLCollectorLeaseRepository(engine).get_run(run_id)
        if run is None:
            raise typer.BadParameter(f"unknown collector run: {run_id}")
        typer.echo(json.dumps(_collector_run_payload(run), default=_json_default))
    finally:
        engine.dispose()


@app.command("stop-collector-run")
def stop_collector_run(
    run_id: Annotated[str, typer.Option("--run-id", min=1)],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    timeout_seconds: Annotated[float, typer.Option("--timeout-seconds", min=1)] = 60,
) -> None:
    """Request SIGINT shutdown and wait for checkpoint flush and lease release."""
    engine = build_engine(database_url or Settings().database_url)
    repository = SQLCollectorLeaseRepository(engine)
    try:
        run = repository.get_run(run_id)
        if run is None:
            raise typer.BadParameter(f"unknown collector run: {run_id}")
        if run.status not in {CollectorRunStatus.RUNNING, CollectorRunStatus.STOP_REQUESTED}:
            typer.echo(f"run_id={run_id} status={run.status.value} already_stopped=true")
            return
        now = datetime.now(UTC)
        if now - run.heartbeat_at > timedelta(seconds=120):
            raise typer.BadParameter(
                "collector heartbeat is stale; refusing to signal a reused PID"
            )
        _verify_collector_process_identity(run)
        repository.save_run(
            replace(
                run,
                status=CollectorRunStatus.STOP_REQUESTED,
                heartbeat_at=now,
                stop_requested_at=now,
            )
        )
        try:
            os.kill(run.pid, signal.SIGINT)
        except ProcessLookupError as exc:
            raise typer.BadParameter("collector PID no longer exists") from exc
        deadline = time.monotonic() + timeout_seconds
        while True:
            current = repository.get_run(run_id)
            if (
                current is not None
                and current.status
                not in {
                    CollectorRunStatus.RUNNING,
                    CollectorRunStatus.STOP_REQUESTED,
                }
                and not repository.has_leases(run_id)
            ):
                typer.echo(f"run_id={run_id} status={current.status.value} checkpoint_flushed=true")
                return
            if time.monotonic() >= deadline:
                raise typer.BadParameter("graceful shutdown timed out; SIGKILL was not used")
            time.sleep(0.25)
    finally:
        engine.dispose()


@app.command("finalize-data-snapshot")
def finalize_data_snapshot(
    cutoff: Annotated[str, typer.Option("--cutoff")],
) -> None:
    """Finalize immutable daily point-in-time membership and canonical manifest."""
    settings = Settings()
    engine = build_engine(settings.database_url)
    try:
        manifest = DataSnapshotService(PostgreSQLResearchRepository(engine)).finalize(
            cutoff_at=_timestamp(cutoff),
            eligibility_policy=SnapshotEligibilityPolicy(
                required_event_types=settings.research_collection.event_types,
                required_venues=settings.research_collection.venues,
                minimum_production_events=(settings.research_collection.minimum_production_events),
                maximum_gap_ratio=Decimal(str(settings.research_collection.maximum_gap_ratio)),
                maximum_stale_ratio=Decimal(str(settings.research_collection.maximum_stale_ratio)),
                require_complete_instrument_rules=(
                    settings.research_collection.require_complete_instrument_rules
                ),
            ),
        )
        path = Path("artifacts/data-snapshots") / manifest.snapshot_id / "manifest.json"
        write_snapshot_manifest(path, manifest)
        typer.echo(
            f"snapshot_id={manifest.snapshot_id} events={len(manifest.events)} "
            f"manifest_sha256={manifest.manifest_sha256} quarantine={manifest.quarantine_count} "
            f"eligibility={manifest.eligibility_status}"
        )
    finally:
        engine.dispose()


@app.command("verify-data-snapshot")
def verify_data_snapshot(
    snapshot_id: Annotated[str, typer.Option("--snapshot-id", min=1)],
) -> None:
    """Reproduce ordered membership and verify every payload and manifest hash."""
    settings = Settings()
    engine = build_engine(settings.database_url)
    try:
        manifest = DataSnapshotService(PostgreSQLResearchRepository(engine)).verify(snapshot_id)
        typer.echo(
            f"snapshot_id={snapshot_id} events={len(manifest.events)} "
            f"manifest_sha256={manifest.manifest_sha256} verified=true"
        )
    finally:
        engine.dispose()


@app.command("paper-trade")
def paper_trade(
    quotes_path: Annotated[Path, typer.Argument(exists=True, readable=True)],
    quantity: Annotated[float, typer.Option(min=0)] = 0.0,
) -> None:
    """Replay quotes through the isolated paper broker; never sends a live order."""
    settings = Settings()
    if not settings.paper_trading or settings.live_trading:
        raise typer.BadParameter("paper-trade requires paper mode on and live mode off")
    frame = pd.read_csv(quotes_path)
    required = {"symbol", "timestamp", "bid", "ask", "bid_size", "ask_size"}
    if not required <= set(frame):
        raise typer.BadParameter(f"quotes CSV missing: {','.join(sorted(required - set(frame)))}")
    broker = PaperBroker(
        Decimal(str(settings.paper.initial_cash)),
        fee_rate=Decimal(str(settings.paper.fee_rate)),
        slippage_bps=Decimal(str(settings.paper.slippage_bps)),
        max_participation=Decimal(str(settings.paper.maximum_participation)),
    )
    first_timestamp = _timestamp(frame.iloc[0]["timestamp"])
    if quantity > 0:
        broker.submit(
            PaperOrderRequest(
                client_order_id="cli-paper-order",
                symbol=str(frame.iloc[0]["symbol"]),
                side=Side.BUY,
                quantity=Decimal(str(quantity)),
            ),
            first_timestamp,
        )
    last_timestamp = first_timestamp
    for _, row in frame.iterrows():
        last_timestamp = _timestamp(row["timestamp"])
        broker.on_quote(
            PaperQuote(
                symbol=str(row["symbol"]),
                timestamp=last_timestamp,
                bid=Decimal(str(row["bid"])),
                ask=Decimal(str(row["ask"])),
                bid_size=Decimal(str(row["bid_size"])),
                ask_size=Decimal(str(row["ask_size"])),
                data_quality_score=float(row.get("data_quality_score", 1.0)),
                sequence=int(row["sequence"]) if "sequence" in row else None,
            )
        )
    snapshot = broker.snapshot(last_timestamp)
    typer.echo(
        f"paper replay complete: fills={len(broker.fills)}, equity={snapshot.equity}, live_orders=0"
    )


@app.command("health-check")
def health_check() -> None:
    """Check configuration and database without touching execution APIs."""
    settings = Settings()
    engine = build_engine(settings.database_url)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    engine.dispose()
    typer.echo("health check passed: configuration and database; execution API not contacted")


def _priority_adapter(name: str) -> PublicRestAdapter:
    adapters: dict[str, type[PublicRestAdapter]] = {
        "hyperliquid": HyperliquidMarketDataAdapter,
        "aster": AsterMarketDataAdapter,
        "bitget": BitgetMarketDataAdapter,
        "mexc": MexcMarketDataAdapter,
    }
    try:
        return adapters[name]()
    except KeyError as exc:
        raise typer.BadParameter(f"unsupported priority venue: {name}") from exc


@app.command("venue-status")
def venue_status() -> None:
    """Print configured eligibility without contacting an execution API."""
    settings = Settings()
    now = datetime.now(UTC)
    for name in sorted(settings.venues):
        item = eligibility_from_settings(settings, name)
        allowed, reason = item.permits_new_orders(now)
        typer.echo(
            f"{name}: status={item.status.value} data={item.api_market_data_available} "
            f"execution_allowed={allowed} reason={reason if not allowed else item.reason}"
        )


@app.command("venue-capabilities")
def venue_capabilities(
    venue: Annotated[str, typer.Argument(help="hyperliquid|aster|bitget|mexc")],
) -> None:
    """Print the audited Priority-1 capability matrix."""
    adapter = _priority_adapter(venue)
    typer.echo(adapter.capabilities.model_dump_json(indent=2))
    asyncio.run(adapter.close())


@app.command("audit-capabilities")
def audit_capabilities() -> None:
    """Fail when an implemented capability lacks checked-in contract evidence."""
    adapters = [
        HyperliquidMarketDataAdapter(),
        AsterMarketDataAdapter(),
        BitgetMarketDataAdapter(),
        MexcMarketDataAdapter(),
        DydxMarketDataAdapter(),
        ParadexMarketDataAdapter(),
        LighterMarketDataAdapter(),
        GmoCoinMarketDataAdapter(),
        BitbankMarketDataAdapter(),
        BitflyerMarketDataAdapter(),
    ]
    auditor = CapabilityContractAuditor(Path.cwd())
    failed = False
    reports = []
    for adapter in adapters:
        report = auditor.audit(adapter, adapter.capabilities)
        reports.append(report)
        status = "PASS" if report.passed else ("N/A" if not report.findings else "FAIL")
        typer.echo(f"{report.venue}: {status} ({len(report.findings)})")
        for finding in report.findings:
            if not finding.passed:
                failed = True
                typer.echo(f"  {finding.capability}: {'; '.join(finding.reasons)}")
    auditor.write_artifact(tuple(reports))

    async def close() -> None:
        for adapter in adapters:
            await adapter.close()

    asyncio.run(close())
    if failed:
        raise typer.Exit(1)


@app.command("public-data-smoke")
def public_data_smoke(
    venue: Annotated[str, typer.Argument(help="hyperliquid|aster|bitget|mexc")],
) -> None:
    """Fetch public market metadata only; never reads credentials or calls execution APIs."""

    async def run() -> tuple[int, bool]:
        adapter = _priority_adapter(venue)
        try:
            markets = await adapter.fetch_markets()
            return len(markets), bool(markets)
        finally:
            await adapter.close()

    count, healthy = asyncio.run(run())
    typer.echo(f"{venue}: public_markets={count} healthy={healthy} execution_calls=0")
    if not healthy:
        raise typer.Exit(1)


@app.command("live-preflight")
def live_preflight(
    interactive_confirmation: Annotated[
        bool, typer.Option(help="Prompt for the non-persisted runtime confirmation")
    ] = False,
) -> None:
    """Evaluate Phase 9 safety gates without sending or preparing an order."""
    settings = Settings()
    runtime_confirmation = (
        typer.prompt("Runtime confirmation", hide_input=True) if interactive_confirmation else ""
    )
    context = LivePreflightContext(
        timestamp=datetime.now(UTC),
        operator_confirmation=runtime_confirmation,
        adapter_name="disabled",
        adapter_is_concrete=False,
        adapter_healthy=False,
        data_quality_score=0,
        websocket_connected=False,
        clock_skew_seconds=999,
        kill_switch_active=True,
        paper_validation_passed=False,
        out_of_sample_validation_passed=False,
    )
    report = evaluate_live_preflight(settings, context)
    typer.echo(report.warning)
    for check in report.checks:
        typer.echo(f"{'PASS' if check.passed else 'FAIL'} {check.name}: {check.reason}")
    if not report.approved:
        raise typer.Exit(2)


@app.command("version")
def version() -> None:
    """Print application version."""
    from app import __version__

    typer.echo(__version__)
