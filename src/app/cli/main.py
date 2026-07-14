from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer
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
from app.services.research.data_operations import (
    DataSnapshotService,
    PublicAdapterCollectorSource,
    ResearchMarketDataCollector,
    TrustedResearchCapabilityGate,
    write_snapshot_manifest,
)
from app.services.research.models import ResearchRunResult
from app.services.research.pipeline import ResearchPipeline
from app.services.research.report import ResearchArtifactWriter
from app.services.research.repository import PostgreSQLResearchRepository
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


@app.command("collect-research-data")
def collect_research_data(
    config: Annotated[Path, typer.Option("--config", exists=True, readable=True)],
) -> None:
    """Collect public market data; unverified capabilities remain experimental."""
    settings = Settings(_yaml_file=config)  # type: ignore[call-arg]
    collection = settings.research_collection
    if settings.live_trading or settings.live.enabled:
        raise typer.BadParameter("research collection requires live execution to remain OFF")
    if not collection.collection_enabled:
        raise typer.BadParameter("collection_enabled=true is required")
    adapters = tuple(_research_data_adapter(name) for name in collection.venues)
    registry = TrustedCapabilityRegistry.from_artifacts(
        Path.cwd(), tuple(adapter.capabilities for adapter in adapters)
    )
    engine = build_engine(settings.database_url)
    try:
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
        asyncio.run(collector.run())
        result = collector.result
        typer.echo(
            f"production={result.production_counts} experimental={result.experimental_counts} "
            f"quarantine={result.quarantine_count} live_execution=OFF"
        )
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
            cutoff_at=_timestamp(cutoff)
        )
        path = Path("artifacts/data-snapshots") / manifest.snapshot_id / "manifest.json"
        write_snapshot_manifest(path, manifest)
        typer.echo(
            f"snapshot_id={manifest.snapshot_id} events={len(manifest.events)} "
            f"manifest_sha256={manifest.manifest_sha256} quarantine={manifest.quarantine_count}"
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
