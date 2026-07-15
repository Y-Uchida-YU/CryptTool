from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]
from sqlalchemy import create_engine

from app.cli import main as cli
from app.infrastructure.database.models import Base
from app.services.research.certification_lifecycle import (
    CertificationLifecycle,
    CertificationRunRepository,
    CertificationStage,
)
from app.services.research.certification_runner import LaunchAgentError, LaunchAgentSpec


class Example(Enum):
    VALUE = "value"


@pytest.fixture(autouse=True)
def clear_application_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep CI runtime settings from replacing each test's isolated database."""
    for key in ("APP_DATABASE_URL", "APP_LIVE_TRADING", "APP_LIVE__ENABLED"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CRYPTTOOL_ALLOW_TEST_STORAGE", "1")


def config(tmp_path: Path, **overrides: object) -> Path:
    os.environ["CRYPTTOOL_STATE_DIR"] = str((tmp_path / "state").resolve())
    database = tmp_path / "certification.sqlite"
    engine = create_engine(f"sqlite+pysqlite:///{database}")
    Base.metadata.create_all(engine)
    engine.dispose()
    payload: dict[str, object] = {
        "environment": "test",
        "database_url": f"sqlite+pysqlite:///{database}",
        "production_database_url": f"sqlite+pysqlite:///{tmp_path / 'production.sqlite'}",
        "paper_trading": True,
        "live_trading": False,
        "dry_run": True,
        "live": {"enabled": False, "adapter_name": "disabled"},
        "market_data_certification": {
            "enabled": True,
            "venues": ["hyperliquid", "bitget"],
            "instruments": ["BTC"],
            "capabilities": ["trade"],
            "artifact_root": str((tmp_path / "state" / "certification-runs").resolve()),
        },
    }
    payload.update(overrides)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def launch_spec(tmp_path: Path, config_path: Path) -> LaunchAgentSpec:
    root = tmp_path / "repo"
    python = root / ".venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("python\n")
    return LaunchAgentSpec(
        run_id="runner-cli-test",
        python_executable=python.resolve(),
        repository_root=root.resolve(),
        config_path=config_path.resolve(),
        artifact_root=(tmp_path / "artifacts").resolve(),
        stdout_path=(tmp_path / "logs" / "stdout.log").resolve(),
        stderr_path=(tmp_path / "logs" / "stderr.log").resolve(),
        plist_path=(tmp_path / "agents" / "agent.plist").resolve(),
        state_dir=(tmp_path / "state").resolve(),
        duration_minutes=3,
        uid=501,
    )


def test_certification_preflight_command_reports_every_check(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = config(tmp_path)
    cli.certification_preflight(path)
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PASS"
    assert payload["python_executable_exists"] == "PASS"
    assert payload["database_identity_comparison"] == "PASS_DISTINCT"
    assert payload["migration_current"] == "PASS"
    assert payload["artifact_directory_writable"] == "PASS"
    assert payload["live_execution"] == "OFF"


def test_cli_json_and_timestamp_helpers(tmp_path: Path) -> None:
    assert cli._json_default(Decimal("1.2")) == "1.2"
    assert cli._json_default(datetime(2026, 7, 15, tzinfo=UTC)) == "2026-07-15T00:00:00+00:00"
    assert cli._json_default(Example.VALUE) == "value"
    assert cli._json_default(tmp_path) == str(tmp_path)
    with pytest.raises(TypeError, match="cannot serialize"):
        cli._json_default(object())
    with pytest.raises(Exception, match="UTC offset"):
        cli._timestamp("2026-07-15")
    event_id = cli._certified_market_event_id("certification-" + "x" * 200, "event-" + "y" * 200)
    assert event_id.startswith("certified-")
    assert len(event_id) == 74


@pytest.mark.parametrize(
    "overrides,message",
    [
        ({"live_trading": True}, "live mode requires"),
        ({"production_database_url": None}, "production_database_url"),
        ({"exchange_api_key": "refused"}, "refuses execution credentials"),
        (
            {"market_data_certification": {"enabled": False}},
            "market_data_certification.enabled",
        ),
        (
            {
                "market_data_certification": {
                    "enabled": True,
                    "venues": ["hyperliquid", "bitget"],
                    "instruments": ["BTC"],
                    "capabilities": ["trade"],
                    "artifact_root": "relative/artifacts",
                }
            },
            "artifact_root must be absolute",
        ),
    ],
)
def test_certification_preflight_fails_closed(
    tmp_path: Path, overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(Exception, match=message):
        cli._certification_preflight_payload(config(tmp_path, **overrides))


def test_launch_spec_uses_repository_venv_and_absolute_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = config(tmp_path)
    root = tmp_path / "checkout"
    (root / ".venv" / "bin").mkdir(parents=True)
    (root / ".venv" / "bin" / "python").write_text("python\n")
    monkeypatch.chdir(root)
    value, _ = cli._certification_launch_spec(
        config=path, run_id="absolute-run", duration_minutes=15
    )
    assert value.python_executable == root / ".venv" / "bin" / "python"
    assert all(
        item.is_absolute()
        for item in (
            value.repository_root,
            value.config_path,
            value.artifact_root,
            value.stdout_path,
            value.stderr_path,
            value.plist_path,
        )
    )


def test_settings_yaml_honors_runtime_safety_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = config(tmp_path)
    override = f"sqlite+pysqlite:///{tmp_path / 'override.sqlite'}"
    monkeypatch.setenv("APP_DATABASE_URL", override)
    monkeypatch.setenv("APP_LIVE_TRADING", "false")
    monkeypatch.setenv("APP_LIVE__ENABLED", "false")
    settings = cli._settings_from_yaml(path)
    assert settings.database_url == override
    assert not settings.live_trading
    assert not settings.live.enabled


def test_preflight_rejects_stale_migration(tmp_path: Path) -> None:
    path = config(tmp_path)
    settings = cli._settings_from_yaml(path)
    engine = create_engine(settings.database_url)
    with engine.begin() as connection:
        connection.exec_driver_sql("create table alembic_version (version_num varchar(64))")
        connection.exec_driver_sql("insert into alembic_version values ('old')")
    engine.dispose()
    with pytest.raises(Exception, match="migration is stale"):
        cli._certification_preflight_payload(path)


def test_launch_command_detaches_after_watchdog(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = config(tmp_path)
    value = launch_spec(tmp_path, path)
    settings = cli._settings_from_yaml(path)
    engine = create_engine(settings.database_url)
    unloaded: list[bool] = []

    def unload(_: LaunchAgentSpec) -> str:
        unloaded.append(True)
        return "SIGTERM"

    monkeypatch.setattr(cli, "_certification_launch_spec", lambda **_kwargs: (value, settings))
    monkeypatch.setattr(cli, "build_engine", lambda _: engine)
    monkeypatch.setattr(
        cli,
        "preflight_and_bootstrap_certification_launch_agent",
        lambda _: subprocess.CompletedProcess([], 0, '{"status":"PASS"}\n', ""),
    )
    monkeypatch.setattr(cli, "certification_launch_watchdog", lambda *_args, **_kwargs: 77)
    monkeypatch.setattr(cli, "unload_certification_launch_agent", unload)
    cli.launch_market_data_certification(path, value.run_id, 3, False, 60)
    output = capsys.readouterr().out
    assert '"pid": 77' in output
    assert '"live_execution": "OFF"' in output
    assert unloaded == []


def test_normal_waiting_launch_completes_and_unloads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = config(tmp_path)
    value = launch_spec(tmp_path, path)
    settings = cli._settings_from_yaml(path)
    engine = create_engine(settings.database_url)
    lifecycle = CertificationLifecycle(
        run_id=value.run_id,
        commit_sha="a" * 40,
        config_path=path,
        database_identity="sqlite:test",
        artifact_directory=tmp_path / "run-artifact",
        repository=CertificationRunRepository(engine),
        now=datetime(2026, 7, 15, tzinfo=UTC),
    )
    lifecycle.stage(CertificationStage.PROCESS_COMPLETED)
    unloaded: list[bool] = []

    def unload(_: LaunchAgentSpec) -> str:
        unloaded.append(True)
        return "ALREADY_EXITED"

    monkeypatch.setattr(cli, "_certification_launch_spec", lambda **_kwargs: (value, settings))
    monkeypatch.setattr(cli, "build_engine", lambda _: engine)
    monkeypatch.setattr(
        cli,
        "preflight_and_bootstrap_certification_launch_agent",
        lambda _: subprocess.CompletedProcess([], 0, "PASS\n", ""),
    )
    monkeypatch.setattr(cli, "certification_launch_watchdog", lambda *_args, **_kwargs: 77)
    monkeypatch.setattr(cli, "certification_service_pid", lambda _: None)
    monkeypatch.setattr(cli, "unload_certification_launch_agent", unload)
    cli.launch_market_data_certification(path, value.run_id, 3, True, 60)
    assert "status=COMPLETED exit_code=0" in capsys.readouterr().out
    assert unloaded == [True]


def test_failed_waiting_launch_unloads_and_reports_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = config(tmp_path)
    value = launch_spec(tmp_path, path)
    settings = cli._settings_from_yaml(path)
    engine = create_engine(settings.database_url)
    lifecycle = CertificationLifecycle(
        run_id=value.run_id,
        commit_sha="a" * 40,
        config_path=path,
        database_identity="sqlite:test",
        artifact_directory=tmp_path / "run-artifact",
        repository=CertificationRunRepository(engine),
    )
    lifecycle.fail(RuntimeError("failed startup"))
    unloaded: list[bool] = []

    def unload(_: LaunchAgentSpec) -> str:
        unloaded.append(True)
        return "SIGTERM"

    monkeypatch.setattr(cli, "_certification_launch_spec", lambda **_kwargs: (value, settings))
    monkeypatch.setattr(cli, "build_engine", lambda _: engine)
    monkeypatch.setattr(
        cli,
        "preflight_and_bootstrap_certification_launch_agent",
        lambda _: subprocess.CompletedProcess([], 0, "PASS\n", ""),
    )
    monkeypatch.setattr(cli, "certification_launch_watchdog", lambda *_args, **_kwargs: 77)
    monkeypatch.setattr(cli, "certification_service_pid", lambda _: None)
    monkeypatch.setattr(cli, "unload_certification_launch_agent", unload)
    with pytest.raises(LaunchAgentError, match="FAILED: failed startup"):
        cli.launch_market_data_certification(path, value.run_id, 3, True, 60)
    assert unloaded == [True]


def test_waiting_launch_deadline_unloads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = config(tmp_path)
    value = launch_spec(tmp_path, path)
    settings = cli._settings_from_yaml(path)
    engine = create_engine(settings.database_url)
    times = iter((0.0, 1000.0))
    unloaded: list[bool] = []

    def unload(_: LaunchAgentSpec) -> str:
        unloaded.append(True)
        return "SIGTERM"

    monkeypatch.setattr(cli, "_certification_launch_spec", lambda **_kwargs: (value, settings))
    monkeypatch.setattr(cli, "build_engine", lambda _: engine)
    monkeypatch.setattr(
        cli,
        "preflight_and_bootstrap_certification_launch_agent",
        lambda _: subprocess.CompletedProcess([], 0, "PASS\n", ""),
    )
    monkeypatch.setattr(cli, "certification_launch_watchdog", lambda *_args, **_kwargs: 77)
    monkeypatch.setattr(cli.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(cli, "unload_certification_launch_agent", unload)
    with pytest.raises(LaunchAgentError, match="did not complete"):
        cli.launch_market_data_certification(path, value.run_id, 3, True, 60)
    assert unloaded == [True]
