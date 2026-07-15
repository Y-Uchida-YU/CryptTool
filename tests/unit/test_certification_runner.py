from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from app.infrastructure.database.models import Base
from app.services.research import certification_runner as runner
from app.services.research.certification_lifecycle import CertificationLifecycle


def spec(tmp_path: Path, *, executable: str = "python") -> runner.LaunchAgentSpec:
    root = tmp_path / "repo"
    python = root / ".venv" / "bin" / executable
    python.parent.mkdir(parents=True)
    python.write_text("python\n")
    config = tmp_path / "config.yaml"
    config.write_text("live_trading: false\n")
    return runner.LaunchAgentSpec(
        run_id="certification-test",
        python_executable=python.resolve(),
        repository_root=root.resolve(),
        config_path=config.resolve(),
        artifact_root=(tmp_path / "artifacts").resolve(),
        stdout_path=(tmp_path / "logs" / "stdout.log").resolve(),
        stderr_path=(tmp_path / "logs" / "stderr.log").resolve(),
        plist_path=(tmp_path / "agents" / "agent.plist").resolve(),
        duration_minutes=3,
        uid=os.getuid(),
    )


def test_launch_agent_uses_direct_venv_python(tmp_path: Path) -> None:
    value = spec(tmp_path)
    path = runner.write_plist(value)
    payload = plistlib.loads(path.read_bytes())
    assert payload["ProgramArguments"][:3] == [str(value.python_executable), "-m", "app"]
    assert payload["WorkingDirectory"] == str(value.repository_root)
    assert payload["StandardInPath"] == "/dev/null"
    assert set(runner.SAFE_ENVIRONMENT_KEYS) <= set(payload["EnvironmentVariables"])


def test_launch_agent_rejects_uv_run(tmp_path: Path) -> None:
    with pytest.raises(runner.LaunchAgentError, match="uv run"):
        spec(tmp_path, executable="uv").plist()


def test_preflight_failure_prevents_launch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    value = spec(tmp_path)
    registered: list[bool] = []

    def failed(_: runner.LaunchAgentSpec) -> subprocess.CompletedProcess[str]:
        raise runner.LaunchAgentError("preflight failed; not registered")

    monkeypatch.setattr(runner, "run_preflight", failed)
    monkeypatch.setattr(runner, "bootstrap", lambda _: registered.append(True))
    with pytest.raises(runner.LaunchAgentError, match="not registered"):
        runner.preflight_and_bootstrap(value)
    assert registered == []


def test_missing_process_started_triggers_watchdog_cleanup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    unloaded: list[bool] = []
    monkeypatch.setattr(runner, "service_pid", lambda _: 321)
    monkeypatch.setattr(runner, "application_started", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runner, "process_tree", lambda _: [{"pid": 321}])
    monkeypatch.setattr(runner, "unload", lambda _: unloaded.append(True) or "SIGTERM")
    with pytest.raises(runner.LaunchAgentError, match="watchdog failed"):
        runner.watchdog(value, engine, timeout=0)
    assert unloaded == [True]
    payload = json.loads((value.artifact_root / value.run_id / "runner-failure.json").read_text())
    assert "PROCESS_STARTED" in payload["failure_reason"]
    assert payload["process_tree"] == [{"pid": 321}]


def test_wrapper_alive_without_application_is_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    monkeypatch.setattr(runner, "process_command", lambda _: "/tmp/.venv/bin/uv run app")
    assert not runner.application_process_exists(value, 123)


def test_runner_failure_artifact_is_generated(tmp_path: Path) -> None:
    value = spec(tmp_path)
    path = runner.write_runner_failure(value, pid=None, elapsed=60.1, reason="no app")
    payload = json.loads(path.read_text())
    assert payload["run_id"] == value.run_id
    assert payload["executable"] == str(value.python_executable)
    assert payload["arguments"][1:3] == ["-m", "app"]
    assert set(payload["environment"]) == set(runner.SAFE_ENVIRONMENT_KEYS)
    assert payload["failure_reason"] == "no app"


def test_direct_python_launch_survives_parent_exit(tmp_path: Path) -> None:
    marker = tmp_path / "alive.txt"
    child = f"import pathlib,time;time.sleep(.2);pathlib.Path({str(marker)!r}).write_text('alive')"
    parent = (
        "import subprocess,sys;"
        f"subprocess.Popen([sys.executable,'-c',{child!r}],start_new_session=True,"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)"
    )
    subprocess.run([sys.executable, "-c", parent], check=True)
    deadline = time.monotonic() + 2
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert marker.read_text() == "alive"


def test_normal_launch_completes_and_unloads_service(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    calls: list[tuple[str, ...]] = []
    pids = iter((444, None, None))
    monkeypatch.setattr(runner, "service_pid", lambda _: next(pids, None))

    def launchctl(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        del check
        calls.append(arguments)
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(runner, "_launchctl", launchctl)
    assert runner.unload(value, grace_seconds=0) == "SIGTERM"
    assert ("kill", "SIGTERM", value.service) in calls
    assert ("bootout", f"gui/{value.uid}", str(value.plist_path)) in calls


def test_run_preflight_uses_direct_python_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    captured: dict[str, object] = {}

    def completed(arguments: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured.update(arguments=arguments, **kwargs)
        return subprocess.CompletedProcess(arguments, 0, '{"status":"PASS"}\n', "")

    monkeypatch.setattr(subprocess, "run", completed)
    result = runner.run_preflight(value)
    assert result.returncode == 0
    assert captured["arguments"] == [
        str(value.python_executable),
        "-m",
        "app",
        "certification-preflight",
        "--config",
        str(value.config_path),
    ]
    assert captured["cwd"] == value.repository_root
    assert captured["env"] == value.environment
    assert captured["stdin"] is subprocess.DEVNULL


def test_bootstrap_writes_plist_after_successful_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    preflight = subprocess.CompletedProcess([], 0, "PASS", "")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(runner, "run_preflight", lambda _: preflight)
    monkeypatch.setattr(
        runner,
        "_launchctl",
        lambda *arguments, **_kwargs: (
            calls.append(arguments) or subprocess.CompletedProcess(arguments, 0, "", "")
        ),
    )
    assert runner.preflight_and_bootstrap(value) is preflight
    assert value.plist_path.is_file()
    assert calls == [("bootstrap", f"gui/{value.uid}", str(value.plist_path))]


def test_service_pid_and_application_command_detection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    monkeypatch.setattr(
        runner,
        "_launchctl",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, "state = running\n\tpid = 42\n", ""
        ),
    )
    assert runner.service_pid(value) == 42
    monkeypatch.setattr(
        runner,
        "process_command",
        lambda _: f"{value.python_executable} -m app run-market-data-certification",
    )
    assert runner.application_process_exists(value, 42)
    assert not runner.application_process_exists(value, None)
    monkeypatch.setattr(
        runner,
        "_launchctl",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 113, "", "missing"),
    )
    assert runner.service_pid(value) is None


def test_application_started_from_structured_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    value.stdout_path.parent.mkdir(parents=True)
    value.stdout_path.write_text('{"stage":"PROCESS_STARTED"}\n')
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(runner, "application_process_exists", lambda *_args, **_kwargs: True)
    assert runner.application_started(value, engine, pid=9)
    monkeypatch.setattr(runner, "application_process_exists", lambda *_args, **_kwargs: False)
    assert not runner.application_started(value, engine, pid=9)


def test_process_tree_selects_descendants(monkeypatch: pytest.MonkeyPatch) -> None:
    output = (
        "10 1 S 00:01 python -m app run-market-data-certification\n"
        "11 10 S 00:01 child worker\n"
        "12 11 S 00:01 grandchild worker\n"
        "99 1 S 00:01 unrelated\n"
    )
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, output, ""),
    )
    assert [row["pid"] for row in runner.process_tree(10)] == [10, 11, 12]
    assert runner.process_tree(None) == []


def test_unload_escalates_only_after_sigterm_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(runner, "service_pid", lambda _: 88)
    monkeypatch.setattr(
        runner,
        "_launchctl",
        lambda *arguments, **_kwargs: (
            calls.append(arguments) or subprocess.CompletedProcess(arguments, 0, "", "")
        ),
    )
    assert runner.unload(value, grace_seconds=0) == "SIGKILL_AFTER_TIMEOUT"
    assert ("kill", "SIGTERM", value.service) in calls
    assert ("kill", "SIGKILL", value.service) in calls


def test_migration_and_environment_helpers(tmp_path: Path) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("create table alembic_version (version_num varchar(64))"))
        connection.execute(
            text("insert into alembic_version(version_num) values (:version)"),
            {"version": runner.CURRENT_MIGRATION},
        )
    assert runner.migration_is_current(engine)
    with engine.begin() as connection:
        connection.execute(text("update alembic_version set version_num='old'"))
    assert not runner.migration_is_current(engine)
    assert runner.sanitized_environment({"HOME": "/tmp", "SECRET": "hidden"}) == {"HOME": "/tmp"}
    value = spec(tmp_path)
    assert runner.direct_python_command(value) == value.arguments


def test_spec_rejects_relative_and_missing_paths(tmp_path: Path) -> None:
    value = spec(tmp_path)
    with pytest.raises(runner.LaunchAgentError, match="paths must be absolute"):
        replace(value, repository_root=Path("relative")).validate()
    with pytest.raises(runner.LaunchAgentError, match="does not exist"):
        replace(value, python_executable=(tmp_path / "missing-python").resolve()).validate()


def test_run_preflight_reports_subprocess_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 2, "", "bad config"),
    )
    with pytest.raises(runner.LaunchAgentError, match="bad config"):
        runner.run_preflight(value)


def test_process_command_and_malformed_service_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "python -m app\n", ""),
    )
    assert runner.process_command(10) == "python -m app"
    monkeypatch.setattr(
        runner,
        "_launchctl",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "pid = unknown\n", ""),
    )
    assert runner.service_pid(value) is None


def test_application_started_from_registry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    value = spec(tmp_path)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    CertificationLifecycle(
        run_id=value.run_id,
        commit_sha="a" * 40,
        config_path=value.config_path,
        database_identity="sqlite:test",
        artifact_directory=tmp_path / "lifecycle",
        repository=runner.CertificationRunRepository(engine),
    )
    monkeypatch.setattr(runner, "application_process_exists", lambda *_args, **_kwargs: True)
    assert runner.application_started(value, engine, pid=9)


def test_watchdog_returns_real_application_pid(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(runner, "service_pid", lambda _: 909)
    monkeypatch.setattr(runner, "application_started", lambda *_args, **_kwargs: True)
    assert runner.watchdog(value, engine, timeout=1) == 909


def test_unload_when_service_already_exited(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = spec(tmp_path)
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(runner, "service_pid", lambda _: None)
    monkeypatch.setattr(
        runner,
        "_launchctl",
        lambda *arguments, **_kwargs: (
            calls.append(arguments) or subprocess.CompletedProcess(arguments, 0, "", "")
        ),
    )
    assert runner.unload(value) == "ALREADY_EXITED"
    assert calls == [("bootout", f"gui/{value.uid}", str(value.plist_path))]
