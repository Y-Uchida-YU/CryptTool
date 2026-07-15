from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from app.infrastructure.database.models import Base
from app.services.research import certification_runner as runner


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
