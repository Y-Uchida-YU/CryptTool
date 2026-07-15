"""Fail-closed macOS LaunchAgent runner for market-data certification."""

from __future__ import annotations

import json
import os
import plistlib
import subprocess  # nosec B404
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.services.research.certification_lifecycle import (
    CertificationRunRepository,
    CertificationRunStatus,
    CertificationStage,
)

CURRENT_MIGRATION = "0017_certification_run_registry"
SAFE_ENVIRONMENT_KEYS = ("HOME", "PATH", "PYTHONUNBUFFERED", "PYTHONPATH", "TMPDIR")


class LaunchAgentError(RuntimeError):
    """Raised when a certification process cannot be launched safely."""


@dataclass(frozen=True)
class LaunchAgentSpec:
    run_id: str
    python_executable: Path
    repository_root: Path
    config_path: Path
    artifact_root: Path
    stdout_path: Path
    stderr_path: Path
    plist_path: Path
    duration_minutes: float
    uid: int

    @property
    def label(self) -> str:
        return f"com.crypttool.{self.run_id}"

    @property
    def service(self) -> str:
        return f"gui/{self.uid}/{self.label}"

    @property
    def arguments(self) -> tuple[str, ...]:
        return (
            str(self.python_executable),
            "-m",
            "app",
            "run-market-data-certification",
            "--config",
            str(self.config_path),
            "--run-id",
            self.run_id,
            "--duration-minutes",
            str(self.duration_minutes),
        )

    @property
    def environment(self) -> dict[str, str]:
        home = Path.home().resolve()
        return {
            "HOME": str(home),
            "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": str((self.repository_root / "src").resolve()),
            "TMPDIR": str(Path(os.environ.get("TMPDIR", "/tmp")).resolve()),
        }

    def validate(self) -> None:
        paths = {
            "python executable": self.python_executable,
            "repository root": self.repository_root,
            "config": self.config_path,
            "artifact root": self.artifact_root,
            "stdout": self.stdout_path,
            "stderr": self.stderr_path,
            "plist": self.plist_path,
        }
        relative = [name for name, path in paths.items() if not path.is_absolute()]
        if relative:
            raise LaunchAgentError(f"LaunchAgent paths must be absolute: {', '.join(relative)}")
        if not self.python_executable.is_file():
            raise LaunchAgentError(f"Python executable does not exist: {self.python_executable}")
        if self.python_executable.name == "uv" or "uv" in self.arguments[:3]:
            raise LaunchAgentError("LaunchAgent refuses uv run")
        if self.arguments[1:3] != ("-m", "app"):
            raise LaunchAgentError("LaunchAgent must start the Python application directly")

    def plist(self) -> dict[str, object]:
        self.validate()
        return {
            "Label": self.label,
            "ProgramArguments": list(self.arguments),
            "WorkingDirectory": str(self.repository_root),
            "EnvironmentVariables": self.environment,
            "StandardInPath": "/dev/null",
            "StandardOutPath": str(self.stdout_path),
            "StandardErrorPath": str(self.stderr_path),
            "ProcessType": "Background",
            "RunAtLoad": True,
            "KeepAlive": False,
        }


def write_plist(spec: LaunchAgentSpec) -> Path:
    payload = spec.plist()
    spec.plist_path.parent.mkdir(parents=True, exist_ok=True)
    spec.stdout_path.parent.mkdir(parents=True, exist_ok=True)
    spec.stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with spec.plist_path.open("wb") as handle:
        plistlib.dump(payload, handle, sort_keys=True)
    return spec.plist_path


def preflight_environment(spec: LaunchAgentSpec) -> dict[str, str]:
    return dict(spec.environment)


def run_preflight(
    spec: LaunchAgentSpec, *, timeout: float = 60
) -> subprocess.CompletedProcess[str]:
    spec.validate()
    result = subprocess.run(  # nosec B603
        [
            str(spec.python_executable),
            "-m",
            "app",
            "certification-preflight",
            "--config",
            str(spec.config_path),
        ],
        cwd=spec.repository_root,
        env=preflight_environment(spec),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise LaunchAgentError(
            "certification preflight failed; LaunchAgent was not registered: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result


def _launchctl(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # nosec B603
        ["/bin/launchctl", *arguments],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=check,
    )


def bootstrap(spec: LaunchAgentSpec) -> None:
    write_plist(spec)
    _launchctl("bootstrap", f"gui/{spec.uid}", str(spec.plist_path))


def preflight_and_bootstrap(spec: LaunchAgentSpec) -> subprocess.CompletedProcess[str]:
    """Register only after the same interpreter and environment pass preflight."""
    result = run_preflight(spec)
    bootstrap(spec)
    return result


def service_pid(spec: LaunchAgentSpec) -> int | None:
    result = _launchctl("print", spec.service, check=False)
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        key, separator, value = line.strip().partition(" = ")
        if separator and key == "pid" and value.isdigit():
            return int(value)
    return None


def process_command(pid: int) -> str:
    result = subprocess.run(  # nosec B603
        ["/bin/ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def application_process_exists(spec: LaunchAgentSpec, pid: int | None) -> bool:
    if pid is None:
        return False
    command = process_command(pid)
    return command.startswith(str(spec.python_executable)) and " -m app " in f" {command} "


def application_started(
    spec: LaunchAgentSpec,
    engine: Engine,
    *,
    pid: int | None = None,
) -> bool:
    if not application_process_exists(spec, pid):
        return False
    record = CertificationRunRepository(engine).get(spec.run_id)
    if record is not None and record.status in {
        CertificationRunStatus.STARTING,
        CertificationRunStatus.RUNNING,
    }:
        return True
    for path in (spec.stdout_path, spec.stderr_path):
        try:
            if CertificationStage.PROCESS_STARTED.value in path.read_text(
                encoding="utf-8", errors="replace"
            ):
                return True
        except OSError:
            continue
    return False


def process_tree(pid: int | None) -> list[dict[str, object]]:
    if pid is None:
        return []
    result = subprocess.run(  # nosec B603
        ["/bin/ps", "-axo", "pid=,ppid=,state=,etime=,command="],
        capture_output=True,
        text=True,
        check=False,
    )
    rows: dict[int, tuple[int, str]] = {}
    raw: dict[int, str] = {}
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 4)
        if len(fields) == 5 and fields[0].isdigit() and fields[1].isdigit():
            child, parent = int(fields[0]), int(fields[1])
            rows[child] = (parent, fields[4])
            raw[child] = line.strip()
    selected = {pid}
    changed = True
    while changed:
        changed = False
        for child, (parent, _) in rows.items():
            if parent in selected and child not in selected:
                selected.add(child)
                changed = True
    return [
        {"pid": child, "ppid": rows[child][0], "command": rows[child][1], "ps": raw[child]}
        for child in sorted(selected)
        if child in rows
    ]


def unload(spec: LaunchAgentSpec, *, grace_seconds: float = 10) -> str:
    pid = service_pid(spec)
    if pid is not None:
        _launchctl("kill", "SIGTERM", spec.service, check=False)
        deadline = time.monotonic() + grace_seconds
        while time.monotonic() < deadline and service_pid(spec) is not None:
            time.sleep(0.2)
        if service_pid(spec) is not None:
            _launchctl("kill", "SIGKILL", spec.service, check=False)
            termination = "SIGKILL_AFTER_TIMEOUT"
        else:
            termination = "SIGTERM"
    else:
        termination = "ALREADY_EXITED"
    _launchctl("bootout", f"gui/{spec.uid}", str(spec.plist_path), check=False)
    return termination


def write_runner_failure(
    spec: LaunchAgentSpec,
    *,
    pid: int | None,
    elapsed: float,
    reason: str,
    captured_process_tree: list[dict[str, object]] | None = None,
) -> Path:
    directory = spec.artifact_root / spec.run_id
    directory.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": spec.run_id,
        "pid": pid,
        "executable": str(spec.python_executable),
        "arguments": list(spec.arguments),
        "working_directory": str(spec.repository_root),
        "environment": {key: spec.environment[key] for key in SAFE_ENVIRONMENT_KEYS},
        "stdout_size": spec.stdout_path.stat().st_size if spec.stdout_path.exists() else 0,
        "stderr_size": spec.stderr_path.stat().st_size if spec.stderr_path.exists() else 0,
        "elapsed": elapsed,
        "process_tree": (
            captured_process_tree if captured_process_tree is not None else process_tree(pid)
        ),
        "failure_reason": reason,
        "created_at": datetime.now(UTC).isoformat(),
    }
    path = directory / "runner-failure.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def watchdog(spec: LaunchAgentSpec, engine: Engine, *, timeout: float = 60) -> int:
    started = time.monotonic()
    last_pid: int | None = None
    while time.monotonic() - started < timeout:
        last_pid = service_pid(spec) or last_pid
        if last_pid is not None and application_started(spec, engine, pid=last_pid):
            return last_pid
        time.sleep(0.25)
    reason = "PROCESS_STARTED and STARTING/RUNNING registry were missing within watchdog timeout"
    failure_pid = service_pid(spec) or last_pid
    tree = process_tree(failure_pid)
    unload(spec)
    path = write_runner_failure(
        spec,
        pid=failure_pid,
        elapsed=time.monotonic() - started,
        reason=reason if tree else f"{reason}; application process absent",
        captured_process_tree=tree,
    )
    raise LaunchAgentError(f"LaunchAgent application-start watchdog failed: {path}")


def migration_is_current(engine: Engine) -> bool:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
        database = inspect(engine)
        if database.has_table("alembic_version"):
            current = str(
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            )
            return current == CURRENT_MIGRATION
        return bool(database.has_table("certification_runs"))


def sanitized_environment(environment: Mapping[str, str]) -> dict[str, str]:
    return {key: environment[key] for key in SAFE_ENVIRONMENT_KEYS if key in environment}


def direct_python_command(spec: LaunchAgentSpec) -> Sequence[str]:
    spec.validate()
    return spec.arguments
