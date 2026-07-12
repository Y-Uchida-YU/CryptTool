"""Enforce safety-critical line coverage thresholds from coverage.py JSON."""

import json
import sys
from pathlib import Path
from typing import Any

THRESHOLDS = {
    "risk": (95.0, ("src/app/domain/risk/",)),
    "execution": (
        95.0,
        (
            "src/app/domain/execution/",
            "src/app/domain/portfolio/",
            "src/app/services/backtest/",
        ),
    ),
    "regime": (
        90.0,
        ("src/app/domain/regimes/", "src/app/services/regime_engine/"),
    ),
    "live_execution_interface": (
        95.0,
        (
            "src/app/adapters/exchanges/base.py",
            "src/app/adapters/exchanges/disabled.py",
            "src/app/adapters/exchanges/staged_execution.py",
            "src/app/domain/execution/live_models.py",
            "src/app/infrastructure/database/audit.py",
            "src/app/services/live_trading/",
        ),
    ),
    "venue_market_data_adapters": (
        25.0,
        (
            "src/app/adapters/exchanges/public.py",
            "src/app/adapters/exchanges/domestic.py",
        ),
    ),
    "paper_execution": (95.0, ("src/app/services/paper_trading/",)),
}


def main(path: Path) -> int:
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    files: dict[str, Any] = payload["files"]
    failures: list[str] = []
    for name, (minimum, prefixes) in THRESHOLDS.items():
        selected = [value for key, value in files.items() if key.startswith(prefixes)]
        statements = sum(item["summary"]["num_statements"] for item in selected)
        covered = sum(item["summary"]["covered_lines"] for item in selected)
        percentage = 100 * covered / statements if statements else 0.0
        print(f"{name}: {percentage:.2f}% (required {minimum:.2f}%)")
        if percentage + 1e-9 < minimum:
            failures.append(name)
    if failures:
        print(f"coverage threshold failed: {','.join(failures)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_coverage.py COVERAGE_JSON")
    raise SystemExit(main(Path(sys.argv[1])))
