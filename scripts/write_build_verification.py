from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from xml.etree import ElementTree


def junit_totals(path: Path) -> dict[str, int]:
    root = ElementTree.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    return {
        name: sum(int(suite.attrib.get(name, 0)) for suite in suites)
        for name in ("tests", "failures", "errors", "skipped")
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--junit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--ci-run-id", required=True)
    args = parser.parse_args()
    coverage = json.loads(args.coverage.read_text(encoding="utf-8"))
    pytest_totals = junit_totals(args.junit)
    totals = coverage["totals"]
    payload = {
        "commit_sha": args.commit_sha,
        "ci_run_id": args.ci_run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "pytest": pytest_totals,
        "coverage": {
            "percent_covered": totals["percent_covered"],
            "covered_lines": totals["covered_lines"],
            "num_statements": totals["num_statements"],
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
