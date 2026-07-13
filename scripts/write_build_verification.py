from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from xml.etree import ElementTree


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--junit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--ci-run-id", required=True)
    args = parser.parse_args()
    coverage = json.loads(args.coverage.read_text(encoding="utf-8"))
    suite = ElementTree.parse(args.junit).getroot()
    totals = coverage["totals"]
    payload = {
        "commit_sha": args.commit_sha,
        "ci_run_id": args.ci_run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "pytest": {
            "tests": int(suite.attrib.get("tests", 0)),
            "failures": int(suite.attrib.get("failures", 0)),
            "errors": int(suite.attrib.get("errors", 0)),
            "skipped": int(suite.attrib.get("skipped", 0)),
        },
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
