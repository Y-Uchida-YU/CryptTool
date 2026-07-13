from __future__ import annotations

import argparse
from pathlib import Path

from app.services.capability_audit_artifact import package_capability_audit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    args = parser.parse_args()
    package_capability_audit(args.report, args.manifest, args.commit_sha)


if __name__ == "__main__":
    main()
