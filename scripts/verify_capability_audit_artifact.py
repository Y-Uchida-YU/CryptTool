from __future__ import annotations

import argparse
from pathlib import Path

from app.services.capability_audit_artifact import verify_capability_audit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    args = parser.parse_args()
    verify_capability_audit(args.directory, args.commit_sha)


if __name__ == "__main__":
    main()
