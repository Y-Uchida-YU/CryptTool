from __future__ import annotations

import os
import subprocess  # nosec B404
from pathlib import Path


def _valid_sha(value: str | None) -> str | None:
    if value is None:
        return None
    candidate = value.strip().lower()
    if len(candidate) == 40 and all(character in "0123456789abcdef" for character in candidate):
        return candidate
    return None


def _git(*arguments: str, cwd: Path) -> str | None:
    try:
        completed = subprocess.run(  # nosec B603
            ("git", *arguments),
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip()


def _git_reference(git_dir: Path, reference_name: str) -> str | None:
    candidates = [git_dir]
    common_marker = git_dir / "commondir"
    if common_marker.is_file():
        common_dir = Path(common_marker.read_text(encoding="utf-8").strip())
        if not common_dir.is_absolute():
            common_dir = (git_dir / common_dir).resolve()
        candidates.append(common_dir)
    for candidate in candidates:
        reference = candidate / reference_name
        parsed = _valid_sha(reference.read_text(encoding="utf-8") if reference.is_file() else None)
        if parsed is not None:
            return parsed
        packed_refs = candidate / "packed-refs"
        if packed_refs.is_file():
            for line in packed_refs.read_text(encoding="utf-8").splitlines():
                if line.startswith(("#", "^")):
                    continue
                fields = line.split(" ", maxsplit=1)
                if len(fields) == 2 and fields[1] == reference_name:
                    parsed = _valid_sha(fields[0])
                    if parsed is not None:
                        return parsed
    return None


def resolve_commit_sha(
    *,
    cwd: Path | None = None,
    explicit_sha: str | None = None,
    environment: dict[str, str] | None = None,
) -> str:
    """Resolve an exact commit identity or fail closed.

    Git commands are authoritative. The gitdir parser covers linked worktrees when
    subprocess discovery is constrained. CI and explicit configuration are last-resort
    identities and must still be full SHA-1 values.
    """
    root = cwd or Path.cwd()
    direct = _valid_sha(_git("rev-parse", "HEAD", cwd=root))
    if direct is not None:
        return direct

    top_level = _git("rev-parse", "--show-toplevel", cwd=root)
    if top_level:
        from_top = _valid_sha(_git("rev-parse", "HEAD", cwd=Path(top_level)))
        if from_top is not None:
            return from_top

    dot_git = root / ".git"
    if dot_git.is_file():
        marker = dot_git.read_text(encoding="utf-8").strip()
        if marker.startswith("gitdir: "):
            git_dir = Path(marker.removeprefix("gitdir: "))
            if not git_dir.is_absolute():
                git_dir = (root / git_dir).resolve()
            head = git_dir / "HEAD"
            if head.is_file():
                value = head.read_text(encoding="utf-8").strip()
                if value.startswith("ref: "):
                    parsed = _git_reference(git_dir, value.removeprefix("ref: "))
                else:
                    parsed = _valid_sha(value)
                if parsed is not None:
                    return parsed

    values = environment if environment is not None else os.environ
    for name in ("GITHUB_SHA", "CI_COMMIT_SHA"):
        parsed = _valid_sha(values.get(name))
        if parsed is not None:
            return parsed
    parsed_explicit = _valid_sha(explicit_sha)
    if parsed_explicit is not None:
        return parsed_explicit
    raise RuntimeError("unable to resolve a full commit SHA")
