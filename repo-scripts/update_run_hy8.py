"""
Utilities for synchronizing the run-hy8 vendored submodule.

Running this script will pull the latest upstream commit for vendor/run_hy8 and
refresh vendor/run_hy8.UPSTREAM with the new commit hash and retrieval date.
"""

from __future__ import annotations

import argparse
import subprocess
from datetime import date
from pathlib import Path

REPO_URL = "https://github.com/Chain-Frost/run-hy8.git"
SUBMODULE_PATH: Path = Path("vendor") / "run_hy8"
UPSTREAM_FILE: Path = Path("vendor") / "run_hy8.UPSTREAM"
INSTRUCTIONS = (
    "Update instructions:\n"
    "1. From repository root run `git submodule update --remote vendor/run_hy8`.\n"
    "2. Verify the new commit hash matches expectations and update this file.\n"
    "3. Commit the submodule pointer change alongside any dependent code changes.\n"
)


def parse_args() -> argparse.Namespace:
    """Collect CLI arguments so the script can be automated or customized."""
    parser = argparse.ArgumentParser(
        description="Update the run-hy8 submodule and refresh metadata.", allow_abbrev=False
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Repository root (defaults to auto-detection based on this script).",
    )
    parser.add_argument(
        "--skip-update",
        action="store_true",
        help="Only rewrite metadata using the currently checked-out submodule commit.",
    )
    return parser.parse_args()


def find_repo_root(preferred: Path | None) -> Path:
    """Walk upward from the guessed location until we find a `.git` directory."""
    if preferred is not None:
        root_candidate: Path = preferred.resolve()
    else:
        root_candidate = Path(__file__).resolve().parent

    for path in (root_candidate, *root_candidate.parents):
        if (path / ".git").exists():
            return path
    raise RuntimeError("Unable to locate repository root (missing .git directory).")


def capture(args: list[str], cwd: Path, *, check: bool = True) -> str:
    """Run a subprocess and return stdout, optionally surfacing non-zero exits."""
    result: subprocess.CompletedProcess[str] = subprocess.run(args=args, cwd=cwd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command {' '.join(args)} failed with exit code {result.returncode}:\n{result.stderr.strip()}"
        )
    return result.stdout.strip()


def update_submodule(root: Path) -> None:
    """Fetch the latest upstream commit for the vendor submodule."""
    capture(
        args=["git", "submodule", "update", "--init", "--remote", str(object=SUBMODULE_PATH).replace("\\", "/")],
        cwd=root,
    )


def current_commit(submodule_dir: Path) -> tuple[str, str]:
    """Read the detached commit hash plus a friendly ref (branch or 'detached')."""
    commit_hash: str = capture(args=["git", "rev-parse", "HEAD"], cwd=submodule_dir)
    try:
        branch: str = capture(args=["git", "symbolic-ref", "--quiet", "--short", "HEAD"], cwd=submodule_dir)
    except RuntimeError:
        branch = "detached"
    return commit_hash, branch


def write_metadata(root: Path, commit_hash: str, branch: str) -> Path:
    """Rewrite vendor/run_hy8.UPSTREAM with the new commit and date."""
    content: str = (
        f"Repository: {REPO_URL}\n"
        f"Commit: {commit_hash} ({branch})\n"
        f"Retrieved: {date.today().isoformat()}\n\n"
        f"{INSTRUCTIONS}"
    )
    target: Path = root / UPSTREAM_FILE
    target.write_text(data=content, encoding="utf-8")
    return target


def main() -> int:
    """High-level orchestration: update submodule (optional) and metadata."""
    args: argparse.Namespace = parse_args()
    root: Path = find_repo_root(preferred=args.root)
    submodule_dir: Path = root / SUBMODULE_PATH
    if not submodule_dir.exists():
        raise FileNotFoundError(f"Submodule directory {submodule_dir} does not exist. Has it been initialized?")

    if not args.skip_update:
        print("Updating vendor/run_hy8 from upstream ...")
        update_submodule(root)

    commit_hash, branch = current_commit(submodule_dir=submodule_dir)
    metadata_path: Path = write_metadata(root=root, commit_hash=commit_hash, branch=branch)

    print(f"Vendor submodule now points at {commit_hash} ({branch}).")
    print(f"Wrote metadata to {metadata_path.relative_to(root)}.")
    print("Remember to commit the submodule pointer and metadata file together.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
