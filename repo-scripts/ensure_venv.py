"""Create or refresh the repo's managed virtual environment."""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV_DIR = ROOT / ".venv"
REQUIREMENTS = ROOT / "requirements.txt"
HASH_FILE = VENV_DIR / ".requirements.sha256"


def _venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _run(cmd: list[str]) -> None:
    subprocess.check_call(cmd)


def _requirements_hash() -> str:
    sha = hashlib.sha256()
    if REQUIREMENTS.exists():
        sha.update(REQUIREMENTS.read_bytes())
    return sha.hexdigest()


def ensure_venv(force_recreate: bool) -> bool:
    recreated = False
    if force_recreate and VENV_DIR.exists():
        shutil.rmtree(VENV_DIR)
        recreated = True

    if not VENV_DIR.exists():
        _run([sys.executable, "-m", "venv", str(VENV_DIR)])
        recreated = True

    desired_hash = _requirements_hash()
    recorded_hash = HASH_FILE.read_text().strip() if HASH_FILE.exists() else None
    needs_sync = recreated or recorded_hash != desired_hash

    if needs_sync and REQUIREMENTS.exists():
        python = str(_venv_python())
        _run([python, "-m", "pip", "install", "-U", "pip"])
        _run([python, "-m", "pip", "install", "-r", str(REQUIREMENTS)])
        HASH_FILE.write_text(desired_hash)

    return VENV_DIR.exists() and not needs_sync


def check_status() -> bool:
    if not VENV_DIR.exists():
        return False
    desired_hash = _requirements_hash()
    if not HASH_FILE.exists():
        return False
    return HASH_FILE.read_text().strip() == desired_hash


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Report whether .venv matches requirements.txt without changing anything.",
    )
    parser.add_argument(
        "--force-recreate",
        action="store_true",
        help="Delete and recreate .venv before installing dependencies.",
    )
    args = parser.parse_args()

    if args.check_only:
        up_to_date = check_status()
        if up_to_date:
            print(".venv is present and matches requirements.txt")
            return 0
        print(".venv is missing or out of date")
        return 1

    up_to_date = ensure_venv(force_recreate=args.force_recreate)
    if up_to_date:
        print(".venv already satisfied requirements.txt")
    else:
        print("Ensured .venv matches requirements.txt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
