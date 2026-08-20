"""Shared discovery and checked execution helpers for 7-Zip workflows."""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
from collections.abc import Sequence


def find_7zip() -> Path | None:
    """Return the available 7-Zip executable, including standard Windows installs."""
    if discovered := shutil.which("7z"):
        return Path(discovered)
    candidates = (
        Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "7-Zip" / "7z.exe",
        Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "7-Zip" / "7z.exe",
    )
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def create_7zip_archive(
    *,
    executable: Path,
    output_archive: Path,
    inputs: Sequence[str | Path],
    exclusions: Sequence[str] = (),
) -> None:
    """Create one archive and remove a partial output when 7-Zip fails."""
    command = [
        str(executable),
        "a",
        "-mx=5",
        "-mmt=1",
        str(output_archive),
        *(str(value) for value in inputs),
        *exclusions,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            diagnostic = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(f"7z exit code {result.returncode}: {diagnostic}")
    except Exception:
        output_archive.unlink(missing_ok=True)
        raise


def extract_archive(*, archive_path: Path, output_directory: Path, executable: Path | None) -> None:
    """Extract an archive with 7-Zip when available, otherwise use ``shutil`` formats."""
    if executable is None:
        shutil.unpack_archive(filename=str(archive_path), extract_dir=str(output_directory))
        return
    result = subprocess.run(
        [str(executable), "x", "-y", f"-o{output_directory}", str(archive_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        diagnostic = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"7z exit code {result.returncode}: {diagnostic}")
