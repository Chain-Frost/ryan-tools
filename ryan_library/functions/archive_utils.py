"""Shared discovery and checked execution helpers for 7-Zip workflows."""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


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
    max_part_size_gib: int | None = None,
) -> tuple[Path, ...]:
    """Create an archive, optionally split into size-limited volumes."""
    if max_part_size_gib is not None and max_part_size_gib < 1:
        msg = f"Maximum archive part size must be at least 1 GiB, got {max_part_size_gib}."
        raise ValueError(msg)

    volume_prefix = f"{output_archive.name}."
    existing_volumes = sorted(
        candidate
        for candidate in output_archive.parent.glob(f"{output_archive.name}.*")
        if candidate.name.removeprefix(volume_prefix).isdigit()
    )
    if output_archive.exists() or output_archive.is_symlink() or existing_volumes:
        existing = output_archive if output_archive.exists() or output_archive.is_symlink() else existing_volumes[0]
        msg = f"Output archive already exists: {existing}"
        raise FileExistsError(msg)

    volume_switch = [] if max_part_size_gib is None else [f"-v{max_part_size_gib * 1024**3}b"]
    command = [
        str(executable),
        "a",
        "-mx=5",
        "-mmt=1",
        *volume_switch,
        str(output_archive),
        *(str(value) for value in inputs),
        *exclusions,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            diagnostic = result.stderr.strip() or result.stdout.strip()
            msg = f"7z exit code {result.returncode}: {diagnostic}"
            raise RuntimeError(msg)
        outputs = (
            tuple(
                sorted(
                    (
                        candidate
                        for candidate in output_archive.parent.glob(f"{output_archive.name}.*")
                        if candidate.name.removeprefix(volume_prefix).isdigit()
                    ),
                    key=lambda path: int(path.name.removeprefix(volume_prefix)),
                )
            )
            if max_part_size_gib is not None
            else (output_archive,)
        )
        if not outputs or any(not output.is_file() or output.stat().st_size == 0 for output in outputs):
            msg = f"7-Zip did not create a non-empty archive: {output_archive}"
            raise RuntimeError(msg)
        return outputs
    except BaseException:
        output_archive.unlink(missing_ok=True)
        for volume in output_archive.parent.glob(f"{output_archive.name}.*"):
            if volume.name.removeprefix(volume_prefix).isdigit():
                volume.unlink(missing_ok=True)
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
        msg = f"7z exit code {result.returncode}: {diagnostic}"
        raise RuntimeError(msg)


@dataclass(frozen=True, slots=True)
class FolderCompressionJob:
    """Picklable specification for one folder-to-7z operation."""

    folder: Path
    executable: Path
    excluded_folders: tuple[str, ...] = ()
    excluded_extensions: tuple[str, ...] = ()
    max_part_size_gib: int | None = None


@dataclass(frozen=True, slots=True)
class FolderCompressionResult:
    """Outcome returned to the controlling process for reporting."""

    folder: Path
    archive: Path
    status: Literal["SUCCESS", "SKIPPED", "ERROR"]
    message: str = ""


def compress_folder_7zip(job: FolderCompressionJob) -> FolderCompressionResult:
    """Compress one directory, retaining its original on failure.

    The output name and native 7-Zip exclusion arguments match the original
    archive_compress_folder wrapper. Existing .7z files are never overwritten.
    """
    archive = job.folder.with_suffix(".7z")
    volume_prefix = f"{archive.name}."
    existing_volume = next(
        (
            candidate
            for candidate in archive.parent.glob(f"{archive.name}.*")
            if candidate.name.removeprefix(volume_prefix).isdigit()
        ),
        None,
    )
    if archive.exists() or archive.is_symlink() or existing_volume is not None:
        existing = archive
        if not archive.exists() and not archive.is_symlink() and existing_volume is not None:
            existing = existing_volume
        return FolderCompressionResult(job.folder, existing, "SKIPPED", "Output archive already exists")

    exclusions = [f"-xr!{folder}" for folder in job.excluded_folders]
    for extension in job.excluded_extensions:
        normalized = extension if extension.startswith(".") else f".{extension}"
        exclusions.append(f"-xr!*{normalized}")

    try:
        outputs = create_7zip_archive(
            executable=job.executable,
            output_archive=archive,
            inputs=[f"{job.folder}\\*"],
            exclusions=exclusions,
            max_part_size_gib=job.max_part_size_gib,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        return FolderCompressionResult(job.folder, archive, "ERROR", str(exc))
    message = "" if len(outputs) == 1 else f"Created {len(outputs)} parts"
    return FolderCompressionResult(job.folder, outputs[0], "SUCCESS", message)
