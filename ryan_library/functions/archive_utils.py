"""Shared discovery and checked execution helpers for 7-Zip workflows."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


def find_7zip() -> Path | None:
    """Return the available 7-Zip executable, including standard Windows installs."""
    if discovered := shutil.which("7z"):
        return Path(discovered).absolute()
    candidates = (
        Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "7-Zip" / "7z.exe",
        Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "7-Zip" / "7z.exe",
    )
    return next((candidate.absolute() for candidate in candidates if candidate.is_file()), None)


def archive_path_for_folder(folder: Path) -> Path:
    """Append .7z without dropping the last component of a dotted folder name."""
    if not folder.name:
        msg = f"Cannot name an archive for a filesystem root: {folder}"
        raise ValueError(msg)
    return folder.with_name(f"{folder.name}.7z")


def _existing_archive_artifact(archive: Path) -> Path | None:
    """Detect archive and volume collisions, including case aliases and broken links."""
    base = archive.name.casefold()
    prefix = f"{base}."
    for candidate in sorted(archive.parent.iterdir(), key=lambda path: path.name.casefold()):
        name = candidate.name.casefold()
        if name == base or (name.startswith(prefix) and name[len(prefix) :].isdigit()):
            return candidate
    return None


def _same_file_identity(path: Path, original: os.stat_result) -> bool:
    """Only remove a published file while its native identity still matches our staged file."""
    try:
        current = path.lstat()
    except FileNotFoundError:
        return False
    return bool(original.st_ino and current.st_ino == original.st_ino and current.st_dev == original.st_dev)


def _publish_archive_part(staged: Path, destination: Path) -> None:
    """Publish without replacing a concurrent creator on the same filesystem."""
    existing = next(
        (
            candidate
            for candidate in destination.parent.iterdir()
            if candidate.name.casefold() == destination.name.casefold()
        ),
        None,
    )
    if existing is not None:
        msg = f"Output archive already exists: {existing}"
        raise FileExistsError(msg)
    if os.name == "nt":
        # Windows rename fails rather than replacing a destination created after the scan.
        staged.rename(destination)
    else:
        # POSIX rename would overwrite a concurrent creator; hard links cannot.
        os.link(staged, destination)


def _run_7zip_to_stage(
    *,
    executable: Path,
    staged_archive: Path,
    inputs: Sequence[str | Path],
    exclusions: Sequence[str],
    max_part_size_gib: int | None,
) -> None:
    volume_switch = [] if max_part_size_gib is None else [f"-v{max_part_size_gib * 1024**3}b"]
    command = [
        str(executable),
        "a",
        "-mx=5",
        "-mmt=1",
        *volume_switch,
        str(staged_archive),
        *(str(value) for value in inputs),
        *exclusions,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        diagnostic = result.stderr.strip() or result.stdout.strip()
        msg = f"7z exit code {result.returncode}: {diagnostic}"
        raise RuntimeError(msg)


def _staged_archive_outputs(staged_archive: Path, *, split: bool, destination: Path) -> tuple[Path, ...]:
    prefix = f"{staged_archive.name}."
    outputs = (
        tuple(
            sorted(
                (
                    path
                    for path in staged_archive.parent.iterdir()
                    if path.name.startswith(prefix) and path.name[len(prefix) :].isdigit()
                ),
                key=lambda path: int(path.name[len(prefix) :]),
            )
        )
        if split
        else (staged_archive,)
    )
    if not outputs or any(not output.is_file() or output.stat().st_size == 0 for output in outputs):
        msg = f"7-Zip did not create a non-empty archive: {destination}"
        raise RuntimeError(msg)
    if split:
        numbers = [int(output.name[len(prefix) :]) for output in outputs]
        if numbers != list(range(1, len(outputs) + 1)):
            msg = f"7-Zip produced incomplete or non-sequential volume names: {destination}"
            raise RuntimeError(msg)
    return outputs


def create_7zip_archive(
    *,
    executable: Path,
    output_archive: Path,
    inputs: Sequence[str | Path],
    exclusions: Sequence[str] = (),
    max_part_size_gib: int | None = None,
) -> tuple[Path, ...]:
    """Create complete archive/volumes in owned staging and publish without overwrite.

    Each output part is independently no-clobber published. A failure rolls back
    only parts with the same filesystem identity as this invocation's staging.
    A machine/process crash during multi-volume publication may leave a partial
    set requiring operator inspection; publishing several files is not atomic.
    """
    if max_part_size_gib is not None and max_part_size_gib < 1:
        msg = f"Maximum archive part size must be at least 1 GiB, got {max_part_size_gib}."
        raise ValueError(msg)

    if existing := _existing_archive_artifact(output_archive):
        msg = f"Output archive already exists: {existing}"
        raise FileExistsError(msg)

    lock_path = output_archive.with_name(f".{output_archive.name}.writing")
    try:
        lock = lock_path.open("x+b")
    except FileExistsError as exc:
        msg = f"Archive publication already in progress or lock requires inspection: {lock_path}"
        raise FileExistsError(msg) from exc

    lock_identity = os.fstat(lock.fileno())
    published: list[tuple[Path, os.stat_result]] = []
    try:
        with (
            lock,
            tempfile.TemporaryDirectory(
                prefix=f".{output_archive.name}.staging-",
                dir=output_archive.parent,
            ) as temp_directory,
        ):
            staged_archive = Path(temp_directory) / output_archive.name
            _run_7zip_to_stage(
                executable=executable,
                staged_archive=staged_archive,
                inputs=inputs,
                exclusions=exclusions,
                max_part_size_gib=max_part_size_gib,
            )
            stages = _staged_archive_outputs(
                staged_archive,
                split=max_part_size_gib is not None,
                destination=output_archive,
            )

            if existing := _existing_archive_artifact(output_archive):
                msg = f"Output archive already exists: {existing}"
                raise FileExistsError(msg)

            for stage in stages:
                destination = output_archive.parent / stage.name
                stage_identity = stage.lstat()
                _publish_archive_part(stage, destination)
                published.append((destination, stage_identity))
            return tuple(path for path, _identity in published)
    except BaseException:
        for destination, identity in reversed(published):
            if _same_file_identity(destination, identity):
                destination.unlink()
        raise
    finally:
        if _same_file_identity(lock_path, lock_identity):
            lock_path.unlink()


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

    Dotted folder names keep their entire name before .7z; inputs are never
    deleted, and existing archive/volume destinations are not overwritten.
    """
    archive = archive_path_for_folder(job.folder)
    if existing := _existing_archive_artifact(archive):
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
