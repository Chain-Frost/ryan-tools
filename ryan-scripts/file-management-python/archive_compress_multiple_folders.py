"""Combine multiple folders into a size-limited ZIP archive using 7-Zip.

Each input folder is stored as a top-level folder in the archive, including
when the source folders have different parent directories or drives. Existing
archives are never overwritten, and input folders are not modified. 7-Zip is
required; the script can discover standard Windows installations automatically.
Outputs larger than 5 GiB are split into ``.zip.001``, ``.zip.002``, and so on
so that every file is suitable for online upload.

Example:
    python archive_compress_multiple_folders.py `
        -i "C:\\Data\\Run 1" "D:\\Results\\Run 2" -o "Q:\\Archives\\selected-runs.zip"

Alternatively, paste paths into DEFAULT_INPUT_PATHS and DEFAULT_OUTPUT_PATH
below, then run the script without command-line arguments.
"""

from __future__ import annotations

from pathlib import Path

SCRIPT_VERSION = "2026-11-01.2"

# ---------------------------------------------------------------------------
# Editable settings (command-line arguments override these values)
# ---------------------------------------------------------------------------
MAX_ARCHIVE_PART_SIZE_GIB = 5
MAX_ARCHIVE_PART_SIZE_BYTES = MAX_ARCHIVE_PART_SIZE_GIB * 1024**3
DEFAULT_INPUT_PATHS: tuple[Path, ...] = (
    Path(r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO\TUFLOW_MLGD\results\v06"),
    Path(r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO\TUFLOW_MLGD\results\v07"),
    Path(r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO\TUFLOW_MLGD\results\v12"),
)
DEFAULT_OUTPUT_PATH = Path(
    r"P:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    r"\7 DOCUMENT CONTROL\Documents_History\Results.zip"
)
DEFAULT_7ZIP_EXE: Path | None = None  # None = auto-discover 7z.exe
DEFAULT_EXCLUDED_FOLDERS: tuple[str, ...] = ()
DEFAULT_EXCLUDED_EXTENSIONS: tuple[str, ...] = ()
DEFAULT_DRY_RUN = False
DEFAULT_SHOW_PROGRESS = True
# ---------------------------------------------------------------------------

import argparse
import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence


def find_7zip() -> Path | None:
    """Return the available 7-Zip executable, including standard Windows installs."""
    if discovered := shutil.which("7z"):
        return Path(discovered).absolute()
    candidates = (
        Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "7-Zip" / "7z.exe",
        Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "7-Zip" / "7z.exe",
    )
    return next((candidate.absolute() for candidate in candidates if candidate.is_file()), None)


def _normalise_extension(extension: str) -> str:
    return extension if extension.startswith(".") else f".{extension}"


def _seven_zip_version(executable: Path) -> str:
    try:
        result = subprocess.run(
            [str(executable)],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    for line in result.stdout.splitlines():
        if "7-Zip" in line:
            return line.strip()
    return "unknown"


def _publish_archive(temporary_archive: Path, output_archive: Path, *, show_progress: bool) -> None:
    """Publish a completed archive without overwriting an output created concurrently."""
    try:
        os.link(temporary_archive, output_archive)
    except FileExistsError:
        msg = f"Output archive already exists: {output_archive}"
        raise FileExistsError(msg) from None
    except OSError:
        # Some mapped/network drives do not support hard links. Exclusive creation
        # retains the no-overwrite guarantee, at the cost of one final file copy.
        try:
            with temporary_archive.open("rb") as source, output_archive.open("xb") as destination:
                total_bytes = temporary_archive.stat().st_size
                copied_bytes = 0
                last_reported_percent = -1
                while chunk := source.read(8 * 1024 * 1024):
                    destination.write(chunk)
                    copied_bytes += len(chunk)
                    percent = int(copied_bytes * 100 / total_bytes) if total_bytes else 100
                    if show_progress and (percent >= last_reported_percent + 5 or copied_bytes == total_bytes):
                        print(f"\rPublishing {output_archive.name}: {percent:3d}%", end="", flush=True)
                        last_reported_percent = percent
                if show_progress:
                    print()
        except FileExistsError:
            msg = f"Output archive already exists: {output_archive}"
            raise FileExistsError(msg) from None
        except Exception:
            output_archive.unlink(missing_ok=True)
            raise
    temporary_archive.unlink()


def _temporary_archive_parts(temporary_archive: Path) -> list[Path]:
    """Return the numbered volume files created for a temporary archive."""
    prefix = f"{temporary_archive.name}."
    parts = [
        candidate
        for candidate in temporary_archive.parent.glob(f"{temporary_archive.name}.*")
        if candidate.name.removeprefix(prefix).isdigit()
    ]
    return sorted(parts, key=lambda path: int(path.name.removeprefix(prefix)))


def _output_archive_parts(output_archive: Path, part_count: int) -> list[Path]:
    """Return final names, retaining a normal .zip name for a single volume."""
    if part_count == 1:
        return [output_archive]
    width = max(3, len(str(part_count)))
    return [
        output_archive.with_name(f"{output_archive.name}.{number:0{width}d}") for number in range(1, part_count + 1)
    ]


def _publish_archive_parts(
    temporary_parts: Sequence[Path],
    output_archive: Path,
    *,
    show_progress: bool,
) -> list[Path]:
    """Publish a complete set of archive volumes without overwriting existing files."""
    output_parts = _output_archive_parts(output_archive, len(temporary_parts))
    collision = next((path for path in output_parts if path.exists() or path.is_symlink()), None)
    if collision is not None:
        msg = f"Output archive already exists: {collision}"
        raise FileExistsError(msg)

    published: list[Path] = []
    try:
        for temporary_part, output_part in zip(temporary_parts, output_parts, strict=True):
            print(f"Publishing {output_part.name} ({_format_file_size(temporary_part.stat().st_size)})", flush=True)
            _publish_archive(temporary_part, output_part, show_progress=show_progress)
            published.append(output_part)
    except BaseException:
        for output_part in published:
            output_part.unlink(missing_ok=True)
        raise
    return output_parts


def _format_file_size(size_bytes: int) -> str:
    """Return a concise binary file size for console reporting."""
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} GiB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.1f} MiB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KiB"
    return f"{size_bytes} bytes"


def create_zip_from_folders(
    *,
    folders: Sequence[Path],
    output_archive: Path,
    executable: Path,
    excluded_folders: Sequence[str] = (),
    excluded_extensions: Sequence[str] = (),
    show_progress: bool = True,
) -> list[Path]:
    """Create a ZIP volume set containing each supplied folder at the archive root."""
    temporary_archive = output_archive.with_name(f".{output_archive.stem}.{uuid.uuid4().hex}.partial.zip")
    exclusions = [f"-xr!{folder}" for folder in excluded_folders]
    exclusions.extend(f"-xr!*{_normalise_extension(extension)}" for extension in excluded_extensions)

    try:
        command = [
            str(executable),
            "a",
            "-tzip",
            "-mx=5",
            "-mmt=on",
            *(["-bsp1"] if show_progress else []),
            f"-v{MAX_ARCHIVE_PART_SIZE_BYTES}b",
            str(temporary_archive),
            *(str(folder) for folder in folders),
            *exclusions,
        ]
        print("Stage 1/2: Scanning and compressing with 7-Zip...", flush=True)
        result = subprocess.run(command, check=False)
        if result.returncode != 0:
            msg = f"7-Zip exit code {result.returncode}; see the console output above"
            raise RuntimeError(msg)

        temporary_parts = _temporary_archive_parts(temporary_archive)
        if not temporary_parts or any(part.stat().st_size == 0 for part in temporary_parts):
            msg = f"7-Zip did not create a non-empty archive volume set: {temporary_archive}"
            raise RuntimeError(msg)
        oversized_part = next(
            (part for part in temporary_parts if part.stat().st_size > MAX_ARCHIVE_PART_SIZE_BYTES),
            None,
        )
        if oversized_part is not None:
            msg = f"Archive volume exceeds the 5 GiB limit: {oversized_part}"
            raise RuntimeError(msg)
        total_size = sum(part.stat().st_size for part in temporary_parts)
        print(
            f"Compression complete: {len(temporary_parts)} temporary part(s), {_format_file_size(total_size)} total.",
            flush=True,
        )
        print("Stage 2/2: Publishing completed archive file(s)...", flush=True)
        return _publish_archive_parts(temporary_parts, output_archive, show_progress=show_progress)
    except BaseException:
        temporary_archive.unlink(missing_ok=True)
        for temporary_part in _temporary_archive_parts(temporary_archive):
            temporary_part.unlink(missing_ok=True)
        raise


def _validate_folder_names(folders: Sequence[Path]) -> None:
    """Reject duplicate archive-root folder names before compression."""
    folded_names = [folder.name.casefold() for folder in folders]
    duplicate_names = sorted(name for name in set(folded_names) if folded_names.count(name) > 1)
    if duplicate_names:
        names = ", ".join(duplicate_names)
        msg = f"Input folders must have unique top-level names; duplicate name(s): {names}"
        raise ValueError(msg)


def _validated_output_path(output_path: Path) -> Path:
    """Validate archive naming and reject existing destinations or volumes."""
    output_archive = output_path.absolute()
    if not output_archive.suffix:
        output_archive = output_archive.with_suffix(".zip")
    elif output_archive.suffix.casefold() != ".zip":
        msg = f"Output archive must use the .zip extension: {output_archive}"
        raise ValueError(msg)
    if not output_archive.parent.is_dir():
        msg = f"Output directory does not exist: {output_archive.parent}"
        raise ValueError(msg)
    existing_part = next(
        (
            candidate
            for candidate in output_archive.parent.glob(f"{output_archive.name}.*")
            if candidate.name.removeprefix(f"{output_archive.name}.").isdigit()
        ),
        None,
    )
    if output_archive.exists() or output_archive.is_symlink():
        msg = f"Output archive already exists: {output_archive}"
        raise FileExistsError(msg)
    if existing_part is not None:
        msg = f"Output archive part already exists: {existing_part}"
        raise FileExistsError(msg)

    return output_archive


def _validated_paths(input_paths: Sequence[Path], output_path: Path) -> tuple[list[Path], Path]:
    folders: list[Path] = []
    seen_paths: set[str] = set()
    for supplied_path in input_paths:
        folder = supplied_path.absolute()
        spelling = os.path.normcase(str(folder))
        if spelling in seen_paths:
            continue
        seen_paths.add(spelling)
        if not folder.is_dir():
            msg = f"Input folder does not exist or is not a directory: {folder}"
            raise ValueError(msg)
        folders.append(folder)

    if not folders:
        msg = "At least one input folder is required."
        raise ValueError(msg)

    _validate_folder_names(folders)

    output_archive = _validated_output_path(output_path)

    # Physical containment is intentional here: following links prevents the
    # temporary archive from being created inside a source via a junction/symlink.
    physical_output = output_archive.resolve(strict=False)
    for folder in folders:
        if physical_output.is_relative_to(folder.resolve(strict=True)):
            msg = f"Output archive must not be inside an input folder: {folder}"
            raise ValueError(msg)
    return folders, output_archive


def _print_request(
    folders: Sequence[Path],
    output_archive: Path,
    executable: Path | None,
    configured_excluded_folders: Sequence[str],
    configured_excluded_extensions: Sequence[str],
) -> None:
    """Show configured inputs and executable details before processing."""
    print(f"Script: {Path(__file__).name} {SCRIPT_VERSION}")
    print(f"Python: {sys.version.split()[0]}")
    if executable is not None and executable.is_file():
        print(f"7-Zip: {_seven_zip_version(executable)} ({executable})")
    else:
        print("7-Zip: not currently available (required for a real run)")
    print(f"Output: {output_archive}")
    print(f"Maximum file size: {MAX_ARCHIVE_PART_SIZE_GIB} GiB")
    print("Folders:")
    for folder in folders:
        print(f"  - {folder}")
    if configured_excluded_folders:
        print(f"Excluded folder names: {', '.join(configured_excluded_folders)}")
    if configured_excluded_extensions:
        print(f"Excluded extensions: {', '.join(map(_normalise_extension, configured_excluded_extensions))}")


def main(
    *,
    input_paths: Sequence[Path],
    output_path: Path,
    seven_zip_exe: Path | None = None,
    force_auto_discovery: bool = False,
    excluded_folders: Sequence[str] | None = None,
    excluded_extensions: Sequence[str] | None = None,
    dry_run: bool | None = None,
    show_progress: bool | None = None,
) -> int:
    """Validate the request and combine the selected folders into one ZIP."""
    try:
        folders, output_archive = _validated_paths(input_paths, output_path)
        configured_exe = (
            None if force_auto_discovery else seven_zip_exe if seven_zip_exe is not None else DEFAULT_7ZIP_EXE
        )
        executable = configured_exe.absolute() if configured_exe is not None else find_7zip()
        configured_excluded_folders = (
            tuple(excluded_folders) if excluded_folders is not None else DEFAULT_EXCLUDED_FOLDERS
        )
        configured_excluded_extensions = (
            tuple(excluded_extensions) if excluded_extensions is not None else DEFAULT_EXCLUDED_EXTENSIONS
        )
        configured_dry_run = dry_run if dry_run is not None else DEFAULT_DRY_RUN
        configured_show_progress = show_progress if show_progress is not None else DEFAULT_SHOW_PROGRESS
        if (executable is None or not executable.is_file()) and not configured_dry_run:
            location = executable if executable is not None else "auto-discovery"
            msg = f"7-Zip executable not found ({location}). Use --seven-zip-exe."
            raise ValueError(msg)

        _print_request(folders, output_archive, executable, configured_excluded_folders, configured_excluded_extensions)
        if configured_dry_run:
            print("Dry run complete; no archive was created.")
            return 0

        if executable is None:
            msg = "7-Zip executable is required for a real run."
            raise ValueError(msg)

        started_at = time.monotonic()
        created_archives = create_zip_from_folders(
            folders=folders,
            output_archive=output_archive,
            executable=executable,
            excluded_folders=configured_excluded_folders,
            excluded_extensions=configured_excluded_extensions,
            show_progress=configured_show_progress,
        )
        elapsed_seconds = time.monotonic() - started_at
    except (FileExistsError, OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if len(created_archives) == 1:
        archive_size = _format_file_size(created_archives[0].stat().st_size)
        print(f"Created ZIP archive: {created_archives[0]} ({archive_size})")
    else:
        print(f"Created {len(created_archives)} ZIP archive parts:")
        for created_archive in created_archives:
            print(f"  - {created_archive} ({_format_file_size(created_archive.stat().st_size)})")
    print(f"Elapsed time: {elapsed_seconds / 60:.1f} minutes")
    return 0


def _parse_cli_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-i",
        "--input-paths",
        type=Path,
        nargs="+",
        default=None,
        help="Folders to add to the ZIP; overrides DEFAULT_INPUT_PATHS.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output ZIP path; overrides DEFAULT_OUTPUT_PATH. The .zip extension is added when omitted.",
    )
    executable_group = parser.add_mutually_exclusive_group()
    executable_group.add_argument(
        "--seven-zip-exe",
        type=Path,
        default=None,
        help="Explicit 7z executable path; otherwise automatically discovered.",
    )
    executable_group.add_argument(
        "--auto-seven-zip",
        action="store_true",
        help="Force automatic 7-Zip discovery, overriding an edited DEFAULT_7ZIP_EXE.",
    )
    parser.add_argument(
        "--exclude-folders",
        "--exclude-folder",
        action="extend",
        nargs="*",
        default=None,
        help="Folder names to exclude recursively; repeat the option or supply no names to clear edited defaults.",
    )
    parser.add_argument(
        "--exclude-extensions",
        "--exclude-extension",
        action="extend",
        nargs="*",
        default=None,
        help="Extensions to exclude recursively; repeat the option or supply no values to clear edited defaults.",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Validate and show the plan without creating a ZIP.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Show or hide live 7-Zip and publication progress.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {SCRIPT_VERSION}")
    return parser.parse_args(argv)


if __name__ == "__main__":
    arguments = _parse_cli_arguments()
    raise SystemExit(
        main(
            input_paths=arguments.input_paths if arguments.input_paths is not None else DEFAULT_INPUT_PATHS,
            output_path=arguments.output if arguments.output is not None else DEFAULT_OUTPUT_PATH,
            seven_zip_exe=arguments.seven_zip_exe,
            force_auto_discovery=arguments.auto_seven_zip,
            excluded_folders=arguments.exclude_folders,
            excluded_extensions=arguments.exclude_extensions,
            dry_run=arguments.dry_run,
            show_progress=arguments.progress,
        )
    )
