"""Combine multiple folders into one ZIP archive using 7-Zip.

Each input folder is stored as a top-level folder in the archive, including
when the source folders have different parent directories or drives. Existing
archives are never overwritten, and input folders are not modified. 7-Zip is
required; the script can discover standard Windows installations automatically.

Example:
    python archive_compress_multiple_folders.py `
        -i "C:\\Data\\Run 1" "D:\\Results\\Run 2" -o "Q:\\Archives\\selected-runs.zip"

Alternatively, paste paths into DEFAULT_INPUT_PATHS and DEFAULT_OUTPUT_PATH
below, then run the script without command-line arguments.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path

SCRIPT_VERSION = "2026-10-28.2"

# ---------------------------------------------------------------------------
# Editable settings (command-line arguments override these values)
# ---------------------------------------------------------------------------
DEFAULT_INPUT_PATHS: tuple[Path, ...] = (
    Path(r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO\TUFLOW_MLGD\results\v06"),
    Path(r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO\TUFLOW_MLGD\results\v07"),
    Path(r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO\TUFLOW_MLGD\results\v12"),
)
DEFAULT_OUTPUT_PATH = Path(r"C:\Path\To\combined.zip")
# ---------------------------------------------------------------------------


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


def _publish_archive(temporary_archive: Path, output_archive: Path) -> None:
    """Publish a completed archive without overwriting an output created concurrently."""
    try:
        os.link(temporary_archive, output_archive)
    except FileExistsError:
        message = f"Output archive already exists: {output_archive}"
        raise FileExistsError(message) from None
    except OSError:
        # Some mapped/network drives do not support hard links. Exclusive creation
        # retains the no-overwrite guarantee, at the cost of one final file copy.
        try:
            with temporary_archive.open("rb") as source, output_archive.open("xb") as destination:
                shutil.copyfileobj(source, destination, length=8 * 1024 * 1024)
        except FileExistsError:
            message = f"Output archive already exists: {output_archive}"
            raise FileExistsError(message) from None
        except Exception:
            output_archive.unlink(missing_ok=True)
            raise
    temporary_archive.unlink()


def create_zip_from_folders(
    *,
    folders: Sequence[Path],
    output_archive: Path,
    executable: Path,
    excluded_folders: Sequence[str] = (),
    excluded_extensions: Sequence[str] = (),
) -> None:
    """Create one ZIP containing each supplied folder at the archive root."""
    temporary_archive = output_archive.with_name(f".{output_archive.stem}.{uuid.uuid4().hex}.partial.zip")
    exclusions = [f"-xr!{folder}" for folder in excluded_folders]
    exclusions.extend(f"-xr!*{_normalise_extension(extension)}" for extension in excluded_extensions)

    try:
        for folder in folders:
            command = [
                str(executable),
                "a",
                "-tzip",
                "-mx=5",
                "-mmt=on",
                str(temporary_archive),
                folder.name,
                *exclusions,
            ]
            result = subprocess.run(
                command,
                cwd=folder.parent,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                diagnostic = result.stderr.strip() or result.stdout.strip()
                message = f"7-Zip exit code {result.returncode} while adding {folder}: {diagnostic}"
                raise RuntimeError(message)
        _publish_archive(temporary_archive, output_archive)
    except Exception:
        temporary_archive.unlink(missing_ok=True)
        raise


def _validated_folders(input_paths: Sequence[Path]) -> list[Path]:
    """Return distinct, existing folders with unique archive root names."""
    folders: list[Path] = []
    seen_paths: set[str] = set()
    for supplied_path in input_paths:
        folder = supplied_path.absolute()
        spelling = os.path.normcase(str(folder))
        if spelling in seen_paths:
            continue
        seen_paths.add(spelling)
        if not folder.is_dir():
            message = f"Input folder does not exist or is not a directory: {folder}"
            raise ValueError(message)
        folders.append(folder)

    if not folders:
        message = "At least one input folder is required."
        raise ValueError(message)
    folded_names = [folder.name.casefold() for folder in folders]
    duplicate_names = sorted(name for name in set(folded_names) if folded_names.count(name) > 1)
    if duplicate_names:
        names = ", ".join(duplicate_names)
        message = f"Input folders must have unique top-level names; duplicate name(s): {names}"
        raise ValueError(message)
    return folders


def _validated_output(output_path: Path, folders: Sequence[Path]) -> Path:
    """Check output extension, access, uniqueness and symlink-aware containment."""
    output_archive = output_path.absolute()
    if not output_archive.suffix:
        output_archive = output_archive.with_suffix(".zip")
    elif output_archive.suffix.casefold() != ".zip":
        message = f"Output archive must use the .zip extension: {output_archive}"
        raise ValueError(message)
    if not output_archive.parent.is_dir():
        message = f"Output directory does not exist: {output_archive.parent}"
        raise ValueError(message)
    if output_archive.exists():
        message = f"Output archive already exists: {output_archive}"
        raise FileExistsError(message)
    physical_output = output_archive.resolve(strict=False)
    for folder in folders:
        # Physical containment is deliberate: junctions/symlinks matter.
        if physical_output.is_relative_to(folder.resolve(strict=True)):
            message = f"Output archive must not be inside an input folder: {folder}"
            raise ValueError(message)
    return output_archive


def _validated_paths(input_paths: Sequence[Path], output_path: Path) -> tuple[list[Path], Path]:
    folders = _validated_folders(input_paths)
    return folders, _validated_output(output_path, folders)


def main(
    *,
    input_paths: Sequence[Path],
    output_path: Path,
    seven_zip_exe: Path | None = None,
    excluded_folders: Sequence[str] = (),
    excluded_extensions: Sequence[str] = (),
    dry_run: bool = False,
) -> int:
    """Validate the request and combine the selected folders into one ZIP."""
    try:
        folders, output_archive = _validated_paths(input_paths, output_path)
        executable = seven_zip_exe.absolute() if seven_zip_exe is not None else find_7zip()
        if executable is None or not executable.is_file():
            location = executable if executable is not None else "auto-discovery"
            message = f"7-Zip executable not found ({location}). Use --seven-zip-exe."
            raise ValueError(message)

        print(f"Script: {Path(__file__).name} {SCRIPT_VERSION}")
        print(f"Python: {sys.version.split()[0]}")
        print(f"7-Zip: {_seven_zip_version(executable)} ({executable})")
        print(f"Output: {output_archive}")
        print("Folders:")
        for folder in folders:
            print(f"  - {folder}")
        if excluded_folders:
            print(f"Excluded folder names: {', '.join(excluded_folders)}")
        if excluded_extensions:
            print(f"Excluded extensions: {', '.join(map(_normalise_extension, excluded_extensions))}")
        if dry_run:
            print("Dry run complete; no archive was created.")
            return 0

        create_zip_from_folders(
            folders=folders,
            output_archive=output_archive,
            executable=executable,
            excluded_folders=excluded_folders,
            excluded_extensions=excluded_extensions,
        )
    except (FileExistsError, OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Created ZIP archive: {output_archive}")
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
    parser.add_argument(
        "--seven-zip-exe",
        type=Path,
        default=None,
        help="Explicit 7z executable path; otherwise automatically discovered.",
    )
    parser.add_argument(
        "--exclude-folder",
        action="append",
        default=[],
        help="Folder name to exclude recursively; repeat for multiple names.",
    )
    parser.add_argument(
        "--exclude-extension",
        action="append",
        default=[],
        help="File extension to exclude recursively; repeat for multiple extensions.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate and show the plan without creating a ZIP.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {SCRIPT_VERSION}")
    return parser.parse_args(argv)


if __name__ == "__main__":
    arguments = _parse_cli_arguments()
    raise SystemExit(
        main(
            input_paths=arguments.input_paths if arguments.input_paths is not None else DEFAULT_INPUT_PATHS,
            output_path=arguments.output if arguments.output is not None else DEFAULT_OUTPUT_PATH,
            seven_zip_exe=arguments.seven_zip_exe,
            excluded_folders=arguments.exclude_folder,
            excluded_extensions=arguments.exclude_extension,
            dry_run=arguments.dry_run,
        )
    )
