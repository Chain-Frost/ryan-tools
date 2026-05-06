from __future__ import annotations

import fnmatch
import shutil
from dataclasses import dataclass
from pathlib import Path


# copy speicfic files from below this script into an output folder.

# ---------------------------------------------------------------------------
# Hard-coded constants
# ---------------------------------------------------------------------------

OUTPUT_DIR_NAME = "TFfiles_ext_tmp"


@dataclass(frozen=True)
class CopyRun:
    number: int
    title: str
    destination_subdir: str
    list_filename: str
    patterns: tuple[str, ...]
    note: str | None = None


@dataclass(frozen=True)
class CopyRecord:
    source: Path
    destination: Path


RUNS = (
    CopyRun(
        number=1,
        title="copy TUFLOW logfile files",
        destination_subdir="Logfiles",
        list_filename="FilesList_LogFiles.txt",
        patterns=("*.tlf",),
        note="excludes *.hpc.tlf files",
    ),
    CopyRun(
        number=2,
        title="copy d_max raster files",
        destination_subdir="TIF_d",
        list_filename="FilesList_Dmax.txt",
        patterns=(
            "*d_max.tif",
            "*d_max.flt",
            "*d_max.hdr",
            "*d_max.prj",
        ),
    ),
    CopyRun(
        number=3,
        title="copy v_max raster files",
        destination_subdir="TIF_v",
        list_filename="FilesList_Vmax.txt",
        patterns=(
            "*v_max.tif",
            "*v_max.flt",
            "*v_max.hdr",
            "*v_max.prj",
        ),
    ),
    CopyRun(
        number=4,
        title="copy h_max raster files",
        destination_subdir="TIF_h",
        list_filename="FilesList_Hmax.txt",
        patterns=(
            "*h_max.tif",
            "*h_max.flt",
            "*h_max.hdr",
            "*h_max.prj",
        ),
    ),
    CopyRun(
        number=5,
        title="copy *2d_*.csv files",
        destination_subdir="CSV_2d_po",
        list_filename="FilesList_CSV_2d.txt",
        patterns=("*2d_*.csv",),
    ),
    CopyRun(
        number=6,
        title="copy *1d*.csv files",
        destination_subdir="CSV_1d",
        list_filename="FilesList_CSV_1d.txt",
        patterns=("*1d*.csv",),
        note="if no 1d files exist then this folder will be empty",
    ),
    CopyRun(
        number=7,
        title="copy check files including DEM_Z rasters and PO check GPKGs",
        destination_subdir="checkFiles",
        list_filename="checkFiles_list.txt",
        patterns=(
            "*_DEM_Z.tif",
            "*_DEM_Z.flt",
            "*_DEM_Z.hdr",
            "*_DEM_Z.prj",
            "*_po_check_L.gpkg",
            "*_po_check_P.gpkg",
        ),
        note="if no matching check files exist then this folder will be empty",
    ),
)


# ---------------------------------------------------------------------------
# Script logic
# ---------------------------------------------------------------------------

def print_banner() -> None:
    print('  "*******************************************************************"')
    print('  " Tool to extract key TUFLOW files from output folders              "')
    print('  " Darren Farmer, Sept 2024 [updated ver Feb 2026]                   "')
    print('  " Python TIF/FLT/HDR/PRJ/GPKG version                              "')
    print('  "                                                                   "')
    print('  " run from upper level of output files                              "')
    print('  " builds folders TIF_d TIF_v TIF_h CSV_2d_po CSV_1d checkFiles      "')
    print('  " also copies DEM_Z rasters, PO check GPKGs, and LogFile tlf files  "')
    print('  " excludes *.hpc.tlf files                                          "')
    print('  "*******************************************************************"')


def pause(message: str = "Press Enter to continue . . .") -> None:
    input(message)


def is_inside_directory(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False

    return True


def should_exclude_file(path: Path) -> bool:
    name = path.name.lower()

    if name.endswith(".hpc.tlf"):
        return True

    return False


def path_matches_any_pattern(path: Path, patterns: tuple[str, ...]) -> bool:
    name = path.name.lower()

    return any(fnmatch.fnmatch(name, pattern.lower()) for pattern in patterns)


def find_files(root: Path, output_dir: Path, patterns: tuple[str, ...]) -> list[Path]:
    matches: list[Path] = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        if is_inside_directory(path=path, directory=output_dir):
            continue

        if should_exclude_file(path):
            continue

        if path_matches_any_pattern(path=path, patterns=patterns):
            matches.append(path)

    return sorted(set(matches), key=lambda p: str(p).lower())


def unique_destination(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    counter = 1

    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"

        if not candidate.exists():
            return candidate

        counter += 1


def copy_files(files: list[Path], destination_dir: Path) -> list[CopyRecord]:
    records: list[CopyRecord] = []

    for source_file in files:
        destination_file = unique_destination(destination_dir / source_file.name)

        print(f"Copying: {source_file}")
        print(f"     to: {destination_file}")

        shutil.copy2(source_file, destination_file)

        records.append(
            CopyRecord(
                source=source_file,
                destination=destination_file,
            )
        )

    return records


def write_file_list(file_list_path: Path, records: list[CopyRecord]) -> None:
    with file_list_path.open("w", encoding="utf-8") as file:
        file.write("Source\tDestination\n")

        for record in records:
            file.write(
                f"{record.source.resolve()}\t"
                f"{record.destination.resolve()}\n"
            )


def run_copy_job(copy_run: CopyRun, root: Path, output_dir: Path) -> int:
    print()
    print(f" -------- RUN {copy_run.number}: {copy_run.title} ------- ")

    if copy_run.note:
        print(f"  ({copy_run.note})")

    print()

    destination_dir = output_dir / copy_run.destination_subdir
    destination_dir.mkdir(parents=True, exist_ok=True)

    files = find_files(
        root=root,
        output_dir=output_dir,
        patterns=copy_run.patterns,
    )

    records = copy_files(files=files, destination_dir=destination_dir)

    list_file = output_dir / copy_run.list_filename
    write_file_list(file_list_path=list_file, records=records)

    print(f"Found {len(records)} file(s).")

    return len(records)


def main() -> None:
    root = Path.cwd()
    output_dir = root / OUTPUT_DIR_NAME

    print_banner()

    print()
    print(f' " warning: make sure output folder <{OUTPUT_DIR_NAME}> does not exist  "')
    print(' "          if exists rename or delete before proceeding              "')
    print()

    pause()

    if output_dir.exists():
        print()
        print(f' " ISSUE: Folder <{OUTPUT_DIR_NAME}> exists, exiting..."')
        print()
        pause()
        raise SystemExit(1)

    output_dir.mkdir()

    total_files = 0

    for copy_run in RUNS:
        total_files += run_copy_job(
            copy_run=copy_run,
            root=root,
            output_dir=output_dir,
        )

    print()
    print(f' " run complete... {total_files} file(s) copied into <{OUTPUT_DIR_NAME}> "')
    print(f' " file lists are also saved in <{OUTPUT_DIR_NAME}> "')
    print()

    pause()


if __name__ == "__main__":
    main()