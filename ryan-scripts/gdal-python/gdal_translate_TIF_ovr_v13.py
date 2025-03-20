import os
import subprocess
import concurrent.futures
from dataclasses import dataclass
from pathlib import Path
from _collections_abc import Generator
from tqdm import tqdm

# Import your environment setup functions.
from ryan_library.functions.gdal.gdal_environment import (
    setup_environment,
    check_executable,
)


def main() -> None:
    script_dir: Path = Path(__file__).resolve().parent
    script_dir = Path(
        r"P:\BGER\PER\RP20181.366 HOPE DOWNS 4 EARLY TONNES - RTIO\7 DOCUMENT CONTROL\2 RECEIVED DATA\1 CLIENT\250220 - HD Pipeline Survey\DTM Surface XYZ"
    )
    os.chdir(script_dir)
    run_app()


@dataclass
class Task:
    source_file: Path
    out_file: Path
    commands: list[str]
    gdaladdo_commands: list[str]
    process_tif: bool
    process_ovr: bool
    ovr_file: Path


def get_processing_parameters() -> tuple[list[str], list[str], list[str], str, str]:
    """Set processing parameters and determine GDAL tool paths using the prebuilt environment setup."""
    items: list[str] = ["flt", "asc", "rst", "xyz"]
    commands: list[str] = [
        "-stats",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "PREDICTOR=2",
        "-co",
        "NUM_THREADS=ALL_CPUS",
        "-co",
        "SPARSE_OK=TRUE",
        "-co",
        "BIGTIFF=IF_SAFER",
    ]
    gdaladdo_commands: list[str] = [
        "--config",
        "COMPRESS_OVERVIEW",
        "DEFLATE",
        "--config",
        "PREDICTOR_OVERVIEW",
        "2",
        "--config",
        "NUM_THREADS",
        "ALL_CPUS",
        "--config",
        "SPARSE_OK",
        "TRUE",
    ]
    # Set up the GDAL/QGIS environment.
    setup_environment()
    # Retrieve the gdal_translate path from the environment variables.
    gdal_translate_path: str = os.environ.get("GDAL_TRANSLATE_PATH", "")
    if not gdal_translate_path:
        raise FileNotFoundError("GDAL_TRANSLATE_PATH not set in environment variables.")

    # Derive the gdaladdo path from the same installation directory.
    gdaladdo_path: str = str(Path(gdal_translate_path).parent / "gdaladdo.exe")
    # Check that gdaladdo exists.
    check_executable(gdaladdo_path, "gdaladdo")

    return items, commands, gdaladdo_commands, gdal_translate_path, gdaladdo_path


def main_process() -> None:
    items, commands, gdaladdo_commands, gdal_translate_path, gdaladdo_path = (
        get_processing_parameters()
    )
    max_instances: int = os.cpu_count() or 1

    tasks, skipped_files, max_lengths = prepare_tasks(
        Path.cwd(), items, commands, gdaladdo_commands
    )

    if not tasks:
        handle_no_tasks(skipped_files)
        return

    handle_skipped_files(skipped_files)
    process_files(tasks, max_lengths, gdal_translate_path, gdaladdo_path, max_instances)
    print("Processing Complete")


def handle_no_tasks(skipped_files: list[str]) -> None:
    print("No files were processed.")
    if skipped_files:
        print("Skipped files:")
        for skipped_file in skipped_files:
            print(f"Skipped: {skipped_file}")


def handle_skipped_files(skipped_files: list[str]) -> None:
    for skipped_file in skipped_files:
        print(f"Skipped: {skipped_file}")


def process_files(
    tasks: list[Task],
    max_lengths: dict[str, int],
    gdal_translate_path: str,
    gdaladdo_path: str,
    max_instances: int,
) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_instances) as executor:
        futures = [
            executor.submit(
                run_gdal_commands_with_output,
                task,
                gdal_translate_path,
                gdaladdo_path,
                max_lengths,
            )
            for task in tasks
        ]
        for _ in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Processing Files",
        ):
            pass


def run_gdal_commands_with_output(
    task: Task,
    gdal_translate_path: str,
    gdaladdo_path: str,
    max_lengths: dict[str, int],
) -> None:
    if task.process_tif:
        print(f"Started: {task.source_file.name} -> {task.out_file.name}")
    elif task.process_ovr:
        print(f"Started: {task.out_file.name} (ovr only)")

    success: bool = True
    try:
        if task.process_tif:
            success = execute_gdal_translate(
                gdal_translate_path,
                task.commands,
                task.source_file,
                task.out_file,
                max_lengths,
            )
            if not success:
                raise Exception(f"gdal_translate failed for {task.out_file.name}")

        if success and task.process_ovr:
            success = execute_gdaladdo(
                gdaladdo_path, task.gdaladdo_commands, task.out_file, max_lengths
            )
            if not success:
                raise Exception(f"gdaladdo failed for {task.out_file.name}")

    except subprocess.CalledProcessError as e:
        handle_error(task.out_file, f"Subprocess error: {e}")
        success = False
    except Exception as e:
        handle_error(task.out_file, f"Unexpected error: {e}")
        raise

    if success:
        print(f"Finished: {task.out_file.name}")
    else:
        print(f"Error with file: {task.out_file.name}")


def execute_gdal_translate(
    gdal_translate_path: str,
    commands: list[str],
    source_file: Path,
    out_file: Path,
    max_lengths: dict[str, int],
) -> bool:
    translate_text: str = (
        f"Running gdal_translate:".ljust(len("Running gdal_translate:"))
        + f" {source_file.name}".ljust(max_lengths["max_filename_length"])
        + f" -> {out_file.name}"
    )
    print(translate_text)
    command: list[str] = (
        [gdal_translate_path] + commands + [str(source_file), str(out_file)]
    )
    try:
        result: subprocess.CompletedProcess = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        return False


def execute_gdaladdo(
    gdaladdo_path: str,
    gdaladdo_commands: list[str],
    out_file: Path,
    max_lengths: dict[str, int],
) -> bool:
    addo_text: str = f"Running gdaladdo: {out_file.name}".ljust(
        max_lengths["max_filename_length"]
    )
    print(addo_text)
    command: list[str] = [gdaladdo_path] + gdaladdo_commands + ["-ro", str(out_file)]
    try:
        result: subprocess.CompletedProcess = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        return False


def handle_error(out_file: Path, error_message: str) -> None:
    print(f"Error with file {out_file.name}: {error_message}")


def prepare_tasks(
    directory: Path,
    items: list[str],
    commands: list[str],
    gdaladdo_commands: list[str],
    output_format: str = "tif",
) -> tuple[list[Task], list[str], dict[str, int]]:
    tasks: list[Task] = []
    skipped_files: list[str] = []
    max_filename_length: int = 0
    max_relative_path_length: int = 0

    def file_generator() -> Generator[Path, None, None]:
        for root, _, files in os.walk(directory):
            root_path = Path(root)
            for file in files:
                yield root_path / file

    for file_path in file_generator():
        if any(file_path.suffix.lstrip(".").lower() == item.lower() for item in items):
            source_file: Path = file_path
            tif_file: Path = file_path.with_suffix(f".{output_format}")
            relative_path = file_path.relative_to(directory)
            max_filename_length = max(
                max_filename_length, len(file_path.name), len(tif_file.name)
            )
            max_relative_path_length = max(
                max_relative_path_length, len(str(relative_path))
            )
            process_tif: bool = False
            process_ovr: bool = False
            source_mtime: float = source_file.stat().st_mtime

            if tif_file.exists():
                tif_mtime: float = tif_file.stat().st_mtime
                process_tif = tif_mtime < source_mtime
            else:
                process_tif = True

            ovr_file: Path = tif_file.with_suffix(tif_file.suffix + ".ovr")
            if ovr_file.exists():
                ovr_mtime: float = ovr_file.stat().st_mtime
                process_ovr = process_tif or (ovr_mtime < tif_file.stat().st_mtime)
            else:
                process_ovr = True

            if process_tif or process_ovr:
                tasks.append(
                    Task(
                        source_file=source_file,
                        out_file=tif_file,
                        commands=commands,
                        gdaladdo_commands=gdaladdo_commands,
                        process_tif=process_tif,
                        process_ovr=process_ovr,
                        ovr_file=ovr_file,
                    )
                )
            else:
                skipped_files.append(tif_file.name)

    max_lengths: dict[str, int] = {
        "max_filename_length": max_filename_length,
        "max_relative_path_length": max_relative_path_length,
    }
    return tasks, skipped_files, max_lengths


def run_app() -> None:
    main_process()


if __name__ == "__main__":
    main()
