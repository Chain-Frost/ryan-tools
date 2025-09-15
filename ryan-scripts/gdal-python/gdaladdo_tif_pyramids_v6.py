import os
import subprocess
import concurrent.futures
from threading import Thread
from pathlib import Path
from ryan_library.functions.tkinter_utils import TkinterApp, grid_location_generator
from tqdm import tqdm
from typing import Generator


def get_processing_parameters() -> tuple[list[str], list[str], str]:
    items = ["tif"]
    gdaladdo_commands = [
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
    gdaladdo_path = "C:\\Program Files\\QGIS 3.40.1\\bin\\gdaladdo.exe"

    # Check if GDAL executable exists
    if not Path(gdaladdo_path).is_file():
        raise FileNotFoundError(f"gdaladdo not found at {gdaladdo_path}")

    return items, gdaladdo_commands, gdaladdo_path


def main_process(app: TkinterApp) -> None:
    items, gdaladdo_commands, gdaladdo_path = get_processing_parameters()
    max_instances = os.cpu_count() or 1

    tasks, skipped_files, max_lengths = prepare_tasks(Path.cwd(), items, gdaladdo_commands)

    if not tasks:
        handle_no_tasks(skipped_files)
        return

    handle_skipped_files(app, skipped_files)

    grid_gen = grid_location_generator(start_row=2, start_column=0, max_columns=3)
    process_files(
        tasks,
        app,
        max_lengths,
        grid_gen,
        gdaladdo_path,
        max_instances,
    )

    app.set_title("Processing Complete")


def handle_no_tasks(skipped_files: list[str]) -> None:
    print("No files were processed.")
    if skipped_files:
        print("Skipped files:")
        for skipped_file in skipped_files:
            print(f"Skipped: {skipped_file}")


def handle_skipped_files(app: TkinterApp, skipped_files: list[str]) -> None:
    for skipped_file in skipped_files:
        app.append_text("skip", f"Skipped: {skipped_file}\n")
    app.update_gui()


def process_files(
    tasks,
    app,
    max_lengths,
    grid_gen,
    gdaladdo_path,
    max_instances,
) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_instances) as executor:
        futures = [
            executor.submit(
                run_gdal_commands_with_output,
                task,
                gdaladdo_path,
                app,
                max_lengths,
                grid_gen,
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
    task: dict,
    gdaladdo_path: str,
    app: TkinterApp,
    max_lengths: dict[str, int],
    grid_gen: Generator[tuple[int, int], None, None],
) -> None:
    tif_file = task["tif_file"]
    gdaladdo_commands = task["gdaladdo_commands"]

    relative_path = tif_file.relative_to(Path.cwd())

    # Generate next window location and setup window
    window_key, window_title = setup_processing_window(app, tif_file, relative_path, grid_gen, max_lengths)

    app.append_text("start", f"Started: {tif_file.name}\n")

    success = True

    try:
        success = execute_gdaladdo(
            gdaladdo_path,
            gdaladdo_commands,
            tif_file,
            window_key,
            app,
            max_lengths,
        )
        if not success:
            raise Exception(f"gdaladdo failed for {tif_file.name}")

    except subprocess.CalledProcessError as e:
        handle_error(app, tif_file, f"Subprocess error: {e}")
        success = False
    except Exception as e:
        handle_error(app, tif_file, f"Unexpected error: {e}")
        raise  # Re-raise the exception after handling

    if success:
        app.remove_text_widget(window_key)
        finish_message = f"Finished: {tif_file.name}"
        app.append_text("finish", finish_message + "\n")
    else:
        error_message = f"Error with file: {tif_file.name}"
        app.append_text("error", error_message + "\n")


def execute_gdaladdo(
    gdaladdo_path: str,
    gdaladdo_commands: list[str],
    tif_file: Path,
    window_key: str,
    app: TkinterApp,
    max_lengths: dict[str, int],
) -> bool:
    addo_text = f"Running gdaladdo: {tif_file.name}".ljust(max_lengths["max_filename_length"])
    app.append_text(window_key, addo_text + "\n")

    command = [gdaladdo_path] + gdaladdo_commands + ["-ro", str(tif_file)]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        app.append_text(window_key, result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        app.append_text(window_key, e.stderr)
        return False


def handle_error(app, tif_file: Path, error_message: str) -> None:
    error_message = f"Error with file {tif_file.name}: {error_message}"
    app.append_text("error", error_message + "\n")


def setup_processing_window(app, tif_file: Path, relative_path: Path, grid_gen, max_lengths) -> tuple[str, str]:
    row, column = next(grid_gen)
    window_key = f"{tif_file.name}_window"
    window_title = f"Processing: {tif_file.name}, {relative_path}"

    app.add_text_widget(window_key, window_title, row, column, height=5, width=60)
    return window_key, window_title


def prepare_tasks(
    directory: Path,
    items: list[str],
    gdaladdo_commands: list[str],
) -> tuple[list[dict], list[str], dict[str, int]]:
    tasks = []
    skipped_files = []

    max_filename_length = 0
    max_relative_path_length = 0

    def file_generator():
        for root, _, files in os.walk(directory):
            root_path = Path(root)
            for file in files:
                yield root_path / file

    for tif_file in (f for f in file_generator() if f.suffix.lower() == ".tif"):
        ovr_file = tif_file.with_suffix(tif_file.suffix + ".ovr")
        relative_path = tif_file.relative_to(directory)

        max_filename_length = max(
            max_filename_length,
            len(tif_file.name),
        )
        max_relative_path_length = max(max_relative_path_length, len(str(relative_path)))

        tif_mtime = tif_file.stat().st_mtime

        if ovr_file.exists():
            ovr_mtime = ovr_file.stat().st_mtime
            if ovr_mtime < tif_mtime:
                # The .ovr file is older than the .tif file; need to regenerate
                process_ovr = True
            else:
                # The .ovr file is up to date
                process_ovr = False
        else:
            # The .ovr file does not exist; need to generate it
            process_ovr = True

        if process_ovr:
            tasks.append(
                {
                    "tif_file": tif_file,
                    "gdaladdo_commands": gdaladdo_commands,
                }
            )
        else:
            # .ovr file is up to date; skip
            skipped_files.append(tif_file.name)

    max_lengths = {
        "max_filename_length": max_filename_length,
        "max_relative_path_length": max_relative_path_length,
    }

    return tasks, skipped_files, max_lengths


def run_app() -> None:
    app = TkinterApp("Summary Processing")

    # Adjusting the height and width of the summary windows and enabling the counter
    app.add_text_widget(
        key="start",
        title="Started Files",
        row=0,
        column=0,
        height=5,
        width=60,
        counter=True,
    )

    app.add_text_widget(
        key="finish",
        title="Finished Files",
        row=0,
        column=1,
        height=5,
        width=60,
        counter=True,
    )

    app.add_text_widget(
        key="skip",
        title="Skipped Files",
        row=1,
        column=0,
        height=2,
        width=60,
        counter=True,
    )

    app.add_text_widget(
        key="error",
        title="Files with Errors",
        row=1,
        column=1,
        height=2,
        width=60,
        counter=True,
    )

    # Start the main processing in a separate thread
    Thread(target=main_process, args=(app,)).start()

    app.start_gui()


if __name__ == "__main__":
    script_dir = Path(__file__).absolute().parent
    os.chdir(script_dir)
    run_app()
