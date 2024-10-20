import os
import subprocess
import concurrent.futures
from threading import Thread
from tkinter_utils import TkinterApp, grid_location_generator  # type: ignore
from tqdm import tqdm
from typing import Any, NoReturn
from collections.abc import Generator


def get_processing_parameters() -> tuple[list[str], list[str], list[str], str, str]:
    items: list[str] = ["flt", "asc", "rst"]
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
    gdal_translate_path = "C:\\OSGeo4W\\bin\\gdal_translate.exe"
    gdaladdo_path = "C:\\OSGeo4W\\bin\\gdaladdo.exe"

    return items, commands, gdaladdo_commands, gdal_translate_path, gdaladdo_path


def main_process(app: TkinterApp) -> None:
    items, commands, gdaladdo_commands, gdal_translate_path, gdaladdo_path = get_processing_parameters()
    MAX_instances: int = os.cpu_count() or 1

    tasks, skipped_files, max_lengths = prepare_tasks(os.getcwd(), items, commands, gdaladdo_commands, MAX_instances)

    if not tasks:
        handle_no_tasks(skipped_files)
        return

    handle_skipped_files(app, skipped_files)

    grid_gen = grid_location_generator(start_row=2, start_column=0, max_columns=3)
    process_files(tasks, app, max_lengths, grid_gen, gdal_translate_path, gdaladdo_path)

    app.set_title("Processing Complete")


def handle_no_tasks(skipped_files: list[str]) -> None:
    print("No files were processed.")
    if skipped_files:
        print("Skipped files:")
        for skipped_file in skipped_files:
            print(f"Skipped: {skipped_file}")
    input("Press Enter to exit...")


def handle_skipped_files(app: TkinterApp, skipped_files: list[str]) -> None:
    for skipped_file in skipped_files:
        app.append_text("skip", f"Skipped: {skipped_file}\n")
    app.update_gui()


def process_files(tasks, app, max_lengths, grid_gen, gdal_translate_path, gdaladdo_path) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        execute_with_progress_tracking(executor, tasks, app, max_lengths, grid_gen, gdal_translate_path, gdaladdo_path)


def execute_with_progress_tracking(
    executor, tasks, app, max_lengths, grid_gen, gdal_translate_path, gdaladdo_path
) -> None:
    list(
        tqdm(
            executor.map(
                lambda task: run_gdal_commands_with_output(
                    source_file=task[0],
                    out_file=task[1],
                    commands=task[2],
                    gdaladdo_commands=task[3],
                    gdal_translate_path=gdal_translate_path,
                    gdaladdo_path=gdaladdo_path,
                    app=app,
                    max_lengths=max_lengths,
                    grid_gen=grid_gen,
                ),
                tasks,
            ),
            total=len(tasks),
            desc="Processing Files",
        )
    )


def run_gdal_commands_with_output(
    source_file: str,
    out_file: str,
    commands: list[str],
    gdaladdo_commands: list[str],
    gdal_translate_path: str,
    gdaladdo_path: str,
    app: TkinterApp,
    max_lengths: dict[str, int],
    grid_gen: Generator[tuple[int, int], None, None],
) -> None:
    relative_path = os.path.relpath(source_file)

    # Generate next window location and setup window
    window_key, window_title = setup_processing_window(app, out_file, relative_path, grid_gen, max_lengths)

    app.append_text(
        "start",
        f"Started: {os.path.basename(source_file)} -> {os.path.basename(out_file)}\n",
    )

    success, skipped = True, False

    try:
        if os.path.exists(out_file):
            skipped = handle_skipped_file(app, out_file)
            return

        success = execute_gdal_translate(
            gdal_translate_path,
            commands,
            source_file,
            out_file,
            window_key,
            app,
            max_lengths,
        )

        if success:
            success = execute_gdaladdo(gdaladdo_path, gdaladdo_commands, out_file, window_key, app, max_lengths)

    except Exception as e:
        handle_error(app, out_file, str(e))
        success = False

    if success and not skipped:
        app.remove_text_widget(window_key)
        finalize_processing(app, out_file, skipped, success)


def execute_gdaladdo(
    gdaladdo_path: str,
    gdaladdo_commands: list[str],
    out_file: str,
    window_key: str,
    app: TkinterApp,
    max_lengths: dict[str, int],
) -> bool:
    addo_text = f"Running gdaladdo:".ljust(len("Running gdaladdo:")) + f" {os.path.basename(out_file)}".ljust(
        max_lengths["max_filename_length"]
    )
    app.append_text(window_key, addo_text + "\n")
    success = True

    with subprocess.Popen(
        [gdaladdo_path] + gdaladdo_commands + ["-ro", out_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        if proc.stdout is not None:
            for line in proc.stdout:
                app.append_text(window_key, line)
        proc.wait()
        if proc.returncode != 0:
            success = False

    return success


def setup_processing_window(app, out_file, relative_path, grid_gen, max_lengths) -> tuple[str, str]:
    row, column = next(grid_gen)
    window_key = f"{os.path.basename(out_file)}_window"
    window_title = f"Processing: {os.path.basename(out_file)}, {relative_path}"

    app.add_text_widget(window_key, window_title, row, column, height=5, width=60)
    return window_key, window_title


def handle_skipped_file(app, out_file) -> bool:
    skip_message = f"Skipped: {os.path.basename(out_file)} already exists."
    app.append_text("skip", skip_message + "\n")
    return True


def execute_gdal_translate(gdal_translate_path, commands, source_file, out_file, window_key, app, max_lengths) -> bool:
    translate_text = (
        f"Running gdal_translate:".ljust(len("Running gdal_translate:"))
        + f" {os.path.basename(source_file)}".ljust(max_lengths["max_filename_length"])
        + f" -> {os.path.basename(out_file)}"
    )
    app.append_text(window_key, translate_text + "\n")
    success = True

    with subprocess.Popen(
        [gdal_translate_path] + commands + [source_file, out_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        if proc.stdout is not None:
            for line in proc.stdout:
                app.append_text(window_key, line)
        proc.wait()
        if proc.returncode != 0:
            success = False

    return success


def handle_error(app, out_file, error_message: str) -> None:
    error_message = f"Error with file {os.path.basename(out_file)}: {error_message}"
    app.append_text("error", error_message + "\n")


def finalize_processing(app, out_file, skipped: bool, success: bool) -> None:
    if not skipped:
        if success:
            finish_message = f"Finished: {os.path.basename(out_file)}"
            app.append_text("finish", finish_message + "\n")
        else:
            error_message = f"Error with file: {os.path.basename(out_file)}"
            app.append_text("error", error_message + "\n")


def prepare_tasks(
    directory: str,
    items: list[str],
    commands: list[str],
    gdaladdo_commands: list[str],
    MAX_instances: int,
    output_format: str = "tif",
) -> tuple[list[tuple[str, str, list[str], list[str]]], list[str], dict[str, int]]:
    tasks = []
    skipped_files = []

    max_filename_length = 0
    max_relative_path_length = 0

    for root, _, files in os.walk(directory):
        for file in files:
            if any(file.endswith(item) for item in items):
                tif_file = os.path.splitext(file)[0] + f".{output_format}"
                tif_path = os.path.join(root, tif_file)
                source_file = os.path.join(root, file)
                relative_path = os.path.relpath(source_file)

                max_filename_length = max(
                    max_filename_length,
                    len(os.path.basename(source_file)),
                    len(os.path.basename(tif_file)),
                )
                max_relative_path_length = max(max_relative_path_length, len(relative_path))

                if os.path.exists(tif_path):
                    skipped_files.append(os.path.basename(tif_file))
                    continue

                tasks.append((source_file, tif_path, commands, gdaladdo_commands))

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

    # Start the summary app in the main thread
    Thread(target=main_process, args=(app,)).start()

    app.start_gui()


if __name__ == "__main__":
    script_dir: str = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)
    run_app()
