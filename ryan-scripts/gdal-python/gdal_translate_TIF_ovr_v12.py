import os
import subprocess
import concurrent.futures
from threading import Thread
from pathlib import Path
from ryan_functions.tkinter_utils import TkinterApp, grid_location_generator
from tqdm import tqdm
from typing import Generator


def get_processing_parameters() -> tuple[list[str], list[str], list[str], str, str]:
    items = ["flt", "asc", "rst"]
    commands = [
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
    gdal_translate_path = "C:\\OSGeo4W\\bin\\gdal_translate.exe"
    gdaladdo_path = "C:\\OSGeo4W\\bin\\gdaladdo.exe"

    # Check if GDAL executables exist
    if not Path(gdal_translate_path).is_file():
        raise FileNotFoundError(f"gdal_translate not found at {gdal_translate_path}")
    if not Path(gdaladdo_path).is_file():
        raise FileNotFoundError(f"gdaladdo not found at {gdaladdo_path}")

    return items, commands, gdaladdo_commands, gdal_translate_path, gdaladdo_path


def main_process(app: TkinterApp) -> None:
    items, commands, gdaladdo_commands, gdal_translate_path, gdaladdo_path = get_processing_parameters()
    max_instances = os.cpu_count() or 1

    tasks, skipped_files, max_lengths = prepare_tasks(Path.cwd(), items, commands, gdaladdo_commands)

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
        gdal_translate_path,
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
    gdal_translate_path,
    gdaladdo_path,
    max_instances,
) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_instances) as executor:
        futures = [
            executor.submit(
                run_gdal_commands_with_output,
                task,
                gdal_translate_path,
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
    gdal_translate_path: str,
    gdaladdo_path: str,
    app: TkinterApp,
    max_lengths: dict[str, int],
    grid_gen: Generator[tuple[int, int], None, None],
) -> None:
    source_file = task["source_file"]
    out_file = task["out_file"]
    commands = task["commands"]
    gdaladdo_commands = task["gdaladdo_commands"]
    process_tif = task["process_tif"]
    process_ovr = task["process_ovr"]
    ovr_file = task["ovr_file"]

    relative_path = source_file.relative_to(Path.cwd())

    # Generate next window location and setup window
    window_key, window_title = setup_processing_window(app, out_file, relative_path, grid_gen, max_lengths)

    if process_tif:
        app.append_text("start", f"Started: {source_file.name} -> {out_file.name}\n")
    elif process_ovr:
        app.append_text("start", f"Started: {out_file.name} (ovr only)\n")
    else:
        # Should not reach here, since we only process tasks where process_tif or process_ovr is True
        return

    success = True

    try:
        if process_tif:
            success = execute_gdal_translate(
                gdal_translate_path,
                commands,
                source_file,
                out_file,
                window_key,
                app,
                max_lengths,
            )
            if not success:
                raise Exception(f"gdal_translate failed for {out_file.name}")

        if success and process_ovr:
            success = execute_gdaladdo(
                gdaladdo_path,
                gdaladdo_commands,
                out_file,
                window_key,
                app,
                max_lengths,
            )
            if not success:
                raise Exception(f"gdaladdo failed for {out_file.name}")

    except subprocess.CalledProcessError as e:
        handle_error(app, out_file, f"Subprocess error: {e}")
        success = False
    except Exception as e:
        handle_error(app, out_file, f"Unexpected error: {e}")
        raise  # Re-raise the exception after handling

    if success:
        app.remove_text_widget(window_key)
        finish_message = f"Finished: {out_file.name}"
        app.append_text("finish", finish_message + "\n")
    else:
        error_message = f"Error with file: {out_file.name}"
        app.append_text("error", error_message + "\n")


def execute_gdal_translate(
    gdal_translate_path: str,
    commands: list[str],
    source_file: Path,
    out_file: Path,
    window_key: str,
    app: TkinterApp,
    max_lengths: dict[str, int],
) -> bool:
    translate_text = (
        f"Running gdal_translate:".ljust(len("Running gdal_translate:"))
        + f" {source_file.name}".ljust(max_lengths["max_filename_length"])
        + f" -> {out_file.name}"
    )
    app.append_text(window_key, translate_text + "\n")

    command = [gdal_translate_path] + commands + [str(source_file), str(out_file)]
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


def execute_gdaladdo(
    gdaladdo_path: str,
    gdaladdo_commands: list[str],
    out_file: Path,
    window_key: str,
    app: TkinterApp,
    max_lengths: dict[str, int],
) -> bool:
    addo_text = f"Running gdaladdo: {out_file.name}".ljust(max_lengths["max_filename_length"])
    app.append_text(window_key, addo_text + "\n")

    command = [gdaladdo_path] + gdaladdo_commands + ["-ro", str(out_file)]
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


def handle_error(app, out_file: Path, error_message: str) -> None:
    error_message = f"Error with file {out_file.name}: {error_message}"
    app.append_text("error", error_message + "\n")


def setup_processing_window(app, out_file: Path, relative_path: Path, grid_gen, max_lengths) -> tuple[str, str]:
    row, column = next(grid_gen)
    window_key = f"{out_file.name}_window"
    window_title = f"Processing: {out_file.name}, {relative_path}"

    app.add_text_widget(window_key, window_title, row, column, height=5, width=60)
    return window_key, window_title


def prepare_tasks(
    directory: Path,
    items: list[str],
    commands: list[str],
    gdaladdo_commands: list[str],
    output_format: str = "tif",
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

    for file_path in file_generator():
        if any(file_path.suffix.lstrip(".").lower() == item.lower() for item in items):
            source_file = file_path
            tif_file = file_path.with_suffix(f".{output_format}")
            relative_path = file_path.relative_to(directory)

            max_filename_length = max(
                max_filename_length,
                len(file_path.name),
                len(tif_file.name),
            )
            max_relative_path_length = max(max_relative_path_length, len(str(relative_path)))

            process_tif = False
            process_ovr = False

            source_mtime = source_file.stat().st_mtime

            if tif_file.exists():
                tif_mtime = tif_file.stat().st_mtime
                if tif_mtime < source_mtime:
                    # The .tif file is older than the source file; need to reprocess
                    process_tif = True
                else:
                    # The .tif file is up to date
                    process_tif = False
            else:
                # The .tif file does not exist; need to process
                process_tif = True

            # Now, check the .ovr file
            ovr_file = tif_file.with_suffix(tif_file.suffix + ".ovr")

            if ovr_file.exists():
                ovr_mtime = ovr_file.stat().st_mtime
                if process_tif:
                    # If we are reprocessing the .tif file, we need to regenerate the .ovr file
                    process_ovr = True
                else:
                    # If the .tif file is up to date, check if the .ovr file is up to date
                    tif_mtime = tif_file.stat().st_mtime
                    if ovr_mtime < tif_mtime:
                        # The .ovr file is older than the .tif file
                        process_ovr = True
                    else:
                        # The .ovr file is up to date
                        process_ovr = False
            else:
                # The .ovr file does not exist; need to generate it
                process_ovr = True

            if process_tif or process_ovr:
                tasks.append(
                    {
                        "source_file": source_file,
                        "out_file": tif_file,
                        "commands": commands,
                        "gdaladdo_commands": gdaladdo_commands,
                        "process_tif": process_tif,
                        "process_ovr": process_ovr,
                        "ovr_file": ovr_file,
                    }
                )
            else:
                # Both .tif and .ovr files are up to date; skip
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
