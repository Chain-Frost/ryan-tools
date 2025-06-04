# TUFLOW_Culvert_Timeseries.py

from ryan_library.scripts.tuflow_culverts_timeseries import main_processing
from pathlib import Path
import os

console_log_level = "INFO"  # "DEBUG" "INFO"


def main() -> None:
    """Wrapper script to merge timeseries culvert results
    By default, it processes files in the directory where the script is located."""

    try:
        # Determine the script directory
        script_directory: Path = Path(__file__).resolve().parent
        script_directory = Path(
            # r"Q:\BGER\PER\RPRT\ryan-tools\tests\test_data\tuflow\tutorials"
            r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials"
        )
        os.chdir(script_directory)

        # You can pass a list of paths here if needed; default is the script directory
    except Exception as e:
        print(f"Failed to change working directory: {e}")
        os.system("PAUSE")
        exit(1)

    main_processing(
        paths_to_process=[script_directory],
        include_data_types=[
            "Q",
            "V",
            # "CF",
            "H",
            # "L",
            # "NF",
            # "SQ",
        ],  # Chan is already loaded inside the script for extra info
        console_log_level=console_log_level,
    )


if __name__ == "__main__":
    main()
    os.system("PAUSE")
