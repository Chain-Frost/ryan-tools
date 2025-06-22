# ryan-scripts\TUFLOW-python\POMM_combine.py

import os
from pathlib import Path

from ryan_library.scripts.pomm_combine import main_processing

console_log_level = "INFO"  # "DEBUG" "INFO"


def main() -> None:
    """Wrapper script to merge POMM results
    By default, it processes files in the directory where the script is located."""

    try:
        # Determine the script directory
        script_directory: Path = Path(__file__).resolve().parent
        # script_directory = Path(
        # r"Q:\BGER\PER\RPRT\ryan-tools\tests\test_data\tuflow\tutorials"
        # r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials"
        # r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03"
        # )

        os.chdir(script_directory)

        # You can pass a list of paths here if needed; default is the script directory
    except Exception as e:
        print(f"Failed to change working directory: {e}")
        os.system("PAUSE")
        exit(1)

    print(f"Current Working Directory: {Path.cwd()}")
    main_processing(
        paths_to_process=[script_directory],
        include_data_types=["POMM"],
        console_log_level=console_log_level,
    )


if __name__ == "__main__":
    main()
    os.system("PAUSE")
