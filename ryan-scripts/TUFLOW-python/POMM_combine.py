# ryan-scripts\TUFLOW-python\POMM_combine.py
import os
from pathlib import Path
from ryan_library.scripts.tuflow.pomm_combine import main_processing
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)

console_log_level = "INFO"  # or "DEBUG"


def main() -> None:
    """Wrapper script to merge POMM results.
    By default, it processes files in the script's directory."""
    print_library_version()

    # Determine the script directory
    script_directory: Path = Path(__file__).resolve().parent
    # script_directory = Path(
    #     r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03"
    # )

    if not change_working_directory(target_dir=script_directory):
        return

    main_processing(
        paths_to_process=[script_directory],
        include_data_types=["POMM"],
        console_log_level=console_log_level,
    )


if __name__ == "__main__":
    main()
    os.system("PAUSE")
