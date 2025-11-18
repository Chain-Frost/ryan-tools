# ryan-scripts\TUFLOW-python\LogSummary.py
from pathlib import Path
import os
from ryan_library.scripts.tuflow.tuflow_logsummary import main_processing
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)


def main() -> None:
    """Wrapper script to analyse and summarise TUFLOW log files.
    By default, it processes files in the script's directory recursively."""
    print_library_version()
    console_log_level = "INFO"  # or "DEBUG"
    # Determine the script directory
    script_directory: Path = Path(__file__).absolute().parent
    # script_directory = Path(r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03")

    if not change_working_directory(target_dir=script_directory):
        return

    # Execute the main processing
    main_processing(console_log_level=console_log_level)
    print()
    print_library_version()


if __name__ == "__main__":
    main()
    os.system("PAUSE")
