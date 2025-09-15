# ryan-scripts\TUFLOW-python\POMM-med-max-aep-dur.py

from pathlib import Path
import os
from ryan_library.scripts.pomm_max_items import run_median_peak_report
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)


def main() -> None:
    """Wrapper script for peak reporting."""
    print_library_version()
    console_log_level = "INFO"  # or "DEBUG"
    # Determine the script directory
    script_directory: Path = Path(__file__).absolute().parent
    # script_directory = Path(
    #     r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03"
    # )

    if not change_working_directory(target_dir=script_directory):
        return
    run_median_peak_report(script_directory=script_directory, log_level=console_log_level)


if __name__ == "__main__":
    main()
    os.system("PAUSE")
