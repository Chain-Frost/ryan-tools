# ryan-scripts\TUFLOW-python\TUFLOW_Culvert_Timeseries.py
from pathlib import Path
import os

from ryan_library.scripts.tuflow.tuflow_culverts_timeseries import main_processing
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)

console_log_level = "INFO"


def main() -> None:
    """Wrapper to combine culvert timeseries; double-clickable.
    By default, it processes files in the directory where the script is located."""
    print_library_version()
    console_log_level = "INFO"
    script_dir: Path = Path(__file__).resolve().parent
    # script_dir = Path(
    #     r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03"
    # )
    if not change_working_directory(target_dir=script_dir):
        return

    main_processing(
        paths_to_process=[script_dir],
        include_data_types=["Q"],  # , "V", "H"],
        console_log_level=console_log_level,
        output_parquet=False,
    )
    # "CF",
    # "L",
    # "NF",
    # "SQ",

    # is this stil true?
    # Chan is already loaded inside the script for extra info


if __name__ == "__main__":
    main()
    os.system("PAUSE")
