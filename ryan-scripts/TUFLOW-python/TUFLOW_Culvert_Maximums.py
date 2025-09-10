# ryan-scripts\TUFLOW-python\TUFLOW_Culvert_Maximums.py

from pathlib import Path
import os

from ryan_library.scripts.tuflow.tuflow_culverts_merge import main_processing
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)


def main() -> None:
    """Wrapper to merge culvert maximums; double-clickable.
    It processes files in the script's directory by default."""
    print_library_version()
    console_log_level = "DEBUG"  # or "INFO"
    script_dir: Path = Path(__file__).resolve().parent
    # script_dir = Path(
    #     r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03"
    # )
    if not change_working_directory(target_dir=script_dir):
        return

    main_processing(
        paths_to_process=[script_dir],
        include_data_types=["Nmx", "Cmx", "Chan", "ccA"],
        console_log_level=console_log_level,
        output_parquet=False,
    )


if __name__ == "__main__":
    main()
    os.system("PAUSE")
