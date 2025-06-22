# ryan-scripts\TUFLOW-python\TUFLOW_Culvert_Maximums.py
from pathlib import Path
import os
import sys

from ryan_library.scripts.tuflow_culverts_merge import main_processing

console_log_level = "DEBUG"  # "INFO"  # or "DEBUG"


def main() -> None:
    """Wrapper to merge culvert maximums; double-clickable.
    By default, it processes files in the directory where the script is located."""
    try:
        script_dir: Path = Path(__file__).resolve().parent
        script_dir = Path(r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03")
        os.chdir(script_dir)
    except Exception as e:
        print(f"Failed to cd: {e}")
        os.system("PAUSE")
        exit(1)

    main_processing(
        paths_to_process=[script_dir],
        include_data_types=["Nmx", "Cmx", "Chan", "ccA"],
        console_log_level=console_log_level,
        output_parquet=False,
    )


if __name__ == "__main__":
    main()
    print("end of script")
    # os.system(command="PAUSE")
