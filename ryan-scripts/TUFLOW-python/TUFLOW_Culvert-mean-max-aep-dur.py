# ryan-scripts\TUFLOW-python\TUFLOW_Culvert-mean-max-aep-dur.py

from pathlib import Path
import os

from ryan_library.scripts.tuflow.tuflow_culverts_mean import run_culvert_mean_report
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)


def main() -> None:
    """Wrapper script to create culvert mean peak reports."""

    print_library_version()
    console_log_level = "INFO"  # or "DEBUG"
    script_directory: Path = Path(__file__).absolute().parent

    if not change_working_directory(target_dir=script_directory):
        return

    run_culvert_mean_report(
        script_directory=script_directory,
        log_level=console_log_level,
    )

    print()
    print_library_version()


if __name__ == "__main__":
    main()
    os.system("PAUSE")
