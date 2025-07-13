# ryan-scripts\TUFLOW-python\POMM-med-max-aep-dur.py

from pathlib import Path
import os
from ryan_library.scripts.pomm_max_items import run_median_peak_report


def main() -> None:
    """Wrapper script for peak reporting."""

    try:
        script_directory: Path = Path(__file__).resolve().parent
        os.chdir(script_directory)
    except Exception as e:
        print(f"Failed to change working directory: {e}")
        os.system("PAUSE")
        raise

    print(f"Current Working Directory: {Path.cwd()}")
    run_median_peak_report(script_directory=script_directory, log_level="INFO")


if __name__ == "__main__":
    main()
    os.system("PAUSE")
