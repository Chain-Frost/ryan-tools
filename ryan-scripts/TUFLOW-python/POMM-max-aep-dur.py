# POMM-max-aep-dur.py

from pathlib import Path
import os
from ryan_library.scripts.pomm_max_items import run_peak_report_modern


def main() -> None:
    """Wrapper script for peak reporting.

    Mirrors :mod:`POMM_combine` so it can be called directly from QGIS or a
    command prompt.
    """

    try:
        script_directory: Path = Path(__file__).resolve().parent
        os.chdir(script_directory)
    except Exception as e:  # pragma: no cover - manual execution
        print(f"Failed to change working directory: {e}")
        os.system("PAUSE")
        raise

    print(f"Current Working Directory: {Path.cwd()}")
    run_peak_report_modern(script_directory=script_directory)


if __name__ == "__main__":
    main()
    os.system("PAUSE")
