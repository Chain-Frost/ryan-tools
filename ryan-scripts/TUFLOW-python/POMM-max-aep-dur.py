# POMM-max-aep-dur.py

from pathlib import Path
import os
from ryan_library.scripts.pomm_max_items import run_peak_report


def main():
    """
    Wrapper script to generate peak reports based on 'AEP' and 'Duration'.
    """

    # Determine the script directory and change the working directory to it
    script_directory = Path(__file__).resolve().parent
    os.chdir(script_directory)

    # Run the peak report generation
    run_peak_report()


if __name__ == "__main__":
    main()
