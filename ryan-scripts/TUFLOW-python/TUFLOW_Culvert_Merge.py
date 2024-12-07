# TUFLOW_Culvert_Merge.py

from pathlib import Path
import os
from ryan_library.scripts.tuflow_culverts_merge import tf_culv_merge
from ryan_library.functions.logging_helpers import setup_logging


def main():
    """
    Wrapper script to merge culvert results
    By default, it processes files in the directory where the script is located.
    """
    # Determine the script directory
    script_directory = Path(__file__).resolve().parent
    script_directory = Path(
        r"Q:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials"
    )
    os.chdir(script_directory)

    # Initialize logging
    setup_logging()

    # Run the peak report generation
    # You can pass a list of paths here if needed; default is the script directory
    tf_culv_merge([str(script_directory)])


if __name__ == "__main__":
    main()
