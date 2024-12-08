# TUFLOW_Culvert_Merge.py

from pathlib import Path
import os
from ryan_library.scripts.tuflow_culverts_merge import main_processing


def main():
    """
    Wrapper script to merge culvert results
    By default, it processes files in the directory where the script is located.
    """

    try:
        # Determine the script directory
        script_directory: Path = Path(__file__).resolve().parent
        script_directory = Path(
            # r"Q:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials"
            r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials"
        )
        os.chdir(script_directory)

        # You can pass a list of paths here if needed; default is the script directory
        main_processing([script_directory], console_log_level="DEBUG")
    except Exception as e:
        print(f"Failed to change working directory: {e}")
        os.system("PAUSE")
        exit(1)


if __name__ == "__main__":
    main()
    os.system("PAUSE")
