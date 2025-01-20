# LogSummary.py

from ryan_library.scripts.tuflow_logsummary import main_processing
from pathlib import Path
import os

if __name__ == "__main__":
    # Change working directory to the script's directory
    try:
        script_dir: Path = Path(__file__).resolve().parent
        script_dir = Path(r"E:\Library\Automation\ryan-tools\tests\test_data")
        # script_dir = Path(
        #     r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_11"
        # )
        os.chdir(script_dir)
        print(f"Current Working Directory: {Path.cwd()}")
    except Exception as e:
        print(f"Failed to change working directory: {e}")
        os.system("PAUSE")
        exit(1)

    # Execute the main processing
    main_processing(console_log_level="DEBUG")
    os.system("PAUSE")
