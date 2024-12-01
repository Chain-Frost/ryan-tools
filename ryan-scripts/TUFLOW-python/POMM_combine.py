# POMM_combine.py

from ryan_library.scripts.pomm_combine import main_processing
from pathlib import Path
import os

if __name__ == "__main__":

    os.chdir(Path(__file__).resolve().parent)
    print(f"Current Working Directory: {Path.cwd()}")
    main_processing()
