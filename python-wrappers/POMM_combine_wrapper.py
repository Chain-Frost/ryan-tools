# POMM_combine_wrapper.py
import os
from ryan_scripts.python_TUFLOW import POMM_combine

# this doesn't work at the moment


def run_pomm_combine():
    # This will run the main function from POMM_combine.py
    POMM_combine.main()


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)
    run_pomm_combine()
