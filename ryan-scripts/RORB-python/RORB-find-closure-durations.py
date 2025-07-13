"""Example wrapper for computing RORB closure durations.

Update ``paths`` or ``thresholds`` below to customise processing.
Thresholds default to a broad range of flows when ``None``.
"""

from pathlib import Path
import os

from ryan_library.scripts.RORB.closure_durations import run_closure_durations


def main() -> None:
    script_directory = Path(__file__).resolve().parent
    os.chdir(script_directory)
    # Edit ``paths`` to point to directories with RORB batch.out files.
    # ``thresholds`` can also be provided if the default flow list is unsuitable.
    run_closure_durations(paths=[script_directory], thresholds=None, log_level="INFO")


if __name__ == "__main__":
    main()
