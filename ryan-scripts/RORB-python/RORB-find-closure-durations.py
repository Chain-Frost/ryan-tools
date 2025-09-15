# ryan-scripts\RORB-python\RORB-find-closure-durations.py
"""Wrapper for computing RORB closure durations.

Update ``paths`` or ``thresholds`` below to customise processing.
Thresholds default to a broad range of flows when ``None``."""
import os
from pathlib import Path

from ryan_library.scripts.RORB.closure_durations import run_closure_durations
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)

console_log_level = "INFO"


def main() -> None:
    print_library_version()
    script_directory: Path = Path(__file__).absolute().parent
    # script_directory = Path(r"...")
    if not change_working_directory(target_dir=script_directory):
        return

    # Edit ``paths`` to point to directories with RORB batch.out files.
    # ``thresholds`` can also be provided if the default flow list is unsuitable.
    # thresholds: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
    run_closure_durations(paths=[script_directory], thresholds=None, log_level=console_log_level)


if __name__ == "__main__":
    main()
    os.system("PAUSE")
