# ryan-scripts\\TUFLOW-python\\TUFLOW-find-closure-durations.py
"""Wrapper for computing closure durations from TUFLOW PO files.

Update ``paths`` or ``thresholds`` below to customise processing.
``data_type`` defaults to ``"Flow"`` and ``allowed_locations`` can
restrict which locations are loaded from the CSV files.
"""

import os
from pathlib import Path

from ryan_library.scripts.tuflow.closure_durations import run_closure_durations
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
    run_closure_durations(
        paths=[script_directory],
        thresholds=None,
        data_type="Flow",
        allowed_locations=None,
        log_level=console_log_level,
    )
    print()
    print_library_version()


if __name__ == "__main__":
    main()
    os.system("PAUSE")
