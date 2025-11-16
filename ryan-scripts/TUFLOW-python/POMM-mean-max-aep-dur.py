# ryan-scripts\TUFLOW-python\POMM-mean-max-aep-dur.py

from pathlib import Path
import os

from ryan_library.scripts.pomm_max_items import export_mean_peak_report
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)

# Toggle to include the combined POMM sheet in the Excel export.
INCLUDE_POMM: bool = False

# Update this tuple to restrict processing to specific PO/Location values.
# Leave empty to include every location found in the POMM files.
LOCATIONS_TO_INCLUDE: tuple[str, ...] = ()


def main() -> None:
    """Wrapper script for mean peak reporting."""

    print_library_version()
    console_log_level = "INFO"  # or "DEBUG"
    script_directory: Path = Path(__file__).absolute().parent

    locations_to_include: tuple[str, ...] | None = LOCATIONS_TO_INCLUDE or None

    if not change_working_directory(target_dir=script_directory):
        return
    export_mean_peak_report(
        script_directory=script_directory,
        log_level=console_log_level,
        include_pomm=INCLUDE_POMM,
        locations_to_include=locations_to_include,
    )
    print()
    print_library_version()


if __name__ == "__main__":
    main()
    os.system("PAUSE")
