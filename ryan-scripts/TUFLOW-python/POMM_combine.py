# ryan-scripts\TUFLOW-python\POMM_combine.py
import os
from pathlib import Path
from ryan_library.scripts.tuflow.pomm_combine import main_processing
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)

console_log_level = "INFO"  # or "DEBUG"

# Update this tuple to restrict processing to specific PO/Location values.
# Leave empty to include every location found in the POMM files.
LOCATIONS_TO_INCLUDE: tuple[str, ...] = ()


def main() -> None:
    """Wrapper script to merge POMM results.
    By default, it processes files in the script's directory."""
    print_library_version()

    # Determine the script directory
    script_directory: Path = Path(__file__).absolute().parent
    # script_directory = Path(
    #     r"E:\Library\Automation\ryan-tools\tests\test_data\tuflow\tutorials\Module_03"
    # )

    if not change_working_directory(target_dir=script_directory):
        return

    locations_to_include: tuple[str, ...] | None = LOCATIONS_TO_INCLUDE or None

    main_processing(
        paths_to_process=[script_directory],
        include_data_types=["POMM"],
        console_log_level=console_log_level,
        locations_to_include=locations_to_include,
    )
    print()
    print_library_version()


if __name__ == "__main__":
    main()
    os.system("PAUSE")
