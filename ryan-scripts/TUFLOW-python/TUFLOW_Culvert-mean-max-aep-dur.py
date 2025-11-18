# ryan-scripts\TUFLOW-python\TUFLOW_Culvert-mean-max-aep-dur.py

from pathlib import Path
import os

from ryan_library.scripts.tuflow.tuflow_culverts_mean import run_culvert_mean_report
from ryan_library.scripts.wrapper_utils import change_working_directory, print_library_version

# Toggle the specific culvert data types to collect. Leave empty to accept the library defaults.
INCLUDED_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx")

# Toggle the export of the raw culvert-maximums sheet (may be extremely large).
EXPORT_RAW_MAXIMUMS: bool = True


def main() -> None:
    """Wrapper script to create culvert mean peak reports."""

    print_library_version()
    console_log_level = "INFO"  # or "DEBUG"
    script_directory: Path = Path(__file__).absolute().parent

    if not change_working_directory(target_dir=script_directory):
        return

    include_data_types: tuple[str, ...] | None = INCLUDED_DATA_TYPES or None

    run_culvert_mean_report(
        script_directory=script_directory,
        log_level=console_log_level,
        include_data_types=include_data_types,
        export_raw=EXPORT_RAW_MAXIMUMS,
    )

    print()
    print_library_version()


if __name__ == "__main__":
    main()
    os.system("PAUSE")
