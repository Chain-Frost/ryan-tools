"""Wrapper for computing RORB closure durations directly from CSV files.

This script scans the provided directory for hydrograph CSV files whose names
encode the AEP, storm duration and temporal pattern (e.g.
``GoldenEagle_ aep50_du168hourtp7.csv``). The metadata is extracted from the
filenames and passed to the RORB closure-duration analysis, bypassing the
``batch.out`` parsing step.
"""

from pathlib import Path
import os

from ryan_library.scripts.RORB.closure_durations import run_closure_durations_from_csv


def main() -> None:
    script_directory: Path = Path(__file__).resolve().parent
    script_directory = Path(
        r"P:\BGER\PER\RP23067.012 GOLDEN EAGLE MINE SITE ACC WATERWAY CROSSING - HANROY\4 ENGINEERING\11 HYDROLOGY\RORB\RoC_SSP3-7.0_2030_All"
    )
    os.chdir(path=script_directory)

    # Edit ``paths`` to point to directories with RORB batch.out files.
    # ``thresholds`` can also be provided if the default flow list is unsuitable.
    # thresholds: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
    # Note: This script uses CSV files instead of batch.out files.
    run_closure_durations_from_csv(paths=[script_directory], thresholds=None, log_level="INFO")


if __name__ == "__main__":
    main()
