# ryan-scripts\RORB-python\RORB-find-closure-durations.py
"""Wrapper for computing RORB closure durations.

Update ``paths`` or ``thresholds`` below to customise processing.
Thresholds default to a broad range of flows when ``None``."""

from pathlib import Path
import os

from ryan_library.scripts.RORB.closure_durations import run_closure_durations


def main() -> None:
    script_directory: Path = Path(__file__).resolve().parent
    script_directory = Path(
        r"P:\BGER\PER\RP23067.012 GOLDEN EAGLE MINE SITE ACC WATERWAY CROSSING - HANROY\4 ENGINEERING\11 HYDROLOGY\RORB\RoC_SSP3-7.0_2030_All"
    )
    os.chdir(path=script_directory)

    # Edit ``paths`` to point to directories with RORB batch.out files.
    # ``thresholds`` can also be provided if the default flow list is unsuitable.
    # thresholds: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
    run_closure_durations(paths=[script_directory], thresholds=None, log_level="INFO")


if __name__ == "__main__":
    main()
