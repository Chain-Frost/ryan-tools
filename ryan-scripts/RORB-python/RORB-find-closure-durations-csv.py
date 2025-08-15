"""Wrapper for computing RORB closure durations directly from CSV files.

This script scans the provided directory for hydrograph CSV files whose names
encode the AEP, storm duration and temporal pattern (e.g.
``GoldenEagle_ aep50_du168hourtp7.csv``). The metadata is extracted from the
filenames and passed to the RORB closure-duration analysis, bypassing the
``batch.out`` parsing step.
"""

from pathlib import Path
import argparse

from ryan_library.scripts.RORB.closure_durations import run_closure_durations_from_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute RORB closure durations from CSV files")
    parser.add_argument("directory", type=Path, help="Directory containing RORB hydrograph CSV files")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_closure_durations_from_csv(paths=[args.directory], thresholds=None, log_level="INFO")


if __name__ == "__main__":
    main()
