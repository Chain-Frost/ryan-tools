#!/usr/bin/env python3
from __future__ import annotations

import csv
import logging
from collections.abc import Iterable
from pathlib import Path

# =====================================================================
# CONFIG – EDIT THESE PATHS
# =====================================================================
INPUT_PATH = Path(r"C:\Temp\fmg\anderson-chf\survey-dwg-to-xyz.txt")
OUTPUT_PATH = Path(r"C:\Temp\fmg\anderson-chf\survey-dwg-to-xyz-mod.csv")

DESCRIPTION_LINE = "DESCRIPTION=TIN Face Area"
CLOSED_LINE = "CLOSED=YES"


class ParseError(Exception):
    """Raised when the input file does not match the expected structure."""


def parse_tin_file(
    path: Path,
    logger: logging.Logger | None = None,
) -> set[tuple[float, float, float]]:
    """
    Parse a TIN text file and return a set of unique (x, y, z) points.

    Expected repeating structure:

        DESCRIPTION=TIN Face Area
        CLOSED=YES
        x1,y1,z1
        x2,y2,z2
        x3,y3,z3
        x4,y4,z4

    Blank lines are allowed between blocks and are ignored.
    Any deviation raises ParseError.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    unique_points: set[tuple[float, float, float]] = set()

    state = "EXPECT_DESCRIPTION"
    points_in_block = 0
    line_no = 0

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line_no += 1
            line = raw_line.strip()

            # Ignore blank lines anywhere
            if not line:
                continue

            if state == "EXPECT_DESCRIPTION":
                if line != DESCRIPTION_LINE:
                    raise ParseError(f"Line {line_no}: expected {DESCRIPTION_LINE!r}, got {line!r}")
                state = "EXPECT_CLOSED"
                continue

            if state == "EXPECT_CLOSED":
                if line != CLOSED_LINE:
                    raise ParseError(f"Line {line_no}: expected {CLOSED_LINE!r}, got {line!r}")
                state = "EXPECT_POINT"
                points_in_block = 0
                continue

            if state == "EXPECT_POINT":
                # Expect a coordinate line: x,y,z
                parts = [p.strip() for p in line.split(",")]
                if len(parts) != 3:
                    raise ParseError(
                        f"Line {line_no}: expected 'x,y,z' with three comma-separated " f"values, got {line!r}"
                    )

                try:
                    x, y, z = (float(parts[0]), float(parts[1]), float(parts[2]))
                except ValueError as exc:
                    raise ParseError(f"Line {line_no}: could not parse XYZ as floats: {line!r}") from exc

                unique_points.add((x, y, z))
                points_in_block += 1

                if points_in_block == 4:
                    # Completed one block, next non-blank line must be DESCRIPTION
                    state = "EXPECT_DESCRIPTION"

                continue

            raise RuntimeError(f"Unknown parser state {state!r} at line {line_no}")

    # At EOF, we must be between blocks, not in the middle of one
    if state != "EXPECT_DESCRIPTION":
        raise ParseError(
            f"Unexpected end of file: parser in state {state!r} "
            f"after reading {points_in_block} point line(s) in the last block."
        )

    logger.info("Parsed %d unique points from %s", len(unique_points), path)
    return unique_points


def write_points_to_csv(
    points: Iterable[tuple[float, float, float]],
    output_path: Path,
) -> None:
    """
    Write (x, y, z) points to a CSV with header: X,Y,Z.
    """
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["X", "Y", "Z"])
        for x, y, z in points:
            # Use high precision to avoid rounding issues
            writer.writerow([f"{x:.10f}", f"{y:.10f}", f"{z:.10f}"])


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("Parsing %s", INPUT_PATH)
    try:
        points = parse_tin_file(INPUT_PATH, logger=logger)
    except ParseError as e:
        logger.error("Parse error: %s", e)
        raise SystemExit(1)
    except Exception as e:  # noqa: BLE001
        logger.error("Unexpected error: %s", e)
        raise SystemExit(1)

    logger.info("Writing %d unique points to %s", len(points), OUTPUT_PATH)
    try:
        write_points_to_csv(points, OUTPUT_PATH)
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to write CSV: %s", e)
        raise SystemExit(1)

    logger.info("Done.")


if __name__ == "__main__":
    main()
