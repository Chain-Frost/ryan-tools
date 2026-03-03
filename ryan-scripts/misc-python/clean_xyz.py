from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove rows containing the specified value from every .xyz file in this folder."
    )
    parser.add_argument(
        "--value",
        type=float,
        default=-999.0,
        help="Numeric value to treat as invalid. Rows containing this value are dropped. Default: -999.",
    )
    return parser.parse_args()


def line_has_value(line: str, target: float) -> bool:
    # Split ignores repeated whitespace while streaming line-by-line.
    for token in line.split():
        try:
            if float(token) == target:
                return True
        except ValueError:
            continue
    return False


def clean_file(path: Path, target: float) -> int:
    removed = 0
    tmp_path: Path = path.with_suffix(suffix=path.suffix + ".tmp")

    with path.open(mode="r", encoding="utf-8") as src, tmp_path.open(mode="w", encoding="utf-8") as dst:
        for line in src:
            if line_has_value(line=line, target=target):
                removed += 1
                continue
            dst.write(line)

    if removed:
        tmp_path.replace(target=path)
    else:
        tmp_path.unlink(missing_ok=True)

    return removed


def main() -> None:
    args: argparse.Namespace = parse_args()
    root: Path = Path(__file__).resolve().parent
    xyz_files: list[Path] = sorted(root.glob("*.xyz"))
    if not xyz_files:
        print("No .xyz files found next to this script.")
        return

    total_removed = 0
    for xyz_path in xyz_files:
        removed: int = clean_file(path=xyz_path, target=args.value)
        total_removed += removed
        print(f"{xyz_path.name}: removed {removed} rows")

    print(f"Done. Removed {total_removed} rows across {len(xyz_files)} files.")


if __name__ == "__main__":
    main()
