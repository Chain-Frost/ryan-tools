"""Extract higher-median peak-flow records from RORB .out files.

Run this repository-only script in its own folder to scan *.out files
recursively beneath that folder. It writes processed_output.csv beside
the script. The existing header/filename assumptions remain unchanged;
check results against representative RORB output before engineering use.

Importing the module does not change the working directory, read files or
write reports. Only calling main() or running this file starts processing.
"""

from __future__ import annotations

import re
from functools import reduce
from pathlib import Path

import pandas as pd


def _extract_parameters_and_rows(lines: list[str]) -> tuple[dict[str, float], list[str], list[list[str]]]:
    """Retain the original RORB marker and double-space parsing rules."""
    parameters: dict[str, float] = {}
    data_rows: list[list[str]] = []
    headers: list[str] = []
    header_index: int | None = None

    for i, line in enumerate(lines):
        if "Parameters:  kc =" in line:
            values = re.findall(r"[-+]?\d*\.\d+|\d+", line)
            if len(values) >= 2:
                parameters["kc"] = float(values[0])
                parameters["m"] = float(values[1])
        elif "Initial loss (mm)" in line:
            values = re.findall(r"[-+]?\d*\.\d+|\d+", line)
            if len(values) >= 2:
                parameters["Initial_loss_mm"] = float(values[0])
                parameters["Cont_loss_mm_per_h"] = float(values[1])

        if "Run        Duration             AEP" in line:
            header_index = i
            headers = re.split(r"\s{2,}", line.strip())

        if header_index is not None and i > header_index:
            values = re.split(r"\s{2,}", line.strip())
            if len(values) == len(headers):
                data_rows.append(values)

    return parameters, headers, data_rows


def _get_higher_median_tpat(group: pd.DataFrame, peak_col: str) -> pd.Series:
    """Use the upper median's TPat, retaining the original even-size rule."""
    sorted_group = group.sort_values(by=peak_col, ascending=True).reset_index(drop=True)
    n = len(sorted_group)
    if n == 0:
        return pd.Series({"Median_Peak": None, "Median_TPat": None})
    median_index = n // 2
    try:
        median_peak = sorted_group.iloc[median_index][peak_col]
        median_tpat = sorted_group.iloc[median_index]["TPat"]
    except IndexError:
        median_peak = None
        median_tpat = None
    return pd.Series(data={"Median_Peak": median_peak, "Median_TPat": median_tpat})


def _build_median_frame(df: pd.DataFrame, peak_columns: list[str]) -> pd.DataFrame:
    """Merge peak-column medians on Duration/AEP, as in the legacy script."""
    median_data: list[pd.DataFrame] = []
    for peak_col in peak_columns:
        median_df = (
            df.groupby(["Duration", "AEP"])[[peak_col, "TPat"]]
            .apply(_get_higher_median_tpat, peak_col=peak_col)
            .reset_index()
        )
        median_data.append(
            median_df.rename(columns={"Median_Peak": f"Median_{peak_col}", "Median_TPat": f"Median_TPat_{peak_col}"})
        )
    if len(median_data) == 1:
        return median_data[0]
    return reduce(lambda left, right: left.merge(right, on=["Duration", "AEP"], how="outer"), median_data)


def _process_out_file(file_path: Path, script_dir: Path) -> pd.DataFrame | None:
    """Return one parsed RORB summary or None for a skipped file."""
    try:
        with file_path.open(encoding="latin1") as stream:
            lines = stream.readlines()
    except Exception:  # noqa: BLE001 -- legacy behaviour skips unreadable independent files
        return None

    parameters, headers, data_rows = _extract_parameters_and_rows(lines)
    if not data_rows:
        return None

    df = pd.DataFrame(data=data_rows, columns=headers)
    non_numeric_columns = ["Run", "Duration", "AEP", "TPat"]
    for col in df.columns:
        if col not in non_numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    peak_columns = [col for col in df.columns if col.startswith("Peak")]
    if not peak_columns:
        return None

    final_median_df = _build_median_frame(df, peak_columns)
    for key, value in parameters.items():
        final_median_df[key] = value

    final_median_df["Filename"] = file_path.name
    run_parts = file_path.stem.replace(" ", "_").split("_")
    for num, part in enumerate(run_parts):
        final_median_df[f"r{num}"] = str(part)

    try:
        relative_path = file_path.parent.relative_to(script_dir).as_posix()
    except ValueError:
        relative_path = "."
    final_median_df["Relative_Path"] = relative_path

    median_columns = [col for col in final_median_df.columns if col.startswith("Median_")]
    parameter_columns = list(parameters.keys())
    r_columns = [col for col in final_median_df.columns if col.startswith("r")]
    final_columns_order = [
        "Filename",
        "Relative_Path",
        "Duration",
        "AEP",
        *median_columns,
        *parameter_columns,
        *r_columns,
    ]
    return final_median_df[final_columns_order]


def main() -> None:
    """Process files beside this script; do not mutate the caller's cwd."""
    script_dir = Path(__file__).absolute().parent
    out_files = list(script_dir.rglob("*.out"))
    if not out_files:
        msg = "No .out files found in the directory and its subdirectories."
        raise FileNotFoundError(msg)

    all_median_data: list[pd.DataFrame] = []
    processed_count = 0
    skipped_count = 0
    for file_path in out_files:
        result = _process_out_file(file_path, script_dir)
        if result is None:
            skipped_count += 1
            continue
        all_median_data.append(result)
        processed_count += 1
        print(f"Processed file: {file_path}")

    if not all_median_data:
        msg = "No valid data was processed from the .out files."
        raise ValueError(msg)

    combined_median_df = pd.concat(all_median_data, ignore_index=True)
    output_path = "processed_output.csv"
    combined_median_df.to_csv(script_dir / output_path, index=False)

    print("\nProcessing Summary:")
    print(f"Total files processed: {processed_count}")
    print(f"Total files skipped: {skipped_count}")
    print(f"Results have been saved to '{output_path}'.")


if __name__ == "__main__":
    main()
