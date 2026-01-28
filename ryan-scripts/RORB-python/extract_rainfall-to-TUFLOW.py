import os
import re
from collections.abc import Sequence

import pandas as pd
from pandas import DataFrame

HEADER_LINE_COUNT = 3
TIME_INCREMENT_PATTERN: re.Pattern[str] = re.compile(r"Time increment \(hours\)\s*=\s*([0-9.+-Ee]+)")
SCRIPT_DIRECTORY: str = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIRECTORY: str | None = SCRIPT_DIRECTORY + r"\rainfall"  # Override to save CSVs somewhere else.


def parse_time_increment(lines: Sequence[str]) -> float:
    """
    Read the simulation time increment (in hours) from the raw .out file text.

    The RORB outputs list the increment near the top of the file using the text
    "Time increment (hours)= ...".  Capturing it once avoids supplying an
    external value that might drift from the model configuration.
    """
    for line in lines:
        match: re.Match[str] | None = TIME_INCREMENT_PATTERN.search(line)
        if match:
            return float(match.group(1))
    raise ValueError("Unable to find the time increment in the .out file.")


def collect_header_lines(lines: Sequence[str], start_row: int, line_count: int = HEADER_LINE_COUNT) -> list[str]:
    """
    Grab the header block immediately above the rainfall table.

    Parameters
    ----------
    lines:
        Complete contents of the .out file.
    start_row:
        1-based index representing the first row of rainfall data.
    line_count:
        Number of non-empty header lines to return (default 3 lines for the
        RORB output format).
    """
    header_lines: list[str] = []
    data_index: int = start_row - 1  # convert from 1-based to 0-based indexing
    idx: int = data_index - 1
    while idx >= 0 and len(header_lines) < line_count:
        candidate: str = lines[idx].rstrip("\n")
        if candidate.strip():
            header_lines.insert(0, candidate)
        idx -= 1

    if not header_lines:
        raise ValueError("Could not locate any header lines above the data block.")
    return header_lines


def _column_ranges(header_lines: Sequence[str], sample_data_line: str) -> list[tuple[int, int]]:
    """
    Determine the column start/end positions by aligning the header block with a
    sample data row.  This keeps the parser tolerant to variable spacing that
    occurs in the RORB exports.
    """
    padded_inputs: list[str] = [line.rstrip("\n") for line in header_lines]
    sample_line: str = sample_data_line.rstrip("\n")
    max_len: int = max(len(sample_line), *(len(line) for line in padded_inputs))
    padded_headers: list[str] = [line.ljust(max_len) for line in padded_inputs]
    padded_sample: str = sample_line.ljust(max_len)

    spans: list[tuple[int, int]] = [(match.start(), match.end()) for match in re.finditer(r"\S+", padded_sample)]
    ranges: list[tuple[int, int]] = []

    for idx, (start, end) in enumerate(spans):
        prev_end: int = ranges[-1][1] if ranges else 0
        next_start: int = spans[idx + 1][0] if idx + 1 < len(spans) else max_len

        col_start: int = start
        while col_start > prev_end and any(header[col_start - 1] != " " for header in padded_headers):
            col_start -= 1

        col_end: int = end
        while col_end < next_start and any(header[col_end] != " " for header in padded_headers):
            col_end += 1

        ranges.append((col_start, col_end))

    return ranges


def _normalize_column_label(label: str) -> str:
    """Tidy multi-line header fragments into human readable column labels."""
    replacements: dict[str, str] = {
        "Time Incs": "Time Increment",
        "Sub- catch ment": "Sub-catchment area",
        "Sub- catc ment": "Sub-catchment area",
        "Sub-catchment": "Sub-catchment area",
        "Inte stn. area": "Inter-station area",
        "Inter stn. area": "Inter-station area",
    }
    for source, target in replacements.items():
        if source in label:
            label = label.replace(source, target)
    # Compress internal spacing once replacements are complete.
    label = " ".join(label.split())
    return label


def build_column_names(header_lines: Sequence[str], sample_data_line: str) -> list[str]:
    """
    Build meaningful column names by combining the multi-line header text.

    Sub-area rainfall columns only show the sub-area letter in the final header
    line, so each of those columns is labelled explicitly as "Sub-area X" to
    make the CSV self-describing.
    """
    ranges: list[tuple[int, int]] = _column_ranges(header_lines, sample_data_line)
    max_len: int = max(len(sample_data_line.rstrip("\n")), *(len(line) for line in header_lines))
    padded_headers: list[str] = [line.ljust(max_len) for line in header_lines]

    column_names: list[str] = []
    for start, end in ranges:
        parts: list[str] = [line[start:end].strip() for line in padded_headers if line[start:end].strip()]
        base_token: str = padded_headers[-1][start:end].strip()

        if len(base_token) == 1 and base_token.isalpha():
            column_names.append(f"Sub-area {base_token}")
            continue

        label: str = _normalize_column_label(" ".join(parts))
        column_names.append(label or f"Column {len(column_names) + 1}")

    return column_names


def _adjust_initial_increment(df: DataFrame, rainfall_columns: Sequence[str]) -> None:
    """
    Ensure the first increment reports zero rainfall by shifting any rainfall
    depth down to the second increment.
    """
    if df.empty or len(df) < 2 or not rainfall_columns:
        return

    first_values = df.loc[df.index[0], rainfall_columns]
    if (first_values > 0).any():
        df.loc[df.index[1], rainfall_columns] = df.loc[df.index[1], rainfall_columns] + first_values
        df.loc[df.index[0], rainfall_columns] = 0.0


def _append_zero_rainfall_row(df: DataFrame, increment_column: str, time_increment: float) -> None:
    """Extend the rainfall hyetograph with a trailing zero-depth increment."""
    last_increment = float(df[increment_column].iloc[-1])
    new_increment: float = last_increment + 1.0

    new_row: dict[str, float] = {column: 0.0 for column in df.columns}
    new_row[increment_column] = new_increment
    new_row["Time (hour)"] = new_increment * time_increment

    df.loc[len(df)] = new_row


def fix_and_extract(
    file_path: str,
    start_row: int,
    end_row: int,
    header_line_count: int = HEADER_LINE_COUNT,
) -> tuple[DataFrame, int, int]:
    """
    Extract a rainfall table from a RORB .out file.

    Parameters
    ----------
    file_path:
        Path to the .out file.
    start_row / end_row:
        1-based row numbers that define the rainfall table extents.
    header_line_count:
        Number of header lines to consider directly above the table.
    Returns
    -------
    tuple (DataFrame, first_row, last_row)
        The rainfall data plus the actual first/last line numbers captured.
    """
    with open(file=file_path, mode="r", encoding="utf-8", errors="ignore") as raw_file:
        lines: list[str] = raw_file.readlines()

    time_increment: float = parse_time_increment(lines=lines)
    header_lines: list[str] = collect_header_lines(lines=lines, start_row=start_row, line_count=header_line_count)
    numbered_slice = list(enumerate(lines[start_row - 1 : end_row], start=start_row))
    data_lines: list[tuple[int, str]] = [(line_no, line) for line_no, line in numbered_slice if line.strip()]
    if not data_lines:
        raise ValueError(f"No rainfall data found between rows {start_row} and {end_row}.")

    actual_start_row = data_lines[0][0]
    actual_end_row = data_lines[-1][0]
    sample_line = data_lines[0][1]

    column_names: list[str] = build_column_names(header_lines=header_lines, sample_data_line=sample_line)
    rows: list[list[str]] = [line.split() for _, line in data_lines]
    df = pd.DataFrame(data=rows, columns=column_names)

    # Convert every column to numeric so downstream adjustments work reliably.
    for column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)

    increment_column: str = column_names[0]
    df.insert(loc=1, column="Time (hour)", value=df[increment_column] * time_increment)

    rainfall_columns: list[str] = [column for column in df.columns if column.startswith("Sub-area ")]
    _adjust_initial_increment(df=df, rainfall_columns=rainfall_columns)
    _append_zero_rainfall_row(df=df, increment_column=increment_column, time_increment=time_increment)

    return df, actual_start_row, actual_end_row


def process_out_files(
    directory: str,
    start_row: int,
    end_row: int,
    header_line_count: int = HEADER_LINE_COUNT,
    output_directory: str | None = None,
) -> None:
    """
    Iterate every .out file in *directory* and export rainfall CSVs.

    Parameters
    ----------
    directory:
        Folder containing .out files to convert.
    output_directory:
        Optional destination for CSVs. Defaults to the folder containing this script.
    """
    output_directory = output_directory or SCRIPT_DIRECTORY
    os.makedirs(output_directory, exist_ok=True)

    for file_name in os.listdir(directory):
        lower_name = file_name.lower()
        if not lower_name.endswith(".out") or lower_name.endswith("_batch.out"):
            continue

        file_path: str = os.path.join(directory, file_name)
        try:
            print(f"Processing file: {file_path}")
            extracted_df, first_row, last_row = fix_and_extract(
                file_path=file_path,
                start_row=start_row,
                end_row=end_row,
                header_line_count=header_line_count,
            )
            output_csv_path: str = os.path.join(output_directory, f"{os.path.splitext(file_name)[0]}_Rainfall.csv")
            with open(output_csv_path, "w", newline="") as csv_file:
                csv_file.write(f"Source File:,{file_path}\n")
                csv_file.write(f"Rows Extracted:,{first_row}-{last_row}\n")
                extracted_df.to_csv(path_or_buf=csv_file, index=False)
            print(f"Extracted and saved: {output_csv_path}")
        except Exception as exc:  # pragma: no cover - used for quick diagnostics
            print(f"Failed to process {file_path}: {exc}")


if __name__ == "__main__":
    directory_path = r"folder"
    start_row = 475
    end_row = 519

    process_out_files(
        directory=directory_path,
        start_row=start_row,
        end_row=end_row,
        header_line_count=HEADER_LINE_COUNT,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
    )
