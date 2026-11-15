# ryan_library/functions/excel_export.py
"""Excel export utility functions."""

from __future__ import annotations

import os
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass
from datetime import datetime
from tempfile import TemporaryDirectory

__lazy_modules__: list[str] = ["openpyxl", "pandas"]
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import BinaryIO, Literal, TypedDict

import pandas as pd
from loguru import logger
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

from ..classes.column_definitions import ColumnDefinition, ColumnMetadataRegistry
from .versioning import get_tools_version

DATA_DICTIONARY_SHEET_NAME: str = "data-dictionary"
ParquetCompression = Literal["snappy", "gzip", "brotli", "lz4", "zstd"] | None


class ExportContent(TypedDict):
    dataframes: list[pd.DataFrame]
    sheets: list[str]


@dataclass(frozen=True)
class RawTableExport:
    """An explicitly named CSV or Parquet table in the same batch as report exports.

    The filename is preserved exactly and Parquet uses the batch compression.
    CSV omits the index; Parquet preserves pandas' default index representation.
    """

    file_name: str
    dataframe: pd.DataFrame
    format: Literal["parquet", "csv"]


@dataclass(frozen=True)
class _ExportTarget:
    path: Path
    kind: Literal["excel", "parquet", "csv"]
    dataframes: list[pd.DataFrame]
    sheets: list[str]
    parquet_index: bool | None = False


def build_data_dictionary(
    *,
    sheet_frames: Mapping[str, pd.DataFrame],
    metadata_rows: Mapping[str, str] | None = None,
    registry: ColumnMetadataRegistry | None = None,
) -> pd.DataFrame:
    """Return a column data dictionary for the supplied worksheet DataFrames."""
    effective_registry: ColumnMetadataRegistry = registry or ColumnMetadataRegistry.default()
    rows: list[dict[str, str]] = []

    for key, value in (metadata_rows or {}).items():
        rows.append(
            {
                "sheet": "metadata",
                "column": key,
                "description": value,
                "value_type": "metadata",
                "pandas_dtype": "",
            }
        )

    for sheet_name, frame in sheet_frames.items():
        columns: list[str] = [str(column) for column in frame.columns]
        if not columns:
            rows.append(
                {
                    "sheet": sheet_name,
                    "column": "<no columns>",
                    "description": "Sheet exported without any columns. Review upstream processing.",
                    "value_type": "",
                    "pandas_dtype": "",
                }
            )
            continue

        dtype_map: dict[str, str] = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        definitions: list[ColumnDefinition] = effective_registry.iter_definitions(columns, sheet_name=sheet_name)
        for column_name, definition in zip(columns, definitions, strict=True):
            rows.append(
                {
                    "sheet": sheet_name,
                    "column": column_name,
                    "description": definition.description,
                    "value_type": definition.value_type or "",
                    "pandas_dtype": dtype_map.get(column_name, ""),
                }
            )

    return pd.DataFrame(
        data=rows,
        columns=["sheet", "column", "description", "value_type", "pandas_dtype"],
    )


class ExcelExporter:
    """A utility class for exporting pandas DataFrames to Excel files.

    Methods:
        export_dataframes: Export multiple DataFrames to Excel with optional column widths.
        save_to_excel: Export a single DataFrame to Excel with optional column widths.
        calculate_column_widths: Calculate optimal column widths based on data.
        set_column_widths: Apply specific column widths to a worksheet.
        auto_adjust_column_widths: Automatically adjust column widths based on data.
    """

    MAX_EXCEL_ROWS: int = 1_048_576
    MAX_EXCEL_COLUMNS: int = 16_384
    DEFAULT_NARROW_WIDTH: float = 8.43  # ~64px (Excel default width)
    DEFAULT_NARROW_COLUMNS: tuple[str, ...] = ("file", "rel_directory", "rel_path", "directory_path", "path")

    def export_dataframes(
        self,
        export_dict: dict[str, ExportContent],
        output_directory: Path | None = None,
        column_widths: dict[str, dict[str, float]] | None = None,
        auto_adjust_width: bool = True,
        file_name: str | None = None,
        *,
        export_mode: Literal["excel", "parquet", "both"] = "excel",
        parquet_compression: ParquetCompression = "gzip",
        include_data_dictionary: bool = False,
        data_dictionary_metadata: Mapping[str, str] | None = None,
        raw_exports: Sequence[RawTableExport] = (),
    ) -> None:
        """Export reports as a batch without replacing existing files.

        Standalone Excel workbooks are written directly into the output folder.
        Multi-output batches retain staged, exclusive publication. Existing
        reports are never replaced.

        Args:
            export_dict (dict[str, ExportContent]):
                A dictionary where each key is a base file name and each value contains:
                    - "dataframes": list of DataFrames
                    - "sheets": list of corresponding sheet names
            output_directory (Path | None, optional):
                The directory where the Excel files will be saved.
                Defaults to the current working directory.
            column_widths (dict[str, dict[str, float]] | None, optional):
                A dictionary where keys are sheet names and values are dictionaries
                mapping column names to their desired widths.

        Example:
                    {
                        "Sheet1": {"Name": 20, "Age": 10},
                        "Sheet2": {"Email": 30}
                    }
            auto_adjust_width (bool, optional):
                If set to True, automatically adjusts the column widths based on the
                maximum length of the data in each column. Defaults to True.
            file_name (str | None, optional):
                Explicit workbook name to use when exporting a single entry from
                ``export_dict``. When provided, the auto-generated timestamp prefix is
                skipped and ``file_name`` is written exactly (``.xlsx`` appended when
                missing).
            export_mode (Literal["excel", "parquet", "both"], optional):
                Controls which artefacts are produced:
                  * "excel" (default) writes only Excel files (with automatic Parquet/CSV
                    fallback when Excel limits are exceeded).
                  * "parquet" skips Excel entirely and writes Parquet files matching the
                    Excel naming scheme.
                  * "both" writes Excel files (subject to the standard fallback) and also
                    emits companion Parquet files.
            parquet_compression (str, optional):
                Compression codec passed to pandas whenever Parquet files are written.
                Defaults to ``"gzip"``.
            include_data_dictionary (bool, optional):
                Add a ``data-dictionary`` worksheet describing every exported Excel
                column. Parquet-only exports are unchanged. Defaults to ``False``.
            data_dictionary_metadata (Mapping[str, str] | None, optional):
                Additional metadata rows to place before the column definitions.
            raw_exports (Sequence[RawTableExport], optional):
                Explicitly named raw tables included regardless of ``export_mode``.
                Their destinations and writes participate in the same preflight,
                staging and publication batch as the formatted reports.

        Raises:
            ValueError: If the number of DataFrames doesn't match the number of sheets.
            FileExistsError: If any planned report already exists.
            OSError: If staging or no-replace publication fails. Writer exceptions propagate.

        Example:
            export_dict = {
                "Report": {
                    "dataframes": [df1, df2],
                    "sheets": ["Summary", "Details"]
                },
                "AnotherReport": {
                    "dataframes": [df3],
                    "sheets": ["Data"]
                }
            }
            ExcelExporter().export_dataframes(export_dict, output_directory=Path("exports"))
        """
        targets = self._plan_exports(
            export_dict,
            output_directory,
            file_name,
            export_mode,
            parquet_compression,
            include_data_dictionary,
            raw_exports,
        )
        # A standalone workbook needs no staging subdirectory. Creating it
        # directly also avoids newly-created SMB directories with restrictive ACLs.
        if len(targets) == 1 and targets[0].kind == "excel":
            target = targets[0]
            target.path.parent.mkdir(parents=True, exist_ok=True)
            created = False
            try:
                with target.path.open("xb") as handle:
                    created = True
                    self._write_target(
                        target,
                        target.path,
                        column_widths,
                        auto_adjust_width,
                        parquet_compression,
                        include_data_dictionary,
                        data_dictionary_metadata,
                        existing_handle=handle,
                    )
            except BaseException:
                if created:
                    try:
                        target.path.unlink()
                    except OSError:
                        logger.warning("Could not remove incomplete report: {}", target.path)
                raise
            logger.success("Exported report to {}", target.path)
            return

        published: list[Path] = []
        try:
            # Stage the complete batch before publishing: writer failures expose no partial reports.
            with ExitStack() as stack:
                staged: list[tuple[Path, Path]] = []
                for target in targets:
                    target.path.parent.mkdir(parents=True, exist_ok=True)
                    directory = stack.enter_context(TemporaryDirectory(prefix=".ryan-export-", dir=target.path.parent))
                    temporary = Path(directory) / target.path.name
                    self._write_target(
                        target,
                        temporary,
                        column_widths,
                        auto_adjust_width,
                        parquet_compression,
                        include_data_dictionary,
                        data_dictionary_metadata,
                    )
                    if not temporary.is_file():
                        msg = f"Export did not create the staged report for {target.path}"
                        raise OSError(msg)
                    staged.append((temporary, target.path))
                for temporary, destination in staged:
                    # Atomic no-replace publication also refuses a concurrently created file or symlink.
                    # Filesystems without hard-link support fail closed; there is no overwrite fallback.
                    os.link(temporary, destination)
                    published.append(destination)
        except BaseException as exc:
            if published:
                paths = ", ".join(str(path) for path in published)
                detail = f"Export incomplete; already published reports retained: {paths}"
                exc.add_note(detail)
                logger.error("{}", detail)
            raise
        for destination in published:
            logger.success("Exported report to {}", destination)

    def _plan_exports(
        self,
        export_dict: dict[str, ExportContent],
        output_directory: Path | None,
        file_name: str | None,
        export_mode: str,
        compression: ParquetCompression,
        include_data_dictionary: bool,
        raw_exports: Sequence[RawTableExport] = (),
    ) -> list[_ExportTarget]:
        mode = export_mode.lower()
        if mode not in {"excel", "parquet", "both"}:
            msg = f"Invalid export_mode '{export_mode}'. Expected excel, parquet or both."
            raise ValueError(msg)
        if file_name is not None and len(export_dict) != 1:
            msg = "'file_name' can only be provided when exporting a single workbook."
            raise ValueError(msg)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        targets: list[_ExportTarget] = []
        for raw in raw_exports:
            if raw.format not in {"parquet", "csv"}:
                msg = f"Invalid raw table format: {raw.format!r}"
                raise ValueError(msg)
            if not raw.dataframe.columns.is_unique:
                msg = f"Duplicate column names found in raw table '{raw.file_name}'."
                raise ValueError(msg)
            targets.append(
                _ExportTarget(
                    self._build_output_path(raw.file_name, output_directory),
                    raw.format,
                    [raw.dataframe],
                    [],
                    parquet_index=None,
                )
            )
        for key, content in export_dict.items():
            frames, sheets = content["dataframes"], content["sheets"]
            if len(frames) != len(sheets):
                msg = f"For file '{key}', the number of dataframes and sheets must match."
                raise ValueError(msg)
            if not frames:
                msg = f"No dataframes supplied for '{key}'."
                raise ValueError(msg)
            if len({sheet.casefold() for sheet in sheets}) != len(sheets):
                msg = f"Duplicate sheet names in '{key}'."
                raise ValueError(msg)
            for frame, sheet in zip(frames, sheets, strict=True):
                if not frame.columns.is_unique:
                    msg = f"Duplicate column names found in sheet '{sheet}'."
                    raise ValueError(msg)
            if include_data_dictionary and any(sheet.casefold() == DATA_DICTIONARY_SHEET_NAME for sheet in sheets):
                msg = f"Sheet name '{DATA_DICTIONARY_SHEET_NAME}' is reserved when include_data_dictionary=True."
                raise ValueError(msg)
            stem = self._resolve_export_stem(datetime_string=timestamp, export_key=key, file_name=file_name)
            fallback = mode != "parquet" and self._exceeds_excel_limits(frames)
            if mode != "parquet" and not fallback:
                for sheet in sheets:
                    if not sheet or len(sheet) > 31 or re.search(r"[\\*?:/\[\]]", sheet):
                        msg = f"Invalid Excel sheet name: {sheet!r}"
                        raise ValueError(msg)
                workbook_name = (
                    file_name if file_name is not None and file_name.lower().endswith(".xlsx") else f"{stem}.xlsx"
                )
                targets.append(
                    _ExportTarget(self._build_output_path(workbook_name, output_directory), "excel", frames, sheets)
                )
            if fallback:
                logger.warning("Data for '{}' exceeds Excel size limits. Exporting to Parquet and CSV instead.", stem)
            if mode in {"parquet", "both"} or fallback:
                for frame, sheet in zip(frames, sheets, strict=True):
                    base = f"{stem}_{self._sanitize_name(sheet)}"
                    name = self._build_parquet_filename(base, compression)
                    targets.append(
                        _ExportTarget(self._build_output_path(name, output_directory), "parquet", [frame], [sheet])
                    )
                    if fallback:
                        targets.append(
                            _ExportTarget(
                                self._build_output_path(f"{base}.csv", output_directory), "csv", [frame], [sheet]
                            )
                        )
        identities: set[str] = set()
        for target in targets:
            # Follow links deliberately for physical destination aliases, including linked parent directories.
            identity = str(target.path.resolve()).casefold()
            if identity in identities:
                msg = f"Export destinations collide: {target.path}"
                raise ValueError(msg)
            identities.add(identity)
            if os.path.lexists(target.path):
                msg = f"Report already exists: {target.path}"
                raise FileExistsError(msg)
            # Apply a portable case-insensitive collision policy even on case-sensitive filesystems.
            if target.path.parent.is_dir() and any(
                child.name.casefold() == target.path.name.casefold() for child in target.path.parent.iterdir()
            ):
                msg = f"Report already exists (case-insensitive match): {target.path}"
                raise FileExistsError(msg)
        return targets

    def _write_target(
        self,
        target: _ExportTarget,
        temporary: Path,
        column_widths: dict[str, dict[str, float]] | None,
        auto_adjust_width: bool,
        compression: ParquetCompression,
        include_data_dictionary: bool,
        metadata: Mapping[str, str] | None,
        *,
        existing_handle: BinaryIO | None = None,
    ) -> None:
        logger.debug("Staging {} report for {}", target.kind, target.path)
        if target.kind == "parquet":
            target.dataframes[0].to_parquet(path=temporary, index=target.parquet_index, compression=compression)
            return
        if target.kind == "csv":
            target.dataframes[0].to_csv(path_or_buf=temporary, index=False)
            return
        frames, sheets = list(target.dataframes), list(target.sheets)
        if include_data_dictionary:
            rows = {
                "Generated at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "Generator": f"ryan-tools {get_tools_version()}",
            }
            rows.update(metadata or {})
            frames.append(
                build_data_dictionary(sheet_frames=dict(zip(sheets, frames, strict=True)), metadata_rows=rows)
            )
            sheets.append(DATA_DICTIONARY_SHEET_NAME)
        with (
            nullcontext(existing_handle) if existing_handle is not None else temporary.open("xb") as handle,
            pd.ExcelWriter(path=handle, engine="openpyxl") as writer,
        ):
            # Keep a valid sheet even if the first DataFrame write fails, so close preserves the original error.
            writer.book.create_sheet(sheets[0])
            writer.book.properties.creator = f"ryan-tools {get_tools_version()}"
            for frame, sheet in zip(frames, sheets, strict=True):
                frame.to_excel(excel_writer=writer, sheet_name=sheet, merge_cells=False, index=False)  # pyright: ignore[reportUnknownMemberType]
                worksheet: Worksheet = writer.sheets[sheet]
                if auto_adjust_width:
                    self.auto_adjust_column_widths(worksheet, self.calculate_column_widths(frame))
                    self._apply_default_column_widths(worksheet, frame, sheet)
                if column_widths and sheet in column_widths:
                    self.set_column_widths(worksheet, frame, sheet, column_widths[sheet])

    def _exceeds_excel_limits(self, dataframes: list[pd.DataFrame]) -> bool:
        """Return True if any dataframe exceeds Excel's size limits."""
        for df in dataframes:
            num_data_rows: int = len(df.index)
            num_columns: int = len(df.columns)
            header_rows: int = df.columns.nlevels if num_columns > 0 else 0
            total_rows: int = num_data_rows + header_rows

            if total_rows > self.MAX_EXCEL_ROWS or num_columns > self.MAX_EXCEL_COLUMNS:
                logger.debug(
                    (
                        "Dataframe size rows={} (including {} header rows) columns={} exceeds Excel limits "
                        "(rows={}, columns={})."
                    ),
                    total_rows,
                    header_rows,
                    num_columns,
                    self.MAX_EXCEL_ROWS,
                    self.MAX_EXCEL_COLUMNS,
                )
                return True
        return False

    def _resolve_export_stem(self, *, datetime_string: str, export_key: str, file_name: str | None) -> str:
        """Return the base filename (without extension) for the current export."""
        if file_name:
            return file_name[:-5] if file_name.lower().endswith(".xlsx") else file_name
        return f"{datetime_string}_{export_key}"

    def _build_parquet_filename(self, base_filename: str, compression: ParquetCompression) -> str:
        """Return a parquet filename with an optional compression suffix."""
        if compression:
            suffix = compression.lower()
            if suffix == "gzip":
                return f"{base_filename}.parquet.gzip"
            return f"{base_filename}.parquet.{suffix}"
        return f"{base_filename}.parquet"

    def _build_output_path(self, base_filename: str, output_directory: Path | None) -> Path:
        """Create the full output path for a file name."""
        if output_directory is not None:
            return output_directory / base_filename
        return Path(base_filename)

    def _sanitize_name(self, value: str) -> str:
        """Return a filesystem-friendly version of the provided value."""
        sanitized: str = re.sub(pattern=r"[^A-Za-z0-9_-]+", repl="_", string=value).strip("_")
        return sanitized or "Sheet"

    def save_to_excel(
        self,
        data_frame: pd.DataFrame,
        file_name_prefix: str = "Export",
        sheet_name: str = "Export",
        output_directory: Path | None = None,
        column_widths: dict[str, float] | None = None,
        auto_adjust_width: bool = True,
        file_name: str | None = None,
        *,
        export_mode: Literal["excel", "parquet", "both"] = "excel",
        parquet_compression: ParquetCompression = "gzip",
        include_data_dictionary: bool = False,
        data_dictionary_metadata: Mapping[str, str] | None = None,
    ) -> None:
        """Export a single DataFrame to an Excel file with a single sheet and optional column widths.

        Args:
            data_frame (pd.DataFrame): The DataFrame to export.
            file_name_prefix (str): Prefix for the resulting Excel filename.
            sheet_name (str): Name of the sheet in the Excel file.
            output_directory (Path | None, optional):
                The directory where the Excel file will be saved.
                Defaults to the current working directory.
            column_widths (dict[str, float] | None, optional):
                A dictionary mapping column names to their desired widths.

        Example:
                    {"Name": 20, "Age": 10}
            auto_adjust_width (bool, optional):
                If set to True, automatically adjusts the column widths based on the
                maximum length of the data in each column. Defaults to True.
            file_name (str | None, optional):
                Explicit file name to use for the exported workbook. When provided the
                timestamp-based prefix is skipped and ``file_name`` is written exactly as
                supplied (``.xlsx`` is appended automatically when missing).
            export_mode (Literal["excel", "parquet", "both"], optional):
                See :meth:`export_dataframes` for details.
            parquet_compression (str, optional):
                Compression codec to use when Parquet outputs are requested. Defaults to
                ``"gzip"``.
            include_data_dictionary (bool, optional):
                Add a ``data-dictionary`` worksheet to Excel output.
            data_dictionary_metadata (Mapping[str, str] | None, optional):
                Additional metadata rows for the data dictionary.
        """
        export_dict: dict[str, ExportContent] = {file_name_prefix: {"dataframes": [data_frame], "sheets": [sheet_name]}}

        # Prepare column_widths in the required format
        prepared_column_widths: dict[str, dict[str, float]] | None = (
            {sheet_name: column_widths} if column_widths else None
        )

        self.export_dataframes(
            export_dict=export_dict,
            output_directory=output_directory,
            column_widths=prepared_column_widths,
            auto_adjust_width=auto_adjust_width,
            file_name=file_name,
            export_mode=export_mode,
            parquet_compression=parquet_compression,
            include_data_dictionary=include_data_dictionary,
            data_dictionary_metadata=data_dictionary_metadata,
        )

    def calculate_column_widths(self, df: pd.DataFrame) -> dict[str, float]:
        """Calculate optimal column widths based on the maximum length of data in each column.

        Args:
            df (pd.DataFrame): The DataFrame for which to calculate column widths.

        Returns:
            dict[str, float]: A dictionary mapping column letters to their calculated widths.
        """
        column_widths: dict[str, float] = {
            get_column_letter(idx + 1): max(
                self._calculate_max_cell_length(series=df.iloc[:, idx]),
                # Consider the normalized column label length as well.
                len(str(column_label)),
            )
            + 2  # Adding extra space
            for idx, column_label in enumerate(df.columns)
        }
        logger.debug("Calculated dynamic column widths: {}", column_widths)
        return column_widths

    def _calculate_max_cell_length(self, series: pd.Series) -> int:
        """Return the longest display length for a Series when exporting to Excel."""
        if series.empty:
            return 0

        max_length: int = 0
        for value in series.to_numpy(dtype=object, copy=False):
            value_length: int = len("" if pd.isna(value) else str(value))
            if value_length > max_length:
                max_length = value_length

        return max_length

    def set_column_widths(
        self,
        worksheet: Worksheet,
        df: pd.DataFrame,
        sheet_name: str,
        column_widths: dict[str, float],
    ) -> None:
        """Set specific column widths for a given worksheet based on provided configurations.

        Args:
            worksheet (Worksheet): The OpenPyXL worksheet object.
            df (pd.DataFrame): The pandas DataFrame containing the data.
            sheet_name (str): The name of the current sheet.
            column_widths (dict[str, float]):
                A dictionary mapping column names to their desired widths.

        Raises:
            TypeError: If column indices are not integers.
        """
        for col_name, width in column_widths.items():
            if col_name not in df.columns:
                logger.warning("Column '{}' not found in sheet '{}'. Skipping width setting.", col_name, sheet_name)
                continue

            try:
                col_idx = df.columns.get_loc(col_name)
                if not isinstance(col_idx, int):
                    msg = (
                        f"Expected integer column index for '{col_name}' in sheet '{sheet_name}', "
                        f"but got {type(col_idx).__name__}"
                    )
                    raise TypeError(msg)
                col_letter: str = get_column_letter(col_idx + 1)
                worksheet.column_dimensions[col_letter].width = width
                logger.debug(
                    "Set width for column '{}' ({}) in sheet '{}' to {}.", col_name, col_letter, sheet_name, width
                )
            except TypeError as e:
                logger.exception(
                    "TypeError when setting width for column '{}' in sheet '{}': {}", col_name, sheet_name, e
                )
            except AssertionError as e:
                logger.exception("{}", e)
            except Exception as e:
                logger.exception(
                    "Unexpected error when setting width for column '{}' in sheet '{}': {}", col_name, sheet_name, e
                )

    def auto_adjust_column_widths(self, worksheet: Worksheet, dynamic_widths: dict[str, float]) -> None:
        """Automatically adjust column widths based on calculated dynamic widths.

        Args:
            worksheet (Worksheet): The OpenPyXL worksheet object.
            dynamic_widths (dict[str, float]): Calculated dynamic column widths.
        """
        for col_letter, width in dynamic_widths.items():
            current_width: float = worksheet.column_dimensions[col_letter].width
            if width > current_width:
                worksheet.column_dimensions[col_letter].width = width
                logger.debug("Auto-adjusted width for column '{}' to {}.", col_letter, width)

    def _apply_default_column_widths(self, worksheet: Worksheet, df: pd.DataFrame, sheet_name: str) -> None:
        """Apply hard-coded default widths to known columns after auto-sizing has run."""
        default_widths: dict[str, float] = {
            column_name: self.DEFAULT_NARROW_WIDTH
            for column_name in self.DEFAULT_NARROW_COLUMNS
            if column_name in df.columns
        }
        if not default_widths:
            return

        self.set_column_widths(worksheet=worksheet, df=df, sheet_name=sheet_name, column_widths=default_widths)


# Backwards compatibility functions:
def export_dataframes(export_dict: dict[str, ExportContent], output_directory: Path | None = None) -> None:
    """Backwards-compatible function that delegates to ExcelExporter.

    Args:
        export_dict (dict[str, ExportContent]):
            Dictionary containing export information.
        output_directory (Path, optional):
            Directory to save the exported Excel files.
            Defaults to the current working directory.
    """
    ExcelExporter().export_dataframes(export_dict=export_dict, output_directory=output_directory)


def save_to_excel(
    data_frame: pd.DataFrame,
    file_name_prefix: str = "Export",
    sheet_name: str = "Export",
    output_directory: Path | None = None,
    *,
    file_name: str | None = None,
) -> None:
    """Backwards-compatible function that delegates to ExcelExporter.

    Args:
        data_frame (pd.DataFrame): The DataFrame to export.
        file_name_prefix (str): Prefix for the resulting Excel filename.
        sheet_name (str): Name of the sheet in the Excel file.
        output_directory (Path, optional):
            Directory to save the exported Excel file.
            Defaults to the current working directory.
    """
    ExcelExporter().save_to_excel(
        data_frame=data_frame,
        file_name_prefix=file_name_prefix,
        sheet_name=sheet_name,
        output_directory=output_directory,
        column_widths=None,
        auto_adjust_width=True,
        file_name=file_name,
    )
