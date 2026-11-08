# ryan_library/functions/tuflow/logsummary_excel_append.py
"""Helpers for appending TUFLOW log-summary rows to an existing Excel table."""

__lazy_modules__ = ["pandas"]


import math
import os
import stat
import tempfile
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pandas as pd
from loguru import logger
from openpyxl import load_workbook
from openpyxl.utils.cell import get_column_letter, range_boundaries
from openpyxl.workbook.workbook import Workbook
from openpyxl.worksheet.table import Table, TableColumn
from openpyxl.worksheet.worksheet import Worksheet

from ryan_library.classes.tuflow_string_classes import TuflowStringParser

ExcelCellValue = str | int | float | bool | datetime | date | time | timedelta | Decimal | None


@dataclass(slots=True, frozen=True)
class ExistingLogSummaryRows:
    """Existing workbook keys used to avoid appending duplicate log-summary rows."""

    headers: tuple[str, ...]
    runcodes: frozenset[str]
    log_paths: frozenset[str]
    log_filenames: frozenset[str]


@contextmanager
def master_workbook_claim(*, workbook_path: Path) -> Generator[None]:
    """Prevent cooperating writers from using one master workbook concurrently.

    The claim covers the entire discovery/read/processing/replace workflow.
    Creation is exclusive on the workbook's filesystem (including SMB).
    A crash can leave a claim behind: inspect ownership before manual removal.
    """
    claim_path = workbook_path.with_name(f".{workbook_path.name}.writing")
    try:
        claim_file = claim_path.open("x+b")
    except FileExistsError as exc:
        msg = f"Master workbook is already being updated: {workbook_path} (claim: {claim_path})"
        raise FileExistsError(msg) from exc

    owned_stat: os.stat_result | None = None
    try:
        owned_stat = os.fstat(claim_file.fileno())
        claim_file.write(f"pid={os.getpid()}\n".encode("ascii"))
        claim_file.flush()
        yield
    finally:
        claim_file.close()
        if owned_stat is not None:
            try:
                current = claim_path.lstat()
            except FileNotFoundError:
                pass
            else:
                if (current.st_dev, current.st_ino) == (owned_stat.st_dev, owned_stat.st_ino):
                    claim_path.unlink()


def load_existing_log_summary_rows(
    *,
    workbook_path: Path,
    sheet_name: str,
    table_name: str | None,
) -> ExistingLogSummaryRows:
    """Read existing run-code and log-path keys from a log-summary workbook table."""
    _validate_master_workbook_type(workbook_path=workbook_path)
    if not workbook_path.exists():
        msg = f"Master workbook does not exist: {workbook_path}"
        raise FileNotFoundError(msg)

    _require_unlinked_master_workbook(workbook_path=workbook_path)
    workbook: Workbook = load_workbook(filename=workbook_path, read_only=False, data_only=False)
    try:
        worksheet: Worksheet = _resolve_worksheet(workbook=workbook, sheet_name=sheet_name)
        table: Table | None = _resolve_table(worksheet=worksheet, table_name=table_name)
        min_col, header_row, max_col, max_row = _table_or_sheet_bounds(worksheet=worksheet, table=table)
        headers: tuple[str, ...] = _read_header_row(
            worksheet=worksheet,
            header_row=header_row,
            min_col=min_col,
            max_col=max_col,
        )
        runcode_index: int | None = _column_offset(headers=headers, column_name="Runcode")
        log_path_index: int | None = _column_offset(headers=headers, column_name="orig_log_path")

        runcodes: set[str] = set()
        log_paths: set[str] = set()
        log_filenames: set[str] = set()
        for row in worksheet.iter_rows(
            min_row=header_row + 1,
            max_row=max_row,
            min_col=min_col,
            max_col=max_col,
            values_only=True,
        ):
            if runcode_index is not None and runcode_index < len(row):  # noqa: SIM102 - keeps walrus scoped clearly
                if runcode_key := _normalise_text_key(row[runcode_index]):
                    runcodes.add(runcode_key)

            if log_path_index is not None and log_path_index < len(row):
                raw_log_path: object = row[log_path_index]
                if path_key := _normalise_path_key(raw_log_path):
                    log_paths.add(path_key)
                if filename_key := _normalise_path_filename_key(raw_log_path):
                    log_filenames.add(filename_key)

        logger.info(f"Loaded {len(runcodes)} existing run code(s) from {workbook_path}")
        return ExistingLogSummaryRows(
            headers=headers,
            runcodes=frozenset(runcodes),
            log_paths=frozenset(log_paths),
            log_filenames=frozenset(log_filenames),
        )
    finally:
        workbook.close()


def filter_new_log_files(
    *,
    files: Iterable[Path],
    existing_rows: ExistingLogSummaryRows,
) -> list[Path]:
    """Return only log files whose run code/path is not already in the workbook."""
    new_files: list[Path] = []
    for logfile in files:
        runcode_key: str | None = _runcode_key_from_logfile(logfile=logfile)
        path_key: str | None = _normalise_path_key(logfile)
        filename_key: str | None = _normalise_path_filename_key(logfile)

        if runcode_key and runcode_key in existing_rows.runcodes:
            logger.debug("Skipping existing run code {}", logfile)
            continue
        if path_key and path_key in existing_rows.log_paths:
            logger.debug("Skipping existing log path {}", logfile)
            continue
        if filename_key and filename_key in existing_rows.log_filenames:
            logger.debug("Skipping existing log filename {}", logfile)
            continue

        new_files.append(logfile)
    return new_files


def append_dataframe_to_workbook_table(
    *,
    data_frame: pd.DataFrame,
    workbook_path: Path,
    sheet_name: str,
    table_name: str | None,
    _claim_is_held: bool = False,
) -> None:
    """Append rows under an exclusive claim; coordinators may hold it already."""
    _validate_master_workbook_type(workbook_path=workbook_path)
    if _claim_is_held:
        _append_dataframe_to_workbook_table_claimed(
            data_frame=data_frame, workbook_path=workbook_path, sheet_name=sheet_name, table_name=table_name
        )
        return
    with master_workbook_claim(workbook_path=workbook_path):
        _append_dataframe_to_workbook_table_claimed(
            data_frame=data_frame, workbook_path=workbook_path, sheet_name=sheet_name, table_name=table_name
        )


def _append_dataframe_to_workbook_table_claimed(
    *,
    data_frame: pd.DataFrame,
    workbook_path: Path,
    sheet_name: str,
    table_name: str | None,
) -> None:
    """Write a staged workbook while the caller holds the exclusive claim."""
    _validate_master_workbook_type(workbook_path=workbook_path)
    original_stat: os.stat_result = _require_unlinked_master_workbook(workbook_path=workbook_path)
    workbook: Workbook = load_workbook(filename=workbook_path, read_only=False, data_only=False)
    try:
        worksheet: Worksheet = _resolve_worksheet(workbook=workbook, sheet_name=sheet_name)
        table: Table | None = _resolve_table(worksheet=worksheet, table_name=table_name)
        min_col, header_row, max_col, max_row = _table_or_sheet_bounds(worksheet=worksheet, table=table)
        headers: list[str] = list(
            _read_header_row(
                worksheet=worksheet,
                header_row=header_row,
                min_col=min_col,
                max_col=max_col,
            )
        )

        _add_missing_headers(
            worksheet=worksheet,
            data_frame=data_frame,
            headers=headers,
            header_row=header_row,
            min_col=min_col,
        )
        max_col = min_col + len(headers) - 1

        append_start_row: int = max(max_row + 1, header_row + 1)
        for row_offset, (_, series) in enumerate(data_frame.iterrows(), start=append_start_row):
            for column_offset, column_name in enumerate(headers, start=min_col):
                worksheet.cell(
                    row=row_offset,
                    column=column_offset,
                    value=_excel_safe_value(series.get(column_name)),
                )

        append_end_row: int = append_start_row + len(data_frame.index) - 1
        if table is not None:
            table.ref = _excel_range_ref(
                min_col=min_col,
                min_row=header_row,
                max_col=max_col,
                max_row=append_end_row,
            )
            _sync_table_columns(table=table, headers=headers)
            if table.autoFilter is not None:
                table.autoFilter.ref = table.ref

        logger.info("Saving appended workbook {}", workbook_path)
        # Write in the destination directory so publication is an atomic same-volume replace.
        # The original workbook is never opened for writing, including on writer failure.
        with tempfile.NamedTemporaryFile(
            prefix=f".{workbook_path.stem}-", suffix=".xlsx", dir=workbook_path.parent, delete=False
        ) as staged_file:
            staged_path = Path(staged_file.name)
        try:
            workbook.save(filename=staged_path)
            staged_path.chmod(stat.S_IMODE(original_stat.st_mode))
            current_stat: os.stat_result = _require_unlinked_master_workbook(workbook_path=workbook_path)
            original_signature = (
                original_stat.st_dev,
                original_stat.st_ino,
                original_stat.st_size,
                original_stat.st_mtime_ns,
            )
            current_signature = (
                current_stat.st_dev,
                current_stat.st_ino,
                current_stat.st_size,
                current_stat.st_mtime_ns,
            )
            if original_signature != current_signature:
                msg = f"Master workbook changed during append: {workbook_path}"
                raise RuntimeError(msg)
            staged_path.replace(workbook_path)
        finally:
            staged_path.unlink(missing_ok=True)
    except PermissionError:
        logger.error(f"Unable to save {workbook_path}. Close it in Excel and rerun the append script.")
        raise
    finally:
        workbook.close()


def _validate_master_workbook_type(*, workbook_path: Path) -> None:
    """Never silently strip VBA from macro-enabled workbooks."""
    if workbook_path.suffix.lower() != ".xlsx":
        msg = "Master log append supports .xlsx only; .xlsm and other formats are not modified."
        raise ValueError(msg)


def _windows_hardlink_count(*, path: Path) -> int:
    """Count Windows hardlink names (up to two), never trusting SMB st_nlink."""
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    find_first = kernel32.FindFirstFileNameW
    find_first.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPWSTR,
    ]
    find_first.restype = wintypes.HANDLE
    find_next = kernel32.FindNextFileNameW
    find_next.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD), wintypes.LPWSTR]
    find_next.restype = wintypes.BOOL
    find_close = kernel32.FindClose
    find_close.argtypes = [wintypes.HANDLE]
    find_close.restype = wintypes.BOOL

    # Windows maximum extended-length path buffer, so more-data is a failure.
    buffer = ctypes.create_unicode_buffer(32768)
    length = wintypes.DWORD(len(buffer))
    handle = find_first(str(path), 0, ctypes.byref(length), buffer)
    if handle == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), f"Cannot enumerate hardlinks for {path}")
    try:
        count = 1
        while True:
            length.value = len(buffer)
            if find_next(handle, ctypes.byref(length), buffer):
                count += 1
                if count > 1:
                    return count
                continue
            error = ctypes.get_last_error()
            if error != 38:  # ERROR_HANDLE_EOF
                raise OSError(error, f"Incomplete hardlink enumeration for {path}")
            return count
    finally:
        find_close(handle)


def _verify_windows_hardlinks(*, workbook_path: Path) -> None:
    """Fail closed if a share cannot prove it distinguishes hardlink aliases.

    Some SMB servers report st_nlink == 1 for genuine aliases. Calibrate
    the Windows API using owned one- and two-name files in the SAME directory.
    Never infer a safe workbook from an unsupported or misleading API.
    """
    with tempfile.NamedTemporaryFile(
        prefix=f".{workbook_path.stem}-link-check-", suffix=".tmp", dir=workbook_path.parent, delete=False
    ) as probe_file:
        probe_path = Path(probe_file.name)
    alias_path = probe_path.with_name(f"{probe_path.name}.alias")
    alias_owned = False
    try:
        if _windows_hardlink_count(path=probe_path) != 1:
            msg = f"Cannot verify single-link identity on the master filesystem: {workbook_path}"
            raise ValueError(msg)
        try:
            os.link(probe_path, alias_path)
            alias_owned = True
            if _windows_hardlink_count(path=probe_path) != 2 or _windows_hardlink_count(path=alias_path) != 2:
                msg = f"Hardlink enumeration is unreliable on the master filesystem: {workbook_path}"
                raise ValueError(msg)
            if _windows_hardlink_count(path=workbook_path) != 1:
                msg = f"Hard-linked master workbooks are unsupported: {workbook_path}"
                raise ValueError(msg)
        except OSError as exc:
            msg = f"Cannot verify safe hardlink replacement on this filesystem: {workbook_path}"
            raise ValueError(msg) from exc
    finally:
        if alias_owned:
            alias_path.unlink(missing_ok=True)
        probe_path.unlink(missing_ok=True)


def _require_unlinked_master_workbook(*, workbook_path: Path) -> os.stat_result:
    """Fail closed when replacing the path would sever a workbook link.

    Path.stat follows links deliberately to inspect the existing file's
    physical identity, but an explicit filename symlink is rejected first.
    Mapped drives and UNC directory parents retain their supplied spelling.
    """
    if workbook_path.is_symlink():
        msg = f"Master workbook must not be a symbolic link: {workbook_path}"
        raise ValueError(msg)
    existing = workbook_path.stat()
    if not stat.S_ISREG(existing.st_mode):
        msg = f"Master workbook must be a regular file: {workbook_path}"
        raise ValueError(msg)
    if existing.st_nlink != 1:
        msg = f"Hard-linked master workbooks are unsupported: {workbook_path} (links={existing.st_nlink})"
        raise ValueError(msg)
    if os.name == "nt":
        # SMB can report st_nlink == 1 even when other names point at the same file.
        _verify_windows_hardlinks(workbook_path=workbook_path)
    return existing


def _resolve_worksheet(*, workbook: Workbook, sheet_name: str) -> Worksheet:
    if sheet_name not in workbook.sheetnames:
        msg = f"Sheet '{sheet_name}' not found. Available sheets: {workbook.sheetnames}"
        raise ValueError(msg)
    return workbook[sheet_name]


def _resolve_table(*, worksheet: Worksheet, table_name: str | None) -> Table | None:
    table_names: list[str] = list(worksheet.tables.keys())
    if not table_names:
        logger.warning(f"Worksheet '{worksheet.title}' has no Excel table; appending to used range instead.")
        return None

    resolved_table_name: str = table_name or table_names[0]
    if resolved_table_name not in worksheet.tables:
        msg = f"Table '{resolved_table_name}' not found on sheet '{worksheet.title}'. Available tables: {table_names}"
        raise ValueError(msg)
    table = worksheet.tables[resolved_table_name]
    if not isinstance(table, Table):
        msg = f"Expected openpyxl Table for '{resolved_table_name}', got {type(table).__name__}"
        raise TypeError(msg)
    return table


def _table_or_sheet_bounds(*, worksheet: Worksheet, table: Table | None) -> tuple[int, int, int, int]:
    if table is not None:
        bounds = range_boundaries(table.ref)
        if any(bound is None for bound in bounds):
            msg = f"Invalid table range: {table.ref}"
            raise ValueError(msg)
        return cast("tuple[int, int, int, int]", bounds)
    return 1, 1, worksheet.max_column, worksheet.max_row


def _read_header_row(
    *,
    worksheet: Worksheet,
    header_row: int,
    min_col: int,
    max_col: int,
) -> tuple[str, ...]:
    headers: list[str] = []
    for cell in worksheet.iter_rows(
        min_row=header_row,
        max_row=header_row,
        min_col=min_col,
        max_col=max_col,
        values_only=True,
    ):
        headers = ["" if value is None else str(value) for value in cell]
        break
    if not headers or all(not header for header in headers):
        msg = f"No headers found on worksheet '{worksheet.title}' row {header_row}."
        raise ValueError(msg)
    return tuple(headers)


def _column_offset(*, headers: tuple[str, ...], column_name: str) -> int | None:
    lowered_column_name: str = column_name.lower()
    for index, header in enumerate(headers):
        if header.lower() == lowered_column_name:
            return index
    return None


def _add_missing_headers(
    *,
    worksheet: Worksheet,
    data_frame: pd.DataFrame,
    headers: list[str],
    header_row: int,
    min_col: int,
) -> None:
    existing_headers: set[str] = set(headers)
    for column_name in data_frame.columns:
        column_name_text: str = str(column_name)
        if column_name_text in existing_headers:
            continue
        headers.append(column_name_text)
        existing_headers.add(column_name_text)
        worksheet.cell(row=header_row, column=min_col + len(headers) - 1, value=column_name_text)
        logger.info(f"Added new master log column: {column_name_text}")


def _sync_table_columns(*, table: Table, headers: list[str]) -> None:
    while len(table.tableColumns) < len(headers):
        column_position: int = len(table.tableColumns) + 1
        table.tableColumns.append(TableColumn(id=column_position, name=headers[column_position - 1]))

    for column_position, header in enumerate(headers, start=1):
        table_column: TableColumn = table.tableColumns[column_position - 1]
        table_column.id = column_position
        table_column.name = header


def _excel_range_ref(*, min_col: int, min_row: int, max_col: int, max_row: int) -> str:
    return f"{get_column_letter(min_col)}{min_row}:{get_column_letter(max_col)}{max_row}"


def _excel_safe_value(value: object) -> ExcelCellValue:
    if value is None:
        return None

    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, Decimal) and value.is_nan():
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool, datetime, date, time, timedelta, Decimal)):
        return value

    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return _excel_safe_value(item_method())
        except (TypeError, ValueError):
            pass

    return str(value)


def _runcode_key_from_logfile(*, logfile: Path) -> str | None:
    try:
        return _normalise_text_key(TuflowStringParser(file_path=logfile).clean_run_code)
    except Exception as exc:
        logger.debug("Unable to parse run code from {}: {}", logfile, exc)
        return _normalise_text_key(logfile.stem)


def _normalise_text_key(value: object) -> str | None:
    if value is None:
        return None
    text: str = str(value).strip()
    if not text:
        return None
    lowered: str = text.lower()
    if lowered in {"nan", "none", "<na>"}:
        return None
    return lowered


def _normalise_path_key(value: object) -> str | None:
    text_key: str | None = _normalise_text_key(value)
    if text_key is None:
        return None
    return text_key.replace("/", "\\")


def _normalise_path_filename_key(value: object) -> str | None:
    path_key: str | None = _normalise_path_key(value)
    if path_key is None:
        return None
    return path_key.rsplit("\\", 1)[-1].lower()
