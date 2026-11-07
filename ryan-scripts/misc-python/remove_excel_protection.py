"""Create new Excel copies with worksheet/workbook protection removed.

Inputs are .xlsx/.xlsm files in the selected directories (not recursive).
Originals and existing outputs are never replaced. ZIP members are never
extracted. Unmodified members retain their bytes and ZIP metadata; edited XML
retains namespaces and content but may have different XML serialization.
Use --no-write to preview destinations. Requires the matching ryan-tools package.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-11-07.1"
DEFAULT_INPUT_DIR = Path()
# Relative output folders are based on each input directory.
DEFAULT_OUTPUT_DIR = Path("unprotected")
WRITE = True  # Creates new copies only; existing outputs are always protected.
USE_LIVE_DASHBOARD = True
LIVE_REFRESH_PER_SECOND = 2.0
LIVE_MAX_ROWS = 25
PAUSE_ON_FINISH = True
CONSOLE_LOG_LEVEL = "SUCCESS"
LOG_FILE: Path | None = Path("remove_excel_protection.log")  # Relative to the invocation directory.
FILE_LOG_LEVEL = "DEBUG"

import argparse
import copy
import os
import shutil
import stat
import tempfile
import zipfile
from xml.dom import Node, minidom
from xml.parsers import expat

from loguru import logger

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, validate_dashboard_settings
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathLike, PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner

SPREADSHEET_NAMESPACES = frozenset(
    (None, "http://schemas.openxmlformats.org/spreadsheetml/2006/main", "http://purl.oclc.org/ooxml/spreadsheetml/main")
)


def _reject_linked_path(path: Path) -> None:
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            message = f"Linked paths are not supported for workbook publication: {part}"
            raise ValueError(message)


def _validate_members(members: list[zipfile.ZipInfo]) -> None:
    seen: set[str] = set()
    for member in members:
        name = member.filename
        parts = name.rstrip("/").split("/")
        if (
            member.orig_filename != name
            or not name
            or name.startswith("/")
            or "\\" in name
            or ":" in name
            or any(part in ("", ".", "..") for part in parts)
        ):
            message = f"Unsafe ZIP member name: {name!r}"
            raise ValueError(message)
        key = name.rstrip("/").casefold()
        if key in seen:
            message = f"Duplicate ZIP member name: {name!r}"
            raise ValueError(message)
        seen.add(key)
        kind = stat.S_IFMT(member.external_attr >> 16)
        if kind not in (0, stat.S_IFREG, stat.S_IFDIR):
            message = f"Unsupported ZIP member type: {name!r}"
            raise ValueError(message)
        if member.flag_bits & 1:
            message = f"Encrypted ZIP members are not supported: {name!r}"
            raise ValueError(message)


def _remove_protection(payload: bytes, *, root_name: str, element_name: str) -> bytes:
    # Reject DTD/entity definitions before DOM parsing; no external resource reads.
    parser = expat.ParserCreate()

    def reject_doctype(*_args: object) -> None:
        message = "DTD declarations are not supported in workbook XML"
        raise ValueError(message)

    parser.StartDoctypeDeclHandler = reject_doctype
    parser.Parse(payload, True)
    document = minidom.parseString(payload)  # noqa: S318 - DTD rejected by Expat above
    try:
        root = document.documentElement
        if root is None:
            message = "Workbook XML has no document element"
            raise ValueError(message)
        if root.localName != root_name or root.namespaceURI not in SPREADSHEET_NAMESPACES:
            message = f"Unexpected workbook XML root: {root.tagName}"
            raise ValueError(message)
        changed = False
        for child in list(root.childNodes):
            if (
                child.nodeType == Node.ELEMENT_NODE
                and child.localName == element_name
                and child.namespaceURI == root.namespaceURI
            ):
                root.removeChild(child)
                child.unlink()
                changed = True
        return document.toxml(encoding="utf-8") if changed else payload
    finally:
        document.unlink()


def process_excel_file(file_path: Path, output_dir: Path) -> bool:
    """Build an owned archive and publish a new file without replacing any destination."""
    file_path = file_path.expanduser().absolute()
    output_dir = output_dir.expanduser().absolute()
    output_file = output_dir / f"{file_path.stem}_unprotected{file_path.suffix}"
    try:
        _reject_linked_path(file_path)
        _reject_linked_path(output_dir)
        if os.path.lexists(output_file):
            message = f"Output already exists: {output_file}"
            raise FileExistsError(message)
        with zipfile.ZipFile(file_path, "r") as source:
            members = source.infolist()
            _validate_members(members)
            if "xl/workbook.xml" not in source.namelist():
                message = "Workbook is missing xl/workbook.xml"
                raise ValueError(message)
            output_dir.mkdir(parents=True, exist_ok=True)
            _reject_linked_path(output_dir)
            with tempfile.TemporaryDirectory(prefix=".excel-protection-", dir=output_dir) as staging:
                staged = Path(staging) / "workbook.zip"
                with zipfile.ZipFile(staged, "w") as target:
                    target.comment = source.comment
                    for member in members:
                        metadata = copy.copy(member)
                        name = member.filename
                        is_sheet = name.startswith("xl/worksheets/") and name.count("/") == 2 and name.endswith(".xml")
                        if name == "xl/workbook.xml" or is_sheet:
                            payload = _remove_protection(
                                source.read(member),
                                root_name="worksheet" if is_sheet else "workbook",
                                element_name="sheetProtection" if is_sheet else "workbookProtection",
                            )
                            target.writestr(metadata, payload)
                        else:
                            with source.open(member) as reader, target.open(metadata, "w") as writer:
                                shutil.copyfileobj(reader, writer)
                with zipfile.ZipFile(staged) as generated:
                    if generated.testzip() is not None:
                        message = "Generated workbook failed ZIP integrity verification"
                        raise zipfile.BadZipFile(message)
                _reject_linked_path(output_dir)
                # An atomic hard link creates a complete destination only if absent.
                # Unsupported filesystems fail closed; never fall back to replace/copy.
                os.link(staged, output_file)
        logger.debug("Created workbook copy {}", output_file)
        return True
    except Exception:
        logger.exception("Failed to process Excel file {}", file_path)
        return False


def _plan_jobs(targets: list[Path], output: Path) -> list[tuple[Path, Path]]:
    if not targets or any(not target.is_dir() for target in targets):
        message = "Every input must be an existing directory; provide at least one input"
        raise ValueError(message)
    jobs: list[tuple[Path, Path]] = []
    destinations: set[Path] = set()
    sources: set[Path] = set()
    for target in dict.fromkeys(targets):
        _reject_linked_path(target)
        output_dir = output if output.is_absolute() else target / output
        _reject_linked_path(output_dir)
        for source in sorted(target.iterdir()):
            if not source.is_file() or source.suffix.lower() not in (".xlsx", ".xlsm"):
                continue
            _reject_linked_path(source)
            # Physical identity is deliberate here: aliasing must not broaden a batch.
            identity = source.resolve()
            if identity in sources:
                continue
            sources.add(identity)
            destination = output_dir / f"{source.stem}_unprotected{source.suffix}"
            # Follow links for collision/security identity, never merely for display.
            destination_identity = destination.resolve()
            if destination_identity in destinations or os.path.lexists(destination):
                message = f"Workbook output collision: {destination}"
                raise FileExistsError(message)
            destinations.add(destination_identity)
            jobs.append((source, output_dir))
    if sources & destinations:
        message = "A workbook output aliases a selected source"
        raise ValueError(message)
    return jobs


def _log_path_for_jobs(jobs: list[tuple[Path, Path]], *, write: bool) -> Path | None:
    """Choose the write-run log after checking workbook and link collisions."""
    log_path = None if not write or LOG_FILE is None else Path(LOG_FILE).expanduser().absolute()
    if log_path is not None:
        _reject_linked_path(log_path)
        # Follow links to prevent log output from aliasing workbook inputs/outputs.
        log_identity = log_path.resolve()
        for source, destination in jobs:
            planned_output = destination / f"{source.stem}_unprotected{source.suffix}"
            if log_identity in (source.resolve(), planned_output.resolve()):
                message = f"Log file aliases a workbook input or output: {log_path}"
                raise ValueError(message)
    return log_path


def main(
    *,
    input_directories: PathOrList | None = None,
    output_directory: PathLike | None = None,
    write: bool | None = None,
    use_live_dashboard: bool | None = None,
) -> int:
    """Preflight the entire batch, then create copies or print a mutation-free preview."""
    try:
        targets = [
            path.expanduser().absolute()
            for path in to_path_list(DEFAULT_INPUT_DIR if input_directories is None else input_directories)
        ]
        output = Path(DEFAULT_OUTPUT_DIR if output_directory is None else output_directory).expanduser()
        effective_write = WRITE if write is None else write
        effective_dashboard = USE_LIVE_DASHBOARD if use_live_dashboard is None else use_live_dashboard
        if type(effective_write) is not bool or type(effective_dashboard) is not bool:
            message = "WRITE and USE_LIVE_DASHBOARD must be True or False"
            raise ValueError(message)
        validate_dashboard_settings(refresh_per_second=LIVE_REFRESH_PER_SECOND, max_rows=LIVE_MAX_ROWS)
        jobs = _plan_jobs(targets, output)
        log_path = _log_path_for_jobs(jobs, write=effective_write)
    except (OSError, ValueError, TypeError):
        logger.exception("Invalid workbook batch configuration")
        return 1
    with setup_logger(
        console_log_level=CONSOLE_LOG_LEVEL,
        log_file=str(log_path) if log_path is not None else None,
        file_log_level=FILE_LOG_LEVEL,
    ):
        if not effective_write:
            for source, destination in jobs:
                logger.success("Preview: {} -> {}", source, destination / f"{source.stem}_unprotected{source.suffix}")
            return 0
        with LiveWorkflowDashboard(
            title="Excel protection copies",
            enabled=effective_dashboard,
            refresh_per_second=LIVE_REFRESH_PER_SECOND,
            max_rows=LIVE_MAX_ROWS,
        ) as dashboard:
            dashboard.set_tasks([str(source) for source, _ in jobs])
            dashboard.set_phase("Creating workbook copies")
            failures = 0
            try:
                for index, (source, destination) in enumerate(jobs, start=1):
                    dashboard.mark_running(index=index)
                    succeeded = process_excel_file(source, destination)
                    failures += not succeeded
                    dashboard.mark_finished(index=index, status="OK" if succeeded else "FAIL")
                outcome = "FAILED" if failures else "SUCCESS" if jobs else "SKIPPED"
                dashboard.finish(outcome=outcome, detail=f"{len(jobs) - failures} created, {failures} failed")
                if not effective_dashboard:
                    logger.log(
                        "ERROR" if failures else "SUCCESS",
                        "Workbook copies: {} created, {} failed",
                        len(jobs) - failures,
                        failures,
                    )
                return 1 if failures else 0
            except KeyboardInterrupt:
                dashboard.finish(outcome="INTERRUPTED", detail="Copying interrupted")
                return 130


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-i", "--input_directories", type=Path, nargs="+", help="Input directories (not recursive).")
    parser.add_argument(
        "--output-directory", type=Path, help="Absolute shared folder or relative folder per input root."
    )
    parser.add_argument(
        "--write",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Create copies, or preview with --no-write.",
    )
    parser.add_argument("--live-dashboard", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--no-pause", action="store_true")
    parser.add_argument("--version", action="version", version=WRAPPER_VERSION)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(
        wrapper_source="misc-python/remove_excel_protection.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
    )
    result = main(
        input_directories=args.input_directories,
        output_directory=args.output_directory,
        write=args.write,
        use_live_dashboard=args.live_dashboard,
    )
    print_wrapper_banner(
        wrapper_source="misc-python/remove_excel_protection.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if PAUSE_ON_FINISH and not args.no_pause:
        pause_console()
    raise SystemExit(result)
