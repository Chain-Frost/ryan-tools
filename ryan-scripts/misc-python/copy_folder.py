from __future__ import annotations

import ctypes
import subprocess
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

# Example settings:
# share = r"\\server-or-ip\share-name"
# source: Path = Path(share) / "folder_to_copy"
# destination_root = Path(r"C:\destination\root")
# destination: Path = destination_root / source.name
# username = r"domain-or-machine\username"
# password: str = "password"
share = r"\\p\p"
source: Path = Path(share) / ""
destination_root = Path(r"C:\Temp")
destination: Path = destination_root / source.name
username = r"-"
password: str = ""

RESOURCETYPE_DISK = 0x00000001
NO_ERROR = 0
ERROR_SESSION_CREDENTIAL_CONFLICT = 1219
PROGRESS_CONTINUE = 0
COPY_FILE_DEFAULT_FLAGS = 0
COPY_ENGINE = "robocopy"
ROBOCOPY_THREADS = 16

console = Console()


@dataclass(frozen=True)
class FileCopyItem:
    source: Path
    destination: Path
    size: int


class NETRESOURCE(ctypes.Structure):
    _fields_ = [
        ("dwScope", wintypes.DWORD),
        ("dwType", wintypes.DWORD),
        ("dwDisplayType", wintypes.DWORD),
        ("dwUsage", wintypes.DWORD),
        ("lpLocalName", wintypes.LPWSTR),
        ("lpRemoteName", wintypes.LPWSTR),
        ("lpComment", wintypes.LPWSTR),
        ("lpProvider", wintypes.LPWSTR),
    ]


mpr = ctypes.WinDLL("mpr")
mpr.WNetAddConnection2W.argtypes = [
    ctypes.POINTER(NETRESOURCE),
    wintypes.LPCWSTR,
    wintypes.LPCWSTR,
    wintypes.DWORD,
]
mpr.WNetAddConnection2W.restype = wintypes.DWORD
mpr.WNetCancelConnection2W.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.BOOL]
mpr.WNetCancelConnection2W.restype = wintypes.DWORD

COPY_PROGRESS_ROUTINE = ctypes.WINFUNCTYPE(
    wintypes.DWORD,
    ctypes.c_longlong,
    ctypes.c_longlong,
    ctypes.c_longlong,
    ctypes.c_longlong,
    wintypes.DWORD,
    wintypes.DWORD,
    wintypes.HANDLE,
    wintypes.HANDLE,
    wintypes.LPVOID,
)

kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
kernel32.CopyFileExW.argtypes = [
    wintypes.LPCWSTR,
    wintypes.LPCWSTR,
    COPY_PROGRESS_ROUTINE,
    wintypes.LPVOID,
    ctypes.POINTER(wintypes.BOOL),
    wintypes.DWORD,
]
kernel32.CopyFileExW.restype = wintypes.BOOL


def format_bytes(byte_count: int) -> str:
    size = float(byte_count)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.1f} {unit}"
        size /= 1024

    return f"{size:,.1f} GB"


def discover_files(source_folder: Path, destination_folder: Path) -> list[FileCopyItem]:
    files: list[FileCopyItem] = []
    for source_file in source_folder.rglob("*"):
        if not source_file.is_file():
            continue

        relative_path = source_file.relative_to(source_folder)
        files.append(
            FileCopyItem(
                source=source_file,
                destination=destination_folder / relative_path,
                size=source_file.stat().st_size,
            )
        )

    return files


def create_destination_dirs(source_folder: Path, destination_folder: Path) -> int:
    created_dirs = 0
    for source_dir in source_folder.rglob("*"):
        if not source_dir.is_dir():
            continue

        destination_dir = destination_folder / source_dir.relative_to(source_folder)
        destination_dir.mkdir(parents=True, exist_ok=True)
        created_dirs += 1

    return created_dirs


def copy_file_with_progress(
    item: FileCopyItem,
    progress: Progress,
    file_task_id: TaskID,
    overall_task_id: TaskID,
) -> None:
    item.destination.parent.mkdir(parents=True, exist_ok=True)
    previous_bytes = 0

    def copy_progress(
        _total_file_size: int,
        total_bytes_transferred: int,
        _stream_size: int,
        _stream_bytes_transferred: int,
        _stream_number: int,
        _callback_reason: int,
        _source_file: int,
        _destination_file: int,
        _data: object,
    ) -> int:
        nonlocal previous_bytes
        advance = max(total_bytes_transferred - previous_bytes, 0)
        previous_bytes = total_bytes_transferred
        progress.update(file_task_id, advance=advance)
        progress.update(overall_task_id, advance=advance)
        return PROGRESS_CONTINUE

    progress_callback = COPY_PROGRESS_ROUTINE(copy_progress)
    cancel = wintypes.BOOL(False)
    copied = kernel32.CopyFileExW(
        str(item.source),
        str(item.destination),
        progress_callback,
        None,
        ctypes.byref(cancel),
        COPY_FILE_DEFAULT_FLAGS,
    )

    if not copied:
        raise ctypes.WinError(ctypes.get_last_error())

    if item.size == 0:
        progress.update(file_task_id, completed=1)


def copy_files(files: list[FileCopyItem], total_bytes: int) -> None:
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        refresh_per_second=4,
    )

    with progress:
        overall_task = progress.add_task("Copying files", total=max(total_bytes, 1))
        file_task = progress.add_task("Current file", total=1)

        for file_number, item in enumerate(files, start=1):
            progress.update(
                file_task,
                completed=0,
                total=max(item.size, 1),
                description=f"[{file_number:,}/{len(files):,}] {item.source.name}",
            )

            try:
                copy_file_with_progress(
                    item=item,
                    progress=progress,
                    file_task_id=file_task,
                    overall_task_id=overall_task,
                )
            except OSError as error:
                raise OSError(f"Failed while copying {item.source} to {item.destination}: {error}") from error

        if total_bytes == 0:
            progress.update(overall_task, completed=1)


def run_robocopy(source_folder: Path, destination_folder: Path) -> None:
    command = [
        "robocopy",
        str(source_folder),
        str(destination_folder),
        "/E",
        f"/MT:{ROBOCOPY_THREADS}",
        "/R:2",
        "/W:2",
        "/COPY:DAT",
        "/DCOPY:DAT",
        "/TEE",
    ]

    console.print(f"[bold]Running robocopy with {ROBOCOPY_THREADS} threads...[/bold]")
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    )

    if process.stdout is None:
        raise RuntimeError("Robocopy did not provide an output stream.")

    for output_line in process.stdout:
        line = output_line.rstrip()
        if line:
            console.print(line, markup=False)

    return_code = process.wait()
    if return_code >= 8:
        raise RuntimeError(f"Robocopy failed with exit code {return_code}.")

    console.print(f"[green]Robocopy completed with exit code {return_code}.[/green]")


nr = NETRESOURCE()
nr.dwType = RESOURCETYPE_DISK
nr.lpRemoteName = share

connected_here = False
console.print(
    Panel.fit(
        f"[bold]Copying TUFLOW folder[/bold]\n"
        f"Source: {source}\n"
        f"Destination: {destination}\n"
        f"User: {username}",
        title="TUFLOW_MLGD",
    )
)

result = mpr.WNetAddConnection2W(ctypes.byref(nr), password, username, 0)
if result == NO_ERROR:
    connected_here = True
elif result == ERROR_SESSION_CREDENTIAL_CONFLICT:
    # A connection already exists for this server/share; try using it.
    pass
else:
    raise ctypes.WinError(result)

try:
    if not source.is_dir():
        raise FileNotFoundError(f"Source folder not found: {source}")

    destination_root.mkdir(parents=True, exist_ok=True)

    with console.status("[bold]Scanning source folder...[/bold]"):
        files = discover_files(source_folder=source, destination_folder=destination)

    total_bytes = sum(item.size for item in files)
    console.print(f"Found {len(files):,} files totalling {format_bytes(total_bytes)}.")

    if COPY_ENGINE == "robocopy":
        run_robocopy(source_folder=source, destination_folder=destination)
    else:
        created_dirs = create_destination_dirs(source_folder=source, destination_folder=destination)
        copy_files(files=files, total_bytes=total_bytes)
        console.print(
            f"[green]Copied {len(files):,} files and created {created_dirs:,} directories to {destination}.[/green]"
        )

finally:
    if connected_here:
        mpr.WNetCancelConnection2W(share, 0, True)
