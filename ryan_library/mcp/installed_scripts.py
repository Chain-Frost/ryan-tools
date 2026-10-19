"""List and safely copy allowlisted human-facing wrappers from an installed wheel."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import stat
import tempfile
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path
from typing import cast


def installed_files() -> tuple[str, ...]:
    """Return the reviewed wheel manifest without scanning unmaintained scripts."""
    resource = files("ryan_library.resources.mcp").joinpath("installed_scripts.json")
    raw: object = json.loads(resource.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = "Invalid installed script manifest."
        raise ValueError(msg)
    metadata = cast("dict[str, object]", raw)
    if metadata.get("schema_version") != 1:
        msg = "Unsupported installed script manifest version."
        raise ValueError(msg)
    raw_entries: object = metadata.get("installed_files")
    entries = cast("list[object]", raw_entries) if isinstance(raw_entries, list) else []
    if not entries or not all(isinstance(item, str) for item in entries):
        msg = "Invalid installed script manifest paths."
        raise ValueError(msg)
    return tuple(sorted(cast("list[str]", entries)))


def available_wrappers() -> tuple[str, ...]:
    """List copyable scripts, excluding support documentation and package inits."""
    return tuple(
        item
        for item in installed_files()
        if Path(item).suffix.casefold() in {".py", ".bat", ".ps1"} and Path(item).name != "__init__.py"
    )


def _copy_new_wrapper(source: Path, target: Path) -> None:
    """Create a new wrapper exclusively, preserving mode without path-based chmod."""
    # Exclusive opening prevents a concurrent writer from inserting a symlink.
    with source.open("rb") as original, target.open("xb") as copied:
        shutil.copyfileobj(original, copied)
        if os.name == "posix":
            # Set permissions on the opened file, not a possibly replaced path.
            os.fchmod(copied.fileno(), stat.S_IMODE(source.stat().st_mode))
        else:
            # Windows chmod/copy-mode is path-based; preserve its read-only bit
            # while the exclusively created file is still open.
            shutil.copymode(source, target)


def _replace_wrapper_posix(source: Path, target: Path) -> None:
    """Publish and clean up staged files through verified directory descriptors.

    Never recurse through a staging pathname during cleanup: another user can
    rename a child directory in an unsafe writable parent. POSIX staging and
    publication are relative to the already-open destination and stage dirs.
    """
    directory_flags: int = os.O_RDONLY | cast("int", vars(os)["O_DIRECTORY"]) | cast("int", vars(os)["O_NOFOLLOW"])
    destination_fd = os.open(target.parent, directory_flags)
    try:
        destination_stat = os.fstat(destination_fd)
        current_uid = cast("Callable[[], int]", vars(os)["geteuid"])()
        # A non-sticky, peer-writable directory lets another user swap our
        # staging entry between identity checking and rmdir. A sticky parent
        # is safe only if its owner is the caller or root.
        if destination_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH) and (
            not destination_stat.st_mode & stat.S_ISVTX or destination_stat.st_uid not in {0, current_uid}
        ):
            msg = f"Refusing atomic overwrite in an unsafe peer-writable directory: {target.parent}"
            raise PermissionError(msg)

        # mkdir via dirfd prevents redirecting the parent pathname while
        # staging; a random name also avoids collisions with existing entries.
        staging_name = f".{target.name}.stage-{secrets.token_hex(16)}"
        os.mkdir(staging_name, mode=0o700, dir_fd=destination_fd)
        staging_fd: int | None = None
        try:
            staging_fd = os.open(staging_name, directory_flags, dir_fd=destination_fd)
            staging_stat = os.fstat(staging_fd)
            if (
                staging_stat.st_uid != current_uid
                or stat.S_IMODE(staging_stat.st_mode) != 0o700
            ):
                msg = f"Staging directory must be private and owned by this user: {staging_name}"
                raise PermissionError(msg)

            staged_fd = os.open(
                target.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | cast("int", vars(os)["O_NOFOLLOW"]),
                0o600,
                dir_fd=staging_fd,
            )
            with os.fdopen(staged_fd, "wb") as temporary, source.open("rb") as original:
                shutil.copyfileobj(original, temporary)
                os.fchmod(temporary.fileno(), stat.S_IMODE(os.fstat(original.fileno()).st_mode))
            os.replace(target.name, target.name, src_dir_fd=staging_fd, dst_dir_fd=destination_fd)
        finally:
            if staging_fd is not None:
                try:
                    # If publication failed, unlink only our staged file from
                    # the originally opened directory, not a swapped pathname.
                    try:
                        os.unlink(target.name, dir_fd=staging_fd)
                    except FileNotFoundError:
                        pass

                    # If the name now refers to something else, leave it
                    # untouched. os.rmdir is nonrecursive even if a rename
                    # occurs later; the parent mode guard prevents peer swaps.
                    try:
                        current = os.stat(staging_name, dir_fd=destination_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        pass
                    else:
                        if (current.st_dev, current.st_ino) == (staging_stat.st_dev, staging_stat.st_ino):
                            os.rmdir(staging_name, dir_fd=destination_fd)
                finally:
                    os.close(staging_fd)
            else:
                # Opening the new stage failed. A nonrecursive rmdir cannot
                # descend into a substituted directory or follow a symlink.
                os.rmdir(staging_name, dir_fd=destination_fd)
    finally:
        os.close(destination_fd)

def copy_wrapper(
    script: str,
    destination: Path,
    *,
    overwrite: bool = False,
    dry_run: bool = False,
    scripts_root: Path | None = None,
) -> Path:
    """Copy one exact allowlisted wrapper, never overwriting without permission."""
    if script not in available_wrappers():
        msg = f"Script is not in the installed wrapper allowlist: {script}"
        raise ValueError(msg)
    root = scripts_root if scripts_root is not None else Path(__file__).absolute().parents[2] / "ryan-scripts"
    source = root / script
    if not source.is_file() or source.is_symlink():
        msg = f"Installed wrapper is missing or a symlink: {source}"
        raise FileNotFoundError(msg)
    target_directory = destination.expanduser().absolute()
    if not target_directory.is_dir():
        msg = f"Destination must be an existing directory: {target_directory}"
        raise NotADirectoryError(msg)
    target = target_directory / Path(script).name
    if target.exists() or target.is_symlink():
        if not overwrite:
            msg = f"Destination already exists (pass --overwrite to replace): {target}"
            raise FileExistsError(msg)
        if not target.is_file() or target.is_symlink():
            msg = f"Refusing to replace a non-file or symlink: {target}"
            raise ValueError(msg)
    if dry_run:
        return target
    if not overwrite:
        _copy_new_wrapper(source, target)
        return target

    if os.name == "posix":
        _replace_wrapper_posix(source, target)
    else:
        # Windows does not support fd-relative os.replace. Retain the normal
        # TemporaryDirectory lifecycle and source read-only attribute there.
        with tempfile.TemporaryDirectory(dir=target_directory, prefix=f".{target.name}.stage-") as staging_directory:
            staged = Path(staging_directory) / target.name
            with source.open("rb") as original, staged.open("xb") as temporary:
                shutil.copyfileobj(original, temporary)
            shutil.copymode(source, staged)
            staged.replace(target)
    return target


def main(argv: list[str] | None = None) -> int:
    """Show installed wrappers or copy a selected original to a project directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("list", help="List installed copyable scripts.")
    copier = actions.add_parser("copy", help="Copy one original script; does not execute it.")
    copier.add_argument("script", help="Exact script path shown by the list command.")
    copier.add_argument("destination", type=Path, help="Existing project directory.")
    copier.add_argument("--overwrite", action="store_true", help="Explicitly replace an existing regular file.")
    copier.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    args = parser.parse_args(argv)
    if args.action == "list":
        for item in available_wrappers():
            print(item)
        return 0
    try:
        copied = copy_wrapper(
            args.script,
            args.destination,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(f"{'Would copy to' if args.dry_run else 'Copied to'}: {copied}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
