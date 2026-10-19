"""List and safely copy allowlisted human-facing wrappers from an installed wheel."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import tempfile
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


def _replace_wrapper_posix(source: Path, target: Path, staging_directory: Path) -> None:
    """Publish the genuine staged inode using directory descriptors.

    A peer can rename a private child directory in a writable parent; using
    the child *pathname* at publication would then trust a substituted entry.
    Both publication paths are therefore relative to opened directories.
    """
    directory_flags: int = (
        os.O_RDONLY | cast("int", getattr(os, "O_DIRECTORY")) | cast("int", getattr(os, "O_NOFOLLOW"))
    )
    destination_fd = os.open(target.parent, directory_flags)
    try:
        staging_fd = os.open(staging_directory, directory_flags)
        try:
            staging_stat = os.fstat(staging_fd)
            if staging_stat.st_uid != cast("int", getattr(os, "geteuid")()) or stat.S_IMODE(staging_stat.st_mode) != 0o700:
                msg = f"Staging directory must be private and owned by this user: {staging_directory}"
                raise PermissionError(msg)

            # Open relative to the verified directory inode, not the pathname.
            staged_fd = os.open(
                target.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | cast("int", getattr(os, "O_NOFOLLOW")),
                0o600,
                dir_fd=staging_fd,
            )
            with os.fdopen(staged_fd, "wb") as temporary, source.open("rb") as original:
                shutil.copyfileobj(original, temporary)
                os.fchmod(temporary.fileno(), stat.S_IMODE(os.fstat(original.fileno()).st_mode))
            # Even if the stage directory's pathname is swapped, the source
            # entry below is resolved in the original, already-open directory.
            os.replace(target.name, target.name, src_dir_fd=staging_fd, dst_dir_fd=destination_fd)
        finally:
            os.close(staging_fd)
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

    with tempfile.TemporaryDirectory(dir=target_directory, prefix=f".{target.name}.stage-") as staging_directory:
        staged = Path(staging_directory) / target.name
        if os.name == "posix":
            _replace_wrapper_posix(source, target, Path(staging_directory))
        else:
            # Windows does not support fd-relative os.replace. Preserve the
            # original cross-platform copy behavior and read-only attribute.
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
