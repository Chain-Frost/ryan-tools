"""List and safely copy allowlisted human-facing wrappers from an installed wheel."""

from __future__ import annotations

import argparse
import json
import shutil
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
        # Exclusive creation also rejects a symlink installed since the earlier check.
        with source.open("rb") as original, target.open("xb") as copied:
            shutil.copyfileobj(original, copied)
        return target

    # A temporary file in the destination directory gives os.replace an atomic
    # replacement target. Unlike copy2(source, target), os.replace does not
    # follow a destination symlink swapped in after the existence checks.
    staged: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=target_directory,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            staged = Path(temporary.name)
            with source.open("rb") as original:
                shutil.copyfileobj(original, temporary)
        staged.replace(target)
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)
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
