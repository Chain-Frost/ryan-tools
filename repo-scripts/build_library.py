"""Utility to bump the package version and build a wheel distribution.

This mirrors the behaviour of ``packager.bat`` so it can be executed on
platforms without a Windows shell. The script will:

1. Determine the version number to apply (either from ``--version`` or by
   incrementing the daily counter used in ``setup.py``).
2. Update ``setup.py`` with the chosen version string.
3. Ensure ``python -m build`` is available.
4. Build the wheel into a temporary directory and move it into ``dist/``.

Run from the repository root::

    python repo-scripts/build_library.py
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

RE_VERSION: re.Pattern[str] = re.compile(pattern=r'(version\s*=\s*")(?P<version>[^\"]+)(")')


def _read_setup_version(setup_path: Path) -> str:
    content: str = setup_path.read_text(encoding="utf-8")
    match: re.Match[str] | None = RE_VERSION.search(string=content)
    if not match:
        msg = "Could not locate version string in setup.py"
        raise RuntimeError(msg)
    return match.group("version")


def _format_auto_version(current_version: str, today: _dt.date) -> str:
    date_prefix: str = today.strftime("%y.%m.%d")
    parts: list[str] = current_version.split(".")
    if len(parts) == 4 and ".".join(parts[:3]) == date_prefix:
        try:
            counter: int = int(parts[3]) + 1
        except ValueError:
            counter = 1
    else:
        counter = 1
    return f"{date_prefix}.{counter}"


def _update_setup_version(setup_path: Path, new_version: str) -> str:
    content: str = setup_path.read_text(encoding="utf-8")
    match: re.Match[str] | None = RE_VERSION.search(string=content)
    if not match:
        msg = "Could not locate version string in setup.py"
        raise RuntimeError(msg)
    updated: str = RE_VERSION.sub(repl=rf"\g<1>{new_version}\g<3>", string=content, count=1)
    setup_path.write_text(data=updated, encoding="utf-8")
    return match.group("version")


def _ensure_build_installed(python: str) -> None:
    subprocess.run([python, "-m", "pip", "install", "--upgrade", "build"], check=True)


def _run_build(python: str, project_root: Path, dist_dir: Path) -> Path:
    build_dir = Path(tempfile.mkdtemp(prefix="ryan-tools-build-"))
    try:
        subprocess.run(
            [python, "-m", "build", "--wheel", "--outdir", str(build_dir)],
            cwd=project_root,
            check=True,
        )
        wheels: list[Path] = list(build_dir.glob("*.whl"))
        if not wheels:
            msg = "Build succeeded but no wheel was produced"
            raise RuntimeError(msg)
        dist_dir.mkdir(parents=True, exist_ok=True)
        for wheel in wheels:
            target: Path = dist_dir / wheel.name
            if target.exists():
                target.unlink()
            shutil.move(str(wheel), target)
        return wheels[0]
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def _clean_directories(project_root: Path) -> None:
    for dirname in ("dist", "build"):
        path: Path = project_root / dirname
        if path.exists():
            shutil.rmtree(path)

    for egg_info in project_root.glob("*.egg-info"):
        if egg_info.exists():
            shutil.rmtree(egg_info)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the ryan-functions wheel")
    parser.add_argument(
        "--version",
        help=(
            "Explicit version string to apply. If omitted, the version is derived "
            "from today's date and the current counter in setup.py."
        ),
    )
    parser.add_argument(
        "--skip-pip",
        action="store_true",
        help="Skip ensuring the 'build' package is installed",
    )
    args: argparse.Namespace = parser.parse_args(argv)

    project_root: Path = Path(__file__).resolve().parents[1]
    setup_path: Path = project_root / "setup.py"

    if not setup_path.exists():
        msg = "setup.py not found relative to script location"
        raise SystemExit(msg)

    current_version: str = _read_setup_version(setup_path=setup_path)
    new_version = args.version or _format_auto_version(current_version=current_version, today=_dt.date.today())

    print(f"Current version: {current_version}")
    print(f"New version:     {new_version}")

    previous_content: str = setup_path.read_text(encoding="utf-8")
    try:
        _update_setup_version(setup_path=setup_path, new_version=new_version)
        if not args.skip_pip:
            _ensure_build_installed(python=sys.executable)
        _clean_directories(project_root=project_root)
        dist_dir: Path = project_root / "dist"
        wheel_path: Path = _run_build(python=sys.executable, project_root=project_root, dist_dir=dist_dir)
        print(f"Wheel written to {dist_dir / wheel_path.name}")
    except Exception:
        setup_path.write_text(data=previous_content, encoding="utf-8")
        raise

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
