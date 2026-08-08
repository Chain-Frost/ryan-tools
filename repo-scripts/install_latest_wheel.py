"""Install GIS dependencies or the newest locally built ryan-functions wheel."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

GIS_WHEEL_INDEX: str = "https://gisidx.github.io/gwi"
WHEEL_PATTERN: str = "ryan_functions-*.whl"


def _latest_wheel(dist_dir: Path) -> Path:
    """Return the most recently modified ryan-functions wheel."""
    wheels: list[Path] = list(dist_dir.glob(WHEEL_PATTERN))
    if not wheels:
        msg = f"No {WHEEL_PATTERN} wheel found in {dist_dir}"
        raise FileNotFoundError(msg)
    return max(wheels, key=lambda path: (path.stat().st_mtime_ns, path.name))


def _gis_dependency_command() -> list[str]:
    """Build the binary-only GIS dependency installation command."""
    pip: list[str] = [sys.executable, "-m", "pip"]
    return [
        *pip,
        "install",
        "--upgrade",
        "--extra-index-url",
        GIS_WHEEL_INDEX,
        "--only-binary=:all:",
        "fiona",
        "rasterio",
        "gdal",
    ]


def _pip_commands(wheel: Path, force_reinstall: bool) -> list[list[str]]:
    """Build the pip commands for a normal or no-dependency recovery install."""
    pip: list[str] = [sys.executable, "-m", "pip"]
    if force_reinstall:
        return [
            [
                *pip,
                "install",
                "--upgrade",
                "--force-reinstall",
                "--no-deps",
                str(wheel),
            ]
        ]

    return [
        _gis_dependency_command(),
        [
            *pip,
            "install",
            "--upgrade",
            "--prefer-binary",
            "--extra-index-url",
            GIS_WHEEL_INDEX,
            "--only-binary=fiona",
            "--only-binary=rasterio",
            "--only-binary=gdal",
            str(wheel),
        ],
    ]


def main(argv: list[str] | None = None) -> int:
    """Install the latest wheel and return the first failing pip exit code."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dependencies-only",
        action="store_true",
        help="Install or upgrade Fiona, Rasterio and GDAL without requiring a local wheel",
    )
    parser.add_argument(
        "--force-reinstall",
        action="store_true",
        help="Reinstall the wheel without resolving or changing dependencies",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the selected wheel and pip commands without running them",
    )
    args: argparse.Namespace = parser.parse_args(argv)

    if args.dependencies_only and args.force_reinstall:
        parser.error("--dependencies-only cannot be combined with --force-reinstall")

    print(f"Using Python: {sys.executable}")
    if args.dependencies_only:
        commands: list[list[str]] = [_gis_dependency_command()]
    else:
        project_root: Path = Path(__file__).resolve().parents[1]
        try:
            wheel: Path = _latest_wheel(dist_dir=project_root / "dist")
        except FileNotFoundError as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1

        print(f"Installing:   {wheel}")
        commands = _pip_commands(wheel=wheel, force_reinstall=args.force_reinstall)

    for command in commands:
        print(f"Command:      {subprocess.list2cmdline(command)}")
        if args.dry_run:
            continue
        result: subprocess.CompletedProcess[bytes] = subprocess.run(command, check=False)
        if result.returncode != 0:
            print(f"ERROR: pip exited with status {result.returncode}", file=sys.stderr)
            return result.returncode

    print("Installation completed successfully." if not args.dry_run else "Dry run completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
