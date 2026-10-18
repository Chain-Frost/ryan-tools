"""Install GIS dependencies or the newest locally built ryan-functions wheel."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

GIS_WHEEL_INDEX: str = "https://gisidx.github.io/gwi"
WHEEL_PATTERN: str = "ryan_functions-*.whl"
# Install the current upstream bridge when the wheel-based developer path is used.
QGIS_MCP_ARCHIVE: str = "https://github.com/nkarasiak/qgis-mcp/archive/refs/heads/main.zip"


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


def _pip_commands(
    wheel: Path, force_reinstall: bool, *, qgis_mcp_source: str | None = None, skip_qgis_mcp: bool = False
) -> list[list[str]]:
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

    commands: list[list[str]] = [_gis_dependency_command()]
    if not skip_qgis_mcp:
        source: str = qgis_mcp_source or QGIS_MCP_ARCHIVE
        commands.append([*pip, "install", "--upgrade", source])
    commands.append(
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
            f"{wheel}[mcp]",
        ]
    )
    return commands


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
    parser.add_argument(
        "--qgis-mcp-source",
        help="Override the current upstream QGIS MCP archive with an approved local source directory or URL",
    )
    parser.add_argument(
        "--skip-qgis-mcp",
        action="store_true",
        help="Skip the separately installed QGIS MCP bridge (use for offline/headless machines)",
    )
    args: argparse.Namespace = parser.parse_args(argv)

    if args.dependencies_only and args.force_reinstall:
        parser.error("--dependencies-only cannot be combined with --force-reinstall")

    print(f"Using Python: {sys.executable}")
    if args.dependencies_only:
        commands: list[list[str]] = [_gis_dependency_command()]
    else:
        project_root: Path = Path(__file__).absolute().parents[1]
        try:
            wheel: Path = _latest_wheel(dist_dir=project_root / "dist")
        except FileNotFoundError as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1

        print(f"Installing:   {wheel}")
        commands = _pip_commands(
            wheel=wheel,
            force_reinstall=args.force_reinstall,
            qgis_mcp_source=args.qgis_mcp_source,
            skip_qgis_mcp=args.skip_qgis_mcp,
        )
        if args.force_reinstall and not args.skip_qgis_mcp:
            print("NOTE: --force-reinstall uses --no-deps and does not install QGIS MCP.")

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
