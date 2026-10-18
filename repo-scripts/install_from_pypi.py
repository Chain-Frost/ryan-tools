"""Install current ryan_functions from PyPI and configure Codex MCP if present.

This end-user installer does not use a locally built wheel.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from install_latest_wheel import GIS_WHEEL_INDEX, QGIS_MCP_ARCHIVE
from setup_codex_mcp import main as configure_codex_mcp


def codex_installed() -> bool:
    """Detect the Codex CLI or an existing Codex user installation."""
    home = Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex")).expanduser()
    return shutil.which("codex") is not None or (home / "config.toml").is_file() or (home / "auth.json").is_file()


def pip_commands(*, qgis_mcp_source: str | None = None, skip_qgis_mcp: bool = False) -> list[list[str]]:
    """Prepare Windows-friendly binary GIS bootstrap and latest PyPI install."""
    pip = [sys.executable, "-B", "-m", "pip", "install", "--upgrade"]
    commands = [
        [
            *pip,
            "--extra-index-url",
            GIS_WHEEL_INDEX,
            "--only-binary=:all:",
            "fiona",
            "rasterio",
            "gdal",
        ],
    ]
    if not skip_qgis_mcp:
        commands.append([*pip, qgis_mcp_source or QGIS_MCP_ARCHIVE])
    commands.append(
        [
            *pip,
            "--prefer-binary",
            "--extra-index-url",
            GIS_WHEEL_INDEX,
            "--only-binary=fiona",
            "--only-binary=rasterio",
            "--only-binary=gdal",
            "ryan_functions[mcp]",
        ]
    )
    return commands


def main(argv: list[str] | None = None) -> int:
    """Install from PyPI, then register MCP servers only when Codex exists."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print commands without installing or editing files")
    parser.add_argument("--skip-codex", action="store_true", help="Do not change Codex configuration")
    parser.add_argument("--skip-qgis-mcp", action="store_true", help="Do not download the QGIS MCP Python bridge")
    parser.add_argument("--qgis-mcp-source", help="Alternate QGIS MCP source directory or URL")
    parser.add_argument(
        "--profile",
        choices=("core", "analysis", "create", "privileged"),
        default="create",
        help="ryan-tools MCP capability profile when configuring Codex",
    )
    args = parser.parse_args(argv)
    if sys.version_info < (3, 14):
        print("ERROR: This installer requires Python 3.14 or newer.", file=sys.stderr)
        return 1

    print(f"Python interpreter: {sys.executable}")
    for command in pip_commands(
        qgis_mcp_source=args.qgis_mcp_source,
        skip_qgis_mcp=args.skip_qgis_mcp,
    ):
        print(f"Command: {subprocess.list2cmdline(command)}", flush=True)
        if args.dry_run:
            continue
        result = subprocess.run(command, check=False)
        if result.returncode:
            print(f"ERROR: pip exited with status {result.returncode}", file=sys.stderr)
            return result.returncode

    if args.skip_codex:
        print("Codex setup skipped by request.")
    elif not codex_installed():
        print("Codex is not installed; no Codex configuration changed.")
    elif args.skip_qgis_mcp:
        print("Codex auto-setup skipped because QGIS MCP was excluded.")
        print("Run repo-scripts/setup_codex_mcp.py manually after installing both MCP servers.")
    else:
        root = Path(__file__).absolute().parents[1]
        print("Codex detected; checking both MCP registrations.")
        options = [
            "--python",
            sys.executable,
            "--ryan-root",
            str(root),
            "--profile",
            args.profile,
        ]
        if args.dry_run:
            options.append("--dry-run")
        result = configure_codex_mcp(options)
        if result:
            print("ERROR: Package installation completed, but Codex configuration failed.", file=sys.stderr)
            return result

    print("PyPI installation dry run complete." if args.dry_run else "PyPI installation complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
