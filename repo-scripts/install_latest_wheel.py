"""Install GIS dependencies or the newest locally built ryan-tools wheel."""

from __future__ import annotations

import argparse
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

GIS_WHEEL_INDEX: str = "https://gisidx.github.io/gwi"
WHEEL_PATTERN: str = "ryan_tools-*.whl"
# Install the current upstream bridge when the wheel-based developer path is used.
QGIS_MCP_ARCHIVE: str = "https://github.com/nkarasiak/qgis-mcp/archive/refs/heads/main.zip"
PYTUFLOW_REQUIREMENT_FILE = Path(__file__).absolute().with_name("pytuflow-requirement.txt")


def _pytuflow_requirement() -> str:
    """Read the single immutable PyTUFLOW PR17 source used for normal installs."""
    requirement = PYTUFLOW_REQUIREMENT_FILE.read_text(encoding="utf-8").strip()
    if not requirement.startswith("pytuflow @ https://github.com/Chain-Frost/PyTuflow/archive/"):
        raise ValueError(f"Invalid PyTUFLOW source requirement in {PYTUFLOW_REQUIREMENT_FILE}")
    return requirement


def _latest_wheel(dist_dir: Path) -> Path:
    """Return the most recently modified ryan-tools wheel."""
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
    """Build GIS-first install commands with one exact accepted parser source."""
    pip: list[str] = [sys.executable, "-m", "pip"]
    if force_reinstall:
        # Recovery mode deliberately never changes dependencies or certifies the parser.
        return [[*pip, "install", "--upgrade", "--force-reinstall", "--no-deps", str(wheel)]]

    pinned_parser = _pytuflow_requirement()
    commands: list[list[str]] = [_gis_dependency_command()]
    if not skip_qgis_mcp:
        source: str = qgis_mcp_source or QGIS_MCP_ARCHIVE
        commands.append([*pip, "install", "--upgrade", source])
    # An existing official wheel may have the same version but no validation API.
    # Force the exact selected archive even when the installed version matches.
    commands.append([*pip, "install", "--upgrade", "--force-reinstall", "--no-deps", pinned_parser])
    # Resolve the matching wheel AND this explicit parser requirement together,
    # so PyPI's same-version build cannot be selected as a transitive dependency.
    commands.append(
        [
            *pip, "install", "--upgrade", "--prefer-binary",
            "--extra-index-url", GIS_WHEEL_INDEX,
            "--only-binary=fiona", "--only-binary=rasterio", "--only-binary=gdal",
            f"{wheel}[mcp]", pinned_parser,
        ]
    )
    return commands


def migration_commands(commands: list[list[str]], *, verification_command: list[str]) -> list[list[str]]:
    """Automatically replace the retired distribution after validating its replacement."""
    try:
        distribution("ryan_functions")
    except PackageNotFoundError:
        return commands
    print("Migrating the legacy ryan_functions distribution to ryan-tools.")
    uninstall = [sys.executable, "-m", "pip", "uninstall", "--yes", "ryan_functions"]
    replacement = list(commands[-1])
    # Removing the old owner can remove files shared with an existing new-name install.
    if "--force-reinstall" not in replacement:
        replacement.append("--force-reinstall")
    return [verification_command, *commands[:-1], uninstall, replacement]


def _migration_commands(wheel: Path, commands: list[list[str]]) -> list[list[str]]:
    """Validate the local wheel before the one-time distribution-name migration."""
    verify = [sys.executable, str(Path(__file__).absolute().with_name("verify_wheel.py")), str(wheel)]
    return migration_commands(commands, verification_command=verify)


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
        commands = _migration_commands(wheel, commands)
        if args.force_reinstall:
            print("NOTE: --force-reinstall uses --no-deps and does not update or validate PyTUFLOW or QGIS MCP.")
        else:
            commands.extend(
                [
                    [sys.executable, "-m", "pip", "check"],
                    [
                        sys.executable, "-c",
                        "from ryan_library.functions.tuflow.pytuflow_compatibility "
                        "import require_validation_api; print(require_validation_api().diagnostic.message)",
                    ],
                ]
            )

    for command in commands:
        print(f"Command:      {subprocess.list2cmdline(command)}")
        if args.dry_run:
            continue
        result: subprocess.CompletedProcess[bytes] = subprocess.run(command, check=False)
        if result.returncode != 0:
            print(f"ERROR: pip exited with status {result.returncode}", file=sys.stderr)
            return result.returncode

    if args.dry_run:
        print("Dry run completed: no packages or validation capabilities were checked.")
    elif args.dependencies_only or args.force_reinstall:
        print("Requested recovery installation completed; full PyTUFLOW compatibility was not verified.")
    else:
        print("Installation completed; pip check and public PyTUFLOW capabilities verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
