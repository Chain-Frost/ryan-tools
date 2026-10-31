"""Install current ryan-tools from PyPI and configure Codex MCP if present.

This end-user installer does not use a locally built wheel.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from install_latest_wheel import installation_verification_commands, migration_commands, normal_install_commands
from setup_codex_mcp import disable_bytecode_for_environment
from setup_codex_mcp import main as configure_codex_mcp


def _vscode_codex_installed() -> bool:
    """Find the OpenAI Codex VS Code extension, including extension-only setups."""
    folders = [
        Path(os.environ.get("VSCODE_EXTENSIONS", str(Path.home() / ".vscode" / "extensions"))),
        Path.home() / ".vscode-insiders" / "extensions",
    ]
    for folder in folders:
        if folder.is_dir() and any(folder.glob("openai.chatgpt-*")):
            return True
    for editor in ("code", "code-insiders"):
        executable = shutil.which(editor)
        if executable is None:
            continue
        try:
            result = subprocess.run(
                [executable, "--list-extensions"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0 and "openai.chatgpt" in result.stdout.casefold().splitlines():
            return True
    return False


def codex_installed() -> bool:
    """Detect the CLI, an existing Codex home, or the OpenAI VS Code extension."""
    home = Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex")).expanduser()
    return (
        shutil.which("codex") is not None
        or (home / "config.toml").is_file()
        or (home / "auth.json").is_file()
        or _vscode_codex_installed()
    )


def supported_python(major: int, minor: int) -> bool:
    """Reject Python 3.12 and below without limiting future Python minors."""
    return (major, minor) >= (3, 13)


def pip_commands(
    *, qgis_mcp_source: str | None = None, skip_qgis_mcp: bool = False, disable_bytecode: bool = False
) -> list[list[str]]:
    """Prepare the shared GIS bootstrap, PyPI package and accepted parser install."""
    return normal_install_commands(
        "ryan-tools[mcp]",
        qgis_mcp_source=qgis_mcp_source,
        skip_qgis_mcp=skip_qgis_mcp,
        disable_bytecode=disable_bytecode,
    )


def _configure_codex(args: argparse.Namespace, *, disable_bytecode: bool) -> int:
    """Register installed servers when Codex and both bridges are available."""
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
        options.extend(["--disable-bytecode"] if disable_bytecode else [])
        result = configure_codex_mcp(options)
        if result:
            print("ERROR: Package installation completed, but Codex configuration failed.", file=sys.stderr)
            return result

    return 0


def main(argv: list[str] | None = None) -> int:
    """Install and verify PyTUFLOW support before optional Codex registration."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print commands without installing or editing files")
    parser.add_argument("--skip-codex", action="store_true", help="Do not change Codex configuration")
    parser.add_argument("--skip-qgis-mcp", action="store_true", help="Do not download the QGIS MCP Python bridge")
    parser.add_argument("--qgis-mcp-source", help="Alternate QGIS MCP source directory or URL")
    parser.add_argument(
        "--disable-bytecode", action="store_true", help="Disable .pyc writes (automatic on the BGE domain)"
    )
    parser.add_argument(
        "--profile",
        choices=("core", "analysis", "create", "privileged"),
        default="create",
        help="ryan-tools MCP capability profile when configuring Codex",
    )
    args = parser.parse_args(argv)
    if not supported_python(sys.version_info.major, sys.version_info.minor):
        print("ERROR: This installer requires Python 3.13 or newer.", file=sys.stderr)
        return 1

    disable_bytecode = args.disable_bytecode or disable_bytecode_for_environment()
    sys.dont_write_bytecode = sys.dont_write_bytecode or disable_bytecode
    print(f"Python interpreter: {sys.executable}")
    try:
        commands = pip_commands(
            qgis_mcp_source=args.qgis_mcp_source,
            skip_qgis_mcp=args.skip_qgis_mcp,
            disable_bytecode=disable_bytecode,
        )
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    commands = migration_commands(commands, verification_command=[*commands[-1], "--force-reinstall", "--dry-run"])
    commands.extend(installation_verification_commands(disable_bytecode=disable_bytecode))
    for command in commands:
        print(f"Command: {subprocess.list2cmdline(command)}", flush=True)
        if args.dry_run:
            continue
        completed: subprocess.CompletedProcess[bytes] = subprocess.run(command, check=False)
        if completed.returncode:
            print(f"ERROR: Installer command exited with status {completed.returncode}", file=sys.stderr)
            return completed.returncode

    result: int = _configure_codex(args, disable_bytecode=disable_bytecode)
    if result:
        return result

    if args.dry_run:
        print("PyPI installation dry run complete: no packages or validation capabilities were checked.")
    else:
        print("PyPI installation complete; pip check and public PyTUFLOW capabilities verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
