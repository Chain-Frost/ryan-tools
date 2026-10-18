#!/usr/bin/env python3
"""Register the QGIS and ryan-tools Python MCP servers in Codex.

No third-party dependency is required to edit config.toml.
The QGIS desktop plugin must be installed separately in QGIS.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any, cast

SERVER_MODULES: dict[str, str] = {
    "qgis": "qgis_mcp.server",
    "ryan-tools": "ryan_library.mcp.server",
}
QGIS_MCP_SOURCE = "https://github.com/nkarasiak/qgis-mcp/archive/refs/heads/main.zip"
TABLE = re.compile(r"^\s*\[(?!\[)(.+?)\]\s*(?:#.*)?$")
ARRAY_TABLE = re.compile(r"^\s*\[\[(.+?)\]\]\s*(?:#.*)?$")


def _table_path(line: str) -> tuple[str, ...] | None:
    """Decode quoted table names without treating dots in strings as separators."""
    if match := ARRAY_TABLE.match(line):
        return ("__array_table__", match.group(1))
    if not (match := TABLE.match(line)):
        return None
    key = "__codex_mcp_probe__"
    data: dict[str, Any] = tomllib.loads(f"[{match.group(1)}]\n{key} = true\n")
    path: list[str] = []
    while key not in data:
        name, child = next(iter(data.items()))
        path.append(name)
        if not isinstance(child, dict):
            msg = f"Invalid TOML table: {line.strip()}"
            raise ValueError(msg)
        data = cast(dict[str, Any], child)
    return tuple(path)


def _value(item: Any) -> str:
    if isinstance(item, str):
        return json.dumps(item, ensure_ascii=False)
    if isinstance(item, bool):
        return str(item).lower()
    if isinstance(item, (int, float)):
        return str(item).lower()
    if isinstance(item, list):
        items = cast(list[Any], item)
        return "[" + ", ".join(_value(part) for part in items) + "]"
    if isinstance(item, dict):
        fields = cast(dict[str, Any], item)
        entries = [
            f"{json.dumps(key) if not re.fullmatch(r'[a-zA-Z0-9_-]+', key) else key} = {_value(value)}"
            for key, value in fields.items()
        ]
        return "{ " + ", ".join(entries) + " }"
    msg = f"Unsupported existing Codex setting: {type(item).__name__}"
    raise ValueError(msg)


def _table(header: tuple[str, ...], data: dict[str, Any]) -> str:
    def key(name: str) -> str:
        return name if re.fullmatch(r"[a-zA-Z0-9_-]+", name) else json.dumps(name)

    return (
        "\n".join(
            [f"[{'.'.join(key(part) for part in header)}]"]
            + [f"{key(field)} = {_value(value)}" for field, value in data.items()]
        )
        + "\n"
    )


def render_config(config: str, python: Path, root: Path, profile: str) -> str:  # noqa: C901
    """Preserve unrelated TOML data while replacing two server registrations."""
    existing = cast(dict[str, Any], tomllib.loads(config))
    raw_servers: Any = existing.get("mcp_servers", {})
    if not isinstance(raw_servers, dict):
        msg = "mcp_servers must be a TOML table"
        raise ValueError(msg)
    servers = cast(dict[str, Any], raw_servers)

    replacements: dict[tuple[str, ...], str] = {}
    updated_entries: dict[str, dict[str, Any]] = {}
    for name, module in SERVER_MODULES.items():
        raw_current: Any = servers.get(name, {})
        if not isinstance(raw_current, dict):
            msg = f"Invalid MCP entry: {name}"
            raise ValueError(msg)
        current = cast(dict[str, Any], raw_current)
        extra_tables = [key for key, val in current.items() if isinstance(val, dict) and key != "env"]
        if extra_tables:
            msg = f"Unsupported nested MCP settings in {name}: {extra_tables}"
            raise ValueError(msg)
        raw_env: Any = current.get("env", {})
        if not isinstance(raw_env, dict):
            msg = f"Invalid MCP environment for {name}"
            raise ValueError(msg)
        env = cast(dict[str, Any], raw_env)
        if any(not isinstance(value, str) for value in env.values()):
            msg = f"Invalid MCP environment for {name}"
            raise ValueError(msg)
        safe_env = cast(dict[str, str], env)
        fields: dict[str, Any] = {
            "command": str(python),
            "args": ["-B", "-m", module],
            **{key: val for key, val in current.items() if key not in ("command", "args", "env")},
            "enabled": True,
        }
        environment: dict[str, str] = {**safe_env, "PYTHONDONTWRITEBYTECODE": "1"}
        if name == "ryan-tools":
            environment.update(
                PYTHONUTF8="1",
                RYAN_MCP_PROFILE=profile,
                RYAN_TOOLS_REPOSITORY_ROOT=str(root),
            )
        updated_entries[name] = {**fields, "env": environment}
        replacements[("mcp_servers", name)] = _table(("mcp_servers", name), fields)
        replacements[("mcp_servers", name, "env")] = _table(("mcp_servers", name, "env"), environment)

    # Inline TOML tables are immutable; appendable [mcp_servers.X] sections
    # would make an otherwise valid user config invalid. Keep inline form.
    inline_root = re.compile(r"^\s*mcp_servers\s*=\s*\{.*\}\s*(?:#.*)?$")
    original_lines = config.splitlines(keepends=True)
    for index, line in enumerate(original_lines):
        if not inline_root.fullmatch(line.rstrip("\r\n")):
            continue
        updated_servers = {**servers, **updated_entries}
        newline = "\r\n" if line.endswith("\r\n") else "\n"
        original_lines[index] = f"mcp_servers = {_value(updated_servers)}{newline}"
        result = "".join(original_lines)
        parsed = cast(dict[str, Any], tomllib.loads(result))
        if any(parsed.get(key) != val for key, val in existing.items() if key != "mcp_servers"):
            msg = "Unexpected change to unrelated Codex settings"
            raise ValueError(msg)
        if any(parsed["mcp_servers"].get(key) != val for key, val in servers.items() if key not in SERVER_MODULES):
            msg = "Unexpected change to another MCP server"
            raise ValueError(msg)
        return result

    sections: list[tuple[tuple[str, ...] | None, list[str]]] = []
    path: tuple[str, ...] | None = None
    lines: list[str] = []
    for line in config.splitlines(keepends=True):
        header = _table_path(line)
        if header is not None:
            sections.append((path, lines))
            path, lines = header, [line]
        else:
            lines.append(line)
    sections.append((path, lines))
    encountered = {header for header, _ in sections}
    for name in SERVER_MODULES:
        if name in servers and ("mcp_servers", name) not in encountered:
            msg = f"Use [mcp_servers.{name}] rather than an inline/dotted TOML definition"
            raise ValueError(msg)
    emitted: set[tuple[str, ...]] = set()
    output: list[str] = []
    for header, original in sections:
        if header is not None and header in replacements:
            if header in emitted:
                msg = f"Duplicate MCP table: {header}"
                raise ValueError(msg)
            emitted.add(header)
            output.append(replacements[header].rstrip("\n") + "\n\n")
        else:
            output.extend(original)
    rendered = "".join(output).rstrip("\n") + "\n"
    for header, text in replacements.items():
        if header not in emitted:
            rendered = rendered.rstrip("\n") + "\n\n" + text

    updated = cast(dict[str, Any], tomllib.loads(rendered))
    if any(updated.get(key) != val for key, val in existing.items() if key != "mcp_servers"):
        msg = "Unexpected change to unrelated Codex settings"
        raise ValueError(msg)
    if any(updated["mcp_servers"].get(key) != val for key, val in servers.items() if key not in SERVER_MODULES):
        msg = "Unexpected change to another MCP server"
        raise ValueError(msg)
    return rendered


def _write(path: Path, data: str, *, dry_run: bool) -> None:
    original = path.read_bytes() if path.is_file() else b""
    bom = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    newline = "\r\n" if b"\r\n" in original else "\n"
    encoded = bom + data.replace("\r\n", "\n").replace("\n", newline).encode("utf-8")
    if original == encoded:
        print(f"Already configured: {path}")
        return
    if dry_run:
        print(f"Would update: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        backup = path.with_name(f"{path.name}.bak.{datetime.now():%Y%m%d-%H%M%S-%f}")
        shutil.copy2(path, backup)
        print(f"Backed up existing config to {backup}")
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".codex-mcp-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if path.is_file() and path.read_bytes() != original:
            msg = "Codex configuration changed during setup"
            raise RuntimeError(msg)
        Path(temporary).replace(path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    print(f"Updated {path}")


def _run(command: list[str], *, dry_run: bool) -> None:
    print(subprocess.list2cmdline(command))
    if not dry_run:
        subprocess.run(command, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--ryan-root", type=Path, default=Path(__file__).absolute().parents[1])
    parser.add_argument("--qgis-source", default=QGIS_MCP_SOURCE)
    parser.add_argument("--profile", choices=("core", "analysis", "create", "privileged"), default="create")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--install", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-import-checks", action="store_true")
    args = parser.parse_args(argv)

    python: Path = args.python.expanduser().absolute()
    root: Path = args.ryan_root.expanduser().absolute()
    config: Path = (
        args.config.expanduser().absolute()
        if args.config
        else Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser().absolute() / "config.toml"
    )
    if not python.is_file():
        parser.error(f"Python not found: {python}")
    if not ((root / "pyproject.toml").is_file() and (root / "ryan-scripts").is_dir()):
        parser.error(f"Not a ryan-tools checkout: {root}")

    try:
        original = config.read_text(encoding="utf-8-sig") if config.is_file() else ""
        rendered = render_config(original, python, root, args.profile)

        if args.install:
            source: str = args.qgis_source
            if "://" not in source:
                package = Path(source).expanduser().absolute()
                if not (package / "pyproject.toml").is_file():
                    msg = f"No QGIS MCP Python project found at {package}"
                    raise ValueError(msg)
                source = str(package)
            pip = [str(python), "-B", "-m", "pip", "install", "--upgrade"]
            _run([*pip, source], dry_run=args.dry_run)
            _run([*pip, "-e", f"{root}[mcp]"], dry_run=args.dry_run)

        _write(config, rendered, dry_run=args.dry_run)
        if not args.dry_run and not args.skip_import_checks:
            for module in SERVER_MODULES.values():
                _run([str(python), "-B", "-c", f"import {module}"], dry_run=False)

    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError, tomllib.TOMLDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("Restart Codex and check 'codex mcp list'.")
    print("In QGIS, start the QGIS MCP plugin server; then test ping and diagnose.")
    print("For ryan-tools, call check_repo_health.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
