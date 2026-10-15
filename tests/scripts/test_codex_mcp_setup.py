"""Regression tests for Codex MCP bootstrap and wheel installation integration."""

from __future__ import annotations

import runpy
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

REPO = Path(__file__).absolute().parents[2]
codex: dict[str, Any] = runpy.run_path(str(REPO / "repo-scripts" / "setup_codex_mcp.py"))
installer: dict[str, Any] = runpy.run_path(str(REPO / "repo-scripts" / "install_latest_wheel.py"))
render = cast(Callable[[str, Path, Path, str], str], codex["render_config"])
pip_commands = cast(Callable[..., list[list[str]]], installer["_pip_commands"])


def test_new_config_contains_both_mcp_servers(tmp_path: Path) -> None:
    python = tmp_path / "python.exe"
    result = tomllib.loads(render("", python, tmp_path, "create"))
    servers = result["mcp_servers"]
    assert servers["qgis"]["args"] == ["-B", "-m", "qgis_mcp.server"]
    assert servers["ryan-tools"]["args"] == ["-B", "-m", "ryan_library.mcp.server"]
    assert servers["ryan-tools"]["env"]["RYAN_TOOLS_REPOSITORY_ROOT"] == str(tmp_path)
    assert all(item["command"] == str(python) for item in servers.values())


def test_codex_setup_preserves_unrelated_configuration_and_is_repeatable(tmp_path: Path) -> None:
    config = (
        'model = "gpt-6"\n'
        '[features]\nfoo = true\n'
        '[mcp_servers.other]\ncommand = "other-tool"\n'
        '[mcp_servers.qgis]\ncommand = "old"\nargs = ["old"]\nstartup_timeout_sec = 45\n'
        '[mcp_servers.qgis.env]\nQGIS_MCP_TOKEN = "keep-me"\n'
        '[mcp_servers."ryan-tools"]\ncommand = "old"\nargs = []\n'
    )
    first = render(config, tmp_path / "python.exe", tmp_path, "analysis")
    second = render(first, tmp_path / "python.exe", tmp_path, "analysis")
    assert first == second
    value = tomllib.loads(second)
    assert value["model"] == "gpt-6"
    assert value["features"]["foo"] is True
    assert value["mcp_servers"]["other"]["command"] == "other-tool"
    assert value["mcp_servers"]["qgis"]["startup_timeout_sec"] == 45
    assert value["mcp_servers"]["qgis"]["env"]["QGIS_MCP_TOKEN"] == "keep-me"
    assert value["mcp_servers"]["ryan-tools"]["env"]["RYAN_MCP_PROFILE"] == "analysis"


def test_existing_array_tables_survive(tmp_path: Path) -> None:
    config = '[[hooks]]\nname = "first"\n[[hooks]]\nname = "second"\n'
    result = tomllib.loads(render(config, tmp_path / "python.exe", tmp_path, "create"))
    assert len(result["hooks"]) == 2


def test_invalid_dotted_definition_rejected(tmp_path: Path) -> None:
    config = '[mcp_servers]\nqgis = { command = "old" }\n'
    try:
        render(config, tmp_path / "python.exe", tmp_path, "create")
    except ValueError as error:
        assert "mcp_servers.qgis" in str(error)
    else:
        raise AssertionError("Expected a clear failure for unsupported inline MCP definition")


def test_wheel_installer_includes_qgis_mcp(tmp_path: Path) -> None:
    commands = pip_commands(tmp_path / "ryan_functions-test.whl", False)
    assert len(commands) == 3
    assert "nkarasiak/qgis-mcp" in commands[1][-1]
    assert commands[2][-1].endswith(".whl")


def test_wheel_installer_allows_local_qgis_source(tmp_path: Path) -> None:
    commands = pip_commands(
        tmp_path / "ryan_functions-test.whl",
        False,
        qgis_mcp_source=r"Q:\BGER\PER\RPRT\qgis-mcp",
    )
    assert commands[1][-1] == r"Q:\BGER\PER\RPRT\qgis-mcp"


def test_wheel_installer_supports_offline_and_recovery_paths(tmp_path: Path) -> None:
    wheel = tmp_path / "ryan_functions-test.whl"
    assert len(pip_commands(wheel, False, skip_qgis_mcp=True)) == 2
    recovery = pip_commands(wheel, True)
    assert len(recovery) == 1
    assert "--no-deps" in recovery[0]
