"""Regression tests for Codex MCP bootstrap and wheel installation integration."""

from __future__ import annotations

import runpy
import shutil
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

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
        "[features]\nfoo = true\n"
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
    assert value["mcp_servers"]["qgis"]["env"]["QGIS_MCP_TOKEN"].startswith("keep")
    assert value["mcp_servers"]["ryan-tools"]["env"]["RYAN_MCP_PROFILE"] == "analysis"


def test_existing_array_tables_survive(tmp_path: Path) -> None:
    config = '[[hooks]]\nname = "first"\n[[hooks]]\nname = "second"\n'
    result = tomllib.loads(render(config, tmp_path / "python.exe", tmp_path, "create"))
    assert len(result["hooks"]) == 2


def test_invalid_dotted_definition_rejected(tmp_path: Path) -> None:
    config = '[mcp_servers]\nqgis = { command = "old" }\n'
    with pytest.raises(ValueError, match=r"mcp_servers\.qgis"):
        render(config, tmp_path / "python.exe", tmp_path, "create")


def test_wheel_installer_includes_qgis_mcp(tmp_path: Path) -> None:
    commands = pip_commands(tmp_path / "ryan_functions-test.whl", False)
    assert len(commands) == 3
    assert "nkarasiak/qgis-mcp" in commands[1][-1]
    assert commands[2][-1].endswith(".whl[mcp]")


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


def test_pinned_upstream_qgis_source_is_consistent() -> None:
    upstream = cast(str, installer["QGIS_MCP_ARCHIVE"])
    codex_source = cast(str, codex["QGIS_MCP_SOURCE"])
    requirements = (REPO / "requirements.txt").read_text(encoding="utf-8")
    assert upstream == codex_source
    assert upstream in requirements


def test_inline_root_mcp_servers_preserved_and_idempotent(tmp_path: Path) -> None:
    original = 'model = "gpt-6"\nmcp_servers = { other = { command = "other-tool" } }\n'
    first = render(original, tmp_path / "python.exe", tmp_path, "create")
    second = render(first, tmp_path / "python.exe", tmp_path, "create")
    assert first == second
    data = tomllib.loads(first)
    assert data["model"] == "gpt-6"
    assert data["mcp_servers"]["other"]["command"] == "other-tool"
    assert data["mcp_servers"]["qgis"]["args"] == ["-B", "-m", "qgis_mcp.server"]
    assert data["mcp_servers"]["ryan-tools"]["args"] == ["-B", "-m", "ryan_library.mcp.server"]


def test_inline_root_existing_registration_can_be_updated(tmp_path: Path) -> None:
    original = 'mcp_servers = { qgis = { command = "old" }, other = { command = "keep" } }\n'
    rendered = render(original, tmp_path / "python.exe", tmp_path, "analysis")
    data = tomllib.loads(rendered)["mcp_servers"]
    assert data["qgis"]["command"] == str(tmp_path / "python.exe")
    assert data["other"]["command"] == "keep"
    assert data["ryan-tools"]["env"]["RYAN_MCP_PROFILE"] == "analysis"


def test_pypi_installer_commands_use_distribution_not_local_wheel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "path", [str(REPO / "repo-scripts"), *sys.path])
    pypi = runpy.run_path(str(REPO / "repo-scripts" / "install_from_pypi.py"))
    commands_fn = cast(Callable[..., list[list[str]]], pypi["pip_commands"])
    commands = commands_fn()
    assert "fiona" in commands[0]
    assert "nkarasiak/qgis-mcp" in commands[1][-1]
    assert commands[-1][-1] == "ryan_functions[mcp]"
    assert all(not any(".whl" in argument for argument in command) for command in commands)
    assert len(commands_fn(skip_qgis_mcp=True)) == 2


def test_pypi_installer_detects_existing_codex_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_codex(_command: str) -> str | None:
        return None

    monkeypatch.setattr(sys, "path", [str(REPO / "repo-scripts"), *sys.path])
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    monkeypatch.setattr(shutil, "which", missing_codex)
    pypi = runpy.run_path(str(REPO / "repo-scripts" / "install_from_pypi.py"))
    detected = cast(Callable[[], bool], pypi["codex_installed"])
    assert not detected()
    (tmp_path / "config.toml").write_text("", encoding="utf-8")
    assert detected()
