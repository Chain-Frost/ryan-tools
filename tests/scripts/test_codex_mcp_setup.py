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
    assert servers["qgis"]["args"] == ["-m", "qgis_mcp.server"]
    assert servers["ryan-tools"]["args"] == ["-m", "ryan_library.mcp.server"]
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
    commands = pip_commands(tmp_path / "ryan_tools-test.whl", False)
    assert len(commands) == 3
    assert "nkarasiak/qgis-mcp" in commands[1][-1]
    assert commands[2][-1].endswith(".whl[mcp]")


def test_wheel_installer_allows_local_qgis_source(tmp_path: Path) -> None:
    commands = pip_commands(
        tmp_path / "ryan_tools-test.whl",
        False,
        qgis_mcp_source=r"Q:\BGER\PER\RPRT\qgis-mcp",
    )
    assert commands[1][-1] == r"Q:\BGER\PER\RPRT\qgis-mcp"


def test_wheel_installer_supports_offline_and_recovery_paths(tmp_path: Path) -> None:
    wheel = tmp_path / "ryan_tools-test.whl"
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
    assert data["mcp_servers"]["qgis"]["args"] == ["-m", "qgis_mcp.server"]
    assert data["mcp_servers"]["ryan-tools"]["args"] == ["-m", "ryan_library.mcp.server"]


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
    assert commands[-1][-1] == "ryan-tools[mcp]"
    assert all(not any(".whl" in argument for argument in command) for command in commands)
    assert len(commands_fn(skip_qgis_mcp=True)) == 2


def test_pypi_installer_detects_existing_codex_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_codex(_command: str) -> str | None:
        return None

    def isolated_home(_cls: type[Path]) -> Path:
        return tmp_path

    monkeypatch.setattr(sys, "path", [str(REPO / "repo-scripts"), *sys.path])
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("VSCODE_EXTENSIONS", str(tmp_path / "extensions"))
    monkeypatch.setattr(Path, "home", classmethod(isolated_home))
    monkeypatch.setattr(shutil, "which", missing_codex)
    pypi = runpy.run_path(str(REPO / "repo-scripts" / "install_from_pypi.py"))
    detected = cast(Callable[[], bool], pypi["codex_installed"])
    assert not detected()
    (tmp_path / "config.toml").write_text("", encoding="utf-8")
    assert detected()


def test_pypi_installer_python_support_is_open_ended(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", [str(REPO / "repo-scripts"), *sys.path])
    pypi = runpy.run_path(str(REPO / "repo-scripts" / "install_from_pypi.py"))
    supported = cast(Callable[[int, int], bool], pypi["supported_python"])
    assert not supported(3, 12)
    assert supported(3, 13)
    assert supported(3, 14)
    assert supported(3, 15)
    assert supported(3, 16)


def test_pypi_batch_launcher_accepts_supported_versions() -> None:
    script = (REPO / "install-latest-wheel.bat").read_text(encoding="utf-8")
    assert 'set "PYTHON_CMD=py -3"' in script
    assert "RYAN_TOOLS_PYTHON" in script
    assert "sys.version_info >= (3, 13)" in script
    assert "py -3.14" not in script


def test_vscode_codex_extension_detected_without_codex_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_executable(_command: str) -> str | None:
        return None

    extensions = tmp_path / "vscode-extensions"
    extensions.mkdir()
    monkeypatch.setenv("VSCODE_EXTENSIONS", str(extensions))
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    monkeypatch.setattr(shutil, "which", missing_executable)
    monkeypatch.setattr(sys, "path", [str(REPO / "repo-scripts"), *sys.path])
    pypi = runpy.run_path(str(REPO / "repo-scripts" / "install_from_pypi.py"))
    detected = cast(Callable[[], bool], pypi["codex_installed"])
    (extensions / "openai.chatgpt-26.1").mkdir()
    assert detected()


@pytest.mark.parametrize("disabled", [False, True])
def test_bytecode_policy_updates_existing_registrations(tmp_path: Path, disabled: bool) -> None:
    render_policy = cast(Callable[..., str], codex["render_config"])
    original = (
        '[mcp_servers.qgis]\ncommand = "old"\nargs = ["-B", "-m", "old"]\n'
        '[mcp_servers.qgis.env]\nPYTHONDONTWRITEBYTECODE = "1"\nQGIS_MCP_TOKEN = "keep"\n'
    )
    result = render_policy(original, tmp_path / "python.exe", tmp_path, "create", disable_bytecode=disabled)
    servers = tomllib.loads(result)["mcp_servers"]
    for server in servers.values():
        assert ("-B" in server["args"]) is disabled
        assert (server["env"].get("PYTHONDONTWRITEBYTECODE") == "1") is disabled
    assert servers["qgis"]["env"]["QGIS_MCP_TOKEN"].startswith("keep")
    assert render_policy(result, tmp_path / "python.exe", tmp_path, "create", disable_bytecode=disabled) == result


@pytest.mark.parametrize(
    ("dns_domain", "domain", "fqdn", "expected"),
    [
        ("", "", "home.local", False),
        ("BGE-RESOURCES.COM", "", "home.local", True),
        ("", "BGER", "home.local", True),
        ("", "", "desktop.bge-resources.com", True),
        ("", "", "desktop.bge-resources.com.example.org", False),
    ],
)
def test_bge_bytecode_detection(
    monkeypatch: pytest.MonkeyPatch, dns_domain: str, domain: str, fqdn: str, expected: bool
) -> None:
    import socket

    monkeypatch.setenv("USERDNSDOMAIN", dns_domain)
    monkeypatch.setenv("USERDOMAIN", domain)
    monkeypatch.setattr(socket, "getfqdn", lambda: fqdn)
    detect = cast(Callable[[], bool], codex["disable_bytecode_for_environment"])
    assert detect() is expected


def test_pypi_installer_bytecode_option(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", [str(REPO / "repo-scripts"), *sys.path])
    pypi = runpy.run_path(str(REPO / "repo-scripts" / "install_from_pypi.py"))
    commands = cast(Callable[..., list[list[str]]], pypi["pip_commands"])
    assert all("-B" not in command for command in commands())
    assert all("-B" in command for command in commands(disable_bytecode=True))


def test_default_setup_removes_checkout_requirement(tmp_path: Path) -> None:
    render_installed = cast(Callable[..., str], codex["render_config"])
    original = '[mcp_servers.ryan-tools]\ncommand = "old"\n[mcp_servers.ryan-tools.env]\nRYAN_TOOLS_REPOSITORY_ROOT = "old-checkout"\nCUSTOM = "keep"\n'
    result = tomllib.loads(render_installed(original, tmp_path / "python.exe", None, "create"))
    environment = result["mcp_servers"]["ryan-tools"]["env"]
    assert "RYAN_TOOLS_REPOSITORY_ROOT" not in environment
    assert environment["CUSTOM"] == "keep"


@pytest.mark.parametrize("dry_run", [False, True])
def test_legacy_distribution_requires_migration(monkeypatch: pytest.MonkeyPatch, dry_run: bool) -> None:
    def installed_distribution(_name: str) -> object:
        return object()

    check = cast(Callable[..., bool], installer["legacy_installation_blocks_upgrade"])
    monkeypatch.setitem(check.__globals__, "distribution", installed_distribution)
    assert check(dry_run=dry_run) is (not dry_run)


@pytest.mark.parametrize("key", ['"mcp_servers"', "'mcp_servers'", '"mcp_\\u0073ervers"'])
def test_quoted_inline_root_key_is_preserved(tmp_path: Path, key: str) -> None:
    original = f'model = "gpt-6"\n{key} = {{ other = {{ command = "tool" }} }}\n'
    first = render(original, tmp_path / "python.exe", None, "create")
    second = render(first, tmp_path / "python.exe", None, "create")
    assert second == first
    assert f"{key} =" in first
    decoded = tomllib.loads(first)
    assert decoded["model"] == "gpt-6"
    assert decoded["mcp_servers"]["other"]["command"] == "tool"
    assert decoded["mcp_servers"]["qgis"]["command"] == str(tmp_path / "python.exe")


def test_codex_writer_rejects_edits_after_read(tmp_path: Path) -> None:
    write = cast(Callable[..., None], codex["_write"])
    config = tmp_path / "config.toml"
    initially_read = b'model = "older"\n'
    config.write_bytes(initially_read)
    config.write_text('model = "newer"\n', encoding="utf-8")
    with pytest.raises(RuntimeError, match="changed since setup read"):
        write(config, 'model = "stale"\n', dry_run=False, expected_original=initially_read)
    assert config.read_text(encoding="utf-8") == 'model = "newer"\n'
    assert not list(tmp_path.glob("*.bak.*"))


def test_codex_writer_detects_edits_during_staged_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    write = cast(Callable[..., None], codex["_write"])
    config = tmp_path / "config.toml"
    original = b'model = "before"\n'
    config.write_bytes(original)
    actual_fdopen = os.fdopen

    def concurrent_change(fd: int, mode: str):
        config.write_text('model = "concurrent"\n', encoding="utf-8")
        return actual_fdopen(fd, mode)

    monkeypatch.setattr(os, "fdopen", concurrent_change)
    with pytest.raises(RuntimeError, match="changed during setup"):
        write(config, 'model = "stale"\n', dry_run=False, expected_original=original)
    assert config.read_text(encoding="utf-8") == 'model = "concurrent"\n'


def test_codex_writer_refuses_symlink_without_replacing_it(tmp_path: Path) -> None:
    write = cast(Callable[..., None], codex["_write"])
    target = tmp_path / "managed-config.toml"
    link = tmp_path / "config.toml"
    target.write_text('model = "managed"\n', encoding="utf-8")
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("Creating symbolic links is not permitted in this environment")
    with pytest.raises(ValueError, match="symlink"):
        write(link, 'model = "unsafe"\n', dry_run=False, expected_original=target.read_bytes())
    assert link.is_symlink()
    assert target.read_text(encoding="utf-8") == 'model = "managed"\n'


def test_codex_writer_is_idempotent_for_unchanged_bytes(tmp_path: Path) -> None:
    write = cast(Callable[..., None], codex["_write"])
    config = tmp_path / "config.toml"
    original = b'model = "kept"\n'
    config.write_bytes(original)
    write(config, original.decode(), dry_run=False, expected_original=original)
    assert config.read_bytes() == original
    assert not list(tmp_path.glob("*.bak.*"))
