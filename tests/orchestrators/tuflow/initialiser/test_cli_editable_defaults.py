"""Editable initialiser CLI settings and explicit command-line precedence."""

# These tests intentionally exercise the CLI parser and precedence helpers.
# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.initialiser import cli


def test_configured_init_defaults_and_explicit_false_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "OUTPUT_PATH", Path("new-project"))
    monkeypatch.setattr(cli, "PROJECT_NAME", "Test")
    monkeypatch.setattr(cli, "SCENARIO_NAME", "EXG")
    monkeypatch.setattr(cli, "PROJECTION_FILE", Path("projection.prj"))
    monkeypatch.setattr(cli, "TUFLOW_EXECUTABLE", Path("tuflow.exe"))
    monkeypatch.setattr(cli, "OVERWRITE", True)
    monkeypatch.setattr(cli, "COPY_UTILITIES", True)
    args = cli._parser().parse_args(["init", "--no-overwrite", "--no-copy-utilities"])
    assert cli._configured(args.output, cli.OUTPUT_PATH) == Path("new-project")
    assert cli._configured(args.overwrite, cli.OVERWRITE) is False
    assert cli._configured(args.copy_utilities, cli.COPY_UTILITIES) is False


def test_list_inputs_are_editable_and_cli_overrides_instead_of_appending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "TERRAIN_RASTERS", [Path("lidar.tif"), Path("srtm.tif")])
    monkeypatch.setattr(cli, "AOI_FILE", Path("aoi.gpkg"))
    monkeypatch.setattr(cli, "OUTPUT_PATH", Path("new-project"))
    monkeypatch.setattr(cli, "PROJECT_NAME", "Test")
    monkeypatch.setattr(cli, "SCENARIO_NAME", "EXG")
    monkeypatch.setattr(cli, "PROJECTION_FILE", Path("projection.prj"))
    monkeypatch.setattr(cli, "TUFLOW_EXECUTABLE", Path("tuflow.exe"))
    monkeypatch.setattr(cli, "RAINFALL_NAME", "RF")
    parser = cli._parser()
    defaults = parser.parse_args(["build"])
    override = parser.parse_args(["build", "--terrain", "alternative.tif", "--no-copy-utilities"])
    assert cli._configured(defaults.copy_utilities, cli.COPY_UTILITIES) is True
    assert cli._configured(override.copy_utilities, cli.COPY_UTILITIES) is False
    assert cli._configured(defaults.terrain, cli.TERRAIN_RASTERS) == [Path("lidar.tif"), Path("srtm.tif")]
    assert cli._configured(override.terrain, cli.TERRAIN_RASTERS) == [Path("alternative.tif")]
