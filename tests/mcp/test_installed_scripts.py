"""Installed-script allowlist, catalogue coverage and safe copying regression tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ryan_library.mcp.installed_scripts import available_wrappers, copy_wrapper, installed_files, main

ROOT = Path(__file__).absolute().parents[2]


def test_manifest_is_an_explicit_selection_not_entire_source_tree() -> None:
    selected = installed_files()
    assert selected == tuple(sorted(set(selected)))
    assert "README.md" in selected
    assert "culvert.py" in selected
    assert "floodway.py" in selected
    assert "python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py" not in selected
    assert "unsorted-python/plot_water_level_profiles.py" not in selected
    assert "gdal-bat/gdal_merge.bat" not in selected
    assert "misc-python/remove_excel_protection.py" in selected  # Existing privileged MCP workflow.
    assert len(selected) < len([p for p in (ROOT / "ryan-scripts").rglob("*") if p.is_file()])
    for script in selected:
        assert ".." not in Path(script).parts
        assert (ROOT / "ryan-scripts" / script).is_file(), script


def test_manifest_covers_all_script_target_workflows() -> None:
    resources = ROOT / "ryan_library" / "resources" / "mcp"
    general = json.loads((resources / "workflows.json").read_text(encoding="utf-8"))
    gdal = json.loads((resources / "gdal_cli_tools.json").read_text(encoding="utf-8"))
    expected = {
        item["script"].removeprefix("ryan-scripts/")
        for item in general["workflows"]
        if "script" in item
    }
    expected.update(
        "gdal-python/" + item["script"]
        for item in gdal["tools"]
        if "module" not in item
    )
    assert expected <= set(installed_files())


def test_copy_is_byte_identical_and_does_not_overwrite(tmp_path: Path) -> None:
    script = "file-management-python/archive_compress_folder.py"
    source = ROOT / "ryan-scripts" / script
    assert script in available_wrappers()
    target = tmp_path / Path(script).name
    assert copy_wrapper(script, tmp_path) == target
    assert target.read_bytes() == source.read_bytes()

    with pytest.raises(FileExistsError, match="--overwrite"):
        copy_wrapper(script, tmp_path)

    target.write_bytes(b"modified project copy")
    assert copy_wrapper(script, tmp_path, dry_run=True, overwrite=True) == target
    assert target.read_bytes() == b"modified project copy"
    copy_wrapper(script, tmp_path, overwrite=True)
    assert target.read_bytes() == source.read_bytes()


def test_copy_refuses_unlisted_scripts_and_nonexistent_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not in the installed"):
        copy_wrapper("python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py", tmp_path)
    with pytest.raises(NotADirectoryError):
        copy_wrapper("culvert.py", tmp_path / "missing")


def test_cli_list_and_copy_dry_run(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["list"]) == 0
    assert "culvert.py" in capsys.readouterr().out
    assert main(["copy", "culvert.py", str(tmp_path), "--dry-run"]) == 0
    assert not (tmp_path / "culvert.py").exists()
    assert "Would copy to:" in capsys.readouterr().out
