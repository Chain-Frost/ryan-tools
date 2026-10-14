"""Focused synthetic raster verification tests."""

# Rasterio has no distributed typing stubs (same convention as other repo raster tests).
# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

import csv
import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "ryan-scripts"
    / "TUFLOW-python"
    / "model_management"
    / "verify_converted_tuflow_rasters.py"
)


@pytest.fixture
def tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_converted_tuflow_rasters_tested", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def make_pair(
    directory: Path,
    suffix: str = ".asc",
    *,
    source_values: np.ndarray | None = None,
    tif_values: np.ndarray | None = None,
    newer: bool = True,
    nodata: int = -9999,
    tif_nodata: int | None = None,
    offset: int = 0,
    crs: str | None = "EPSG:28350",
) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    src = directory / f"Depth{suffix}"
    tif = directory / "Depth.tif"
    array = np.array([[1, 2], [3, nodata]], dtype="float32") if source_values is None else source_values
    arr_tif = array if tif_values is None else tif_values
    transform = from_origin(100, 200, 5, 5)
    with rasterio.open(
        src,
        "w",
        driver="AAIGrid" if suffix == ".asc" else "EHdr",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        transform=transform,
        crs=crs,
        nodata=nodata,
    ) as dst:
        dst.write(array, 1)
    with rasterio.open(
        tif,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        transform=from_origin(100 + offset, 200, 5, 5),
        crs=crs,
        nodata=nodata if tif_nodata is None else tif_nodata,
    ) as dst:
        dst.write(arr_tif, 1)
    os.utime(src, ns=(1_000_000_000, 1_000_000_000))
    if suffix == ".flt":
        os.utime(src.with_suffix(".hdr"), ns=(1_000_000_000, 1_000_000_000))
    os.utime(tif, ns=((2 if newer else 1) * 1_000_000_000, (2 if newer else 1) * 1_000_000_000))
    return src, tif


def report_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_asc_match_and_report_no_deletion(tool: ModuleType, tmp_path: Path) -> None:
    src, tif = make_pair(tmp_path / "nested")
    assert tool.main(["--root", str(tmp_path), "--no-progress"]) == 0
    assert src.exists()
    assert tif.exists()
    assert report_rows(tmp_path / "tuflow_raster_conversion_delete_candidates.csv") == [
        {"delete_path": str(src), "retained_tif": str(tif), "type": "raster"}
    ]
    assert not report_rows(tmp_path / "tuflow_raster_conversion_errors.csv")


def test_flt_match_includes_header_but_not_projection(tool: ModuleType, tmp_path: Path) -> None:
    src, tif = make_pair(tmp_path, suffix=".flt")
    hdr = src.with_suffix(".hdr")
    assert hdr.is_file()
    assert tool.main(["--root", str(tmp_path), "--no-progress"]) == 0
    rows = report_rows(tmp_path / "tuflow_raster_conversion_delete_candidates.csv")
    assert {row["delete_path"] for row in rows} == {str(src), str(hdr)}
    assert tif.exists()


def test_mismatch_and_missing_tif_reported(tool: ModuleType, tmp_path: Path) -> None:
    src, _tif = make_pair(tmp_path / "bad", tif_values=np.array([[1, 20], [3, -9999]], dtype="float32"))
    missing = tmp_path / "missing" / "unknown.asc"
    missing.parent.mkdir()
    missing.write_text("dummy", encoding="utf-8")
    assert tool.main(["--root", str(tmp_path), "--no-progress"]) == 1
    errors = report_rows(tmp_path / "tuflow_raster_conversion_errors.csv")
    assert {row["status"] for row in errors} == {"MISMATCH", "MISSING_OR_AMBIGUOUS_TIF"}
    assert any("row 0, column 1" in row["reason"] for row in errors)
    assert report_rows(tmp_path / "tuflow_raster_conversion_delete_candidates.csv") == []
    assert src.exists()


@pytest.mark.parametrize("reason", ["older", "transform", "nodata", "crs"])
def test_metadata_blocks_candidates(tool: ModuleType, tmp_path: Path, reason: str) -> None:
    src, tif = make_pair(
        tmp_path,
        newer=reason != "older",
        offset=1 if reason == "transform" else 0,
        tif_nodata=-8888 if reason == "nodata" else None,
    )
    if reason == "crs":
        with rasterio.open(tif, "r+") as dst:
            dst.crs = "EPSG:4326"
    if reason == "crs":
        os.utime(tif, ns=(2_000_000_000, 2_000_000_000))
    result = tool.inspect_source(src, tmp_path, tmp_path)
    assert not result.eligible


def decline_delete(_prompt: str) -> str:
    return "CANCEL"


def approve_delete(_prompt: str) -> str:
    return "DELETE"


def test_delete_requires_explicit_confirmation(
    tool: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src, tif = make_pair(tmp_path)
    monkeypatch.setattr("builtins.input", decline_delete)
    assert tool.main(["--root", str(tmp_path), "--no-progress", "--delete"]) == 1
    assert src.exists()
    assert tif.exists()
    monkeypatch.setattr("builtins.input", approve_delete)
    assert tool.main(["--root", str(tmp_path), "--no-progress", "--delete"]) == 0
    assert not src.exists()
    assert tif.exists()


def test_changed_tif_or_source_prevents_deletion(tool: ModuleType, tmp_path: Path) -> None:
    src, tif = make_pair(tmp_path)
    result = tool.inspect_source(src, tmp_path, tmp_path)
    assert result.eligible
    tif.write_bytes(b"changed")
    assert tool.perform_deletions([result]) == (0, 1)
    assert src.exists()


def test_unrelated_crs_sentinel_mask_matches(tool: ModuleType, tmp_path: Path) -> None:
    src, _tif = make_pair(tmp_path, tif_values=np.array([[1, 2], [3, -8888]], dtype="float32"), tif_nodata=-8888)
    result = tool.inspect_source(src, tmp_path, tmp_path)
    assert result.eligible


def test_reject_ambiguous_tif(tool: ModuleType, tmp_path: Path) -> None:
    src, _tif = make_pair(tmp_path)
    tif2 = tmp_path / "Depth.tiff"
    tif2.write_bytes(b"dummy")
    result = tool.inspect_source(src, tmp_path, tmp_path)
    assert result.status == "MISSING_OR_AMBIGUOUS_TIF"


def test_report_names_do_not_overwrite(tool: ModuleType, tmp_path: Path) -> None:
    make_pair(tmp_path)
    assert tool.main(["--root", str(tmp_path), "--no-progress"]) == 0
    assert tool.main(["--root", str(tmp_path), "--no-progress"]) == 0
    assert (tmp_path / "tuflow_raster_conversion_delete_candidates_001.csv").exists()
    assert (tmp_path / "tuflow_raster_conversion_errors_001.csv").exists()


def test_mirrored_tif_root(tool: ModuleType, tmp_path: Path) -> None:
    src, tif = make_pair(tmp_path / "sources" / "day1")
    dst = tmp_path / "converted" / "day1" / tif.name
    dst.parent.mkdir(parents=True)
    tif.replace(dst)
    assert (
        tool.main(["--root", str(tmp_path / "sources"), "--tif-root", str(tmp_path / "converted"), "--no-progress"])
        == 0
    )
    assert src.exists()
    assert dst.exists()


def test_header_must_also_precede_tif(tool: ModuleType, tmp_path: Path) -> None:
    src, tif = make_pair(tmp_path, suffix=".flt")
    hdr = src.with_suffix(".hdr")
    os.utime(hdr, ns=(3_000_000_000, 3_000_000_000))
    result = tool.inspect_source(src, tmp_path, tmp_path)
    assert result.status == "TIF_NOT_NEWER"
    assert src.exists()
    assert tif.exists()


def test_broken_tif_is_reported(tool: ModuleType, tmp_path: Path) -> None:
    src, tif = make_pair(tmp_path)
    tif.write_bytes(b"invalid TIFF")
    result = tool.inspect_source(src, tmp_path, tmp_path)
    assert result.status == "ERROR"
    assert src.exists()


def test_untagged_crs_is_excluded(tool: ModuleType, tmp_path: Path) -> None:
    src, _tif = make_pair(tmp_path, crs=None)
    result = tool.inspect_source(src, tmp_path, tmp_path)
    assert result.status == "MISMATCH"
    assert "CRS" in result.reason


def create_directory_symlink(target: Path, link: Path) -> None:
    """Make a real directory link, skipping where Windows permissions forbid it."""
    try:
        link.symlink_to(target, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Directory symlinks unavailable: {exc}")


@pytest.mark.parametrize("linked_input", ["source_root", "tif_root"])
def test_cli_rejects_directory_symlink_before_scanning(tool: ModuleType, tmp_path: Path, linked_input: str) -> None:
    root = tmp_path / "source"
    src, tif = make_pair(root)
    link = tmp_path / "linked"
    create_directory_symlink(root, link)
    arguments = ["--root", str(link if linked_input == "source_root" else root), "--delete", "--no-progress"]
    if linked_input == "tif_root":
        arguments.extend(["--tif-root", str(link)])
    with pytest.raises(SystemExit) as error:
        tool.main(arguments)
    assert error.value.code == 2
    assert src.exists()
    assert tif.exists()


def test_cli_rejects_symlink_in_parent_directory(tool: ModuleType, tmp_path: Path) -> None:
    directory = tmp_path / "real" / "nested"
    src, tif = make_pair(directory)
    link = tmp_path / "alias"
    create_directory_symlink(directory.parent, link)
    root = link / "nested"
    assert not root.is_symlink()  # The directory itself is real; its parent is linked.
    with pytest.raises(SystemExit) as error:
        tool.main(["--root", str(root), "--delete", "--no-progress"])
    assert error.value.code == 2
    assert src.exists()
    assert tif.exists()


def test_scanner_does_not_follow_linked_source_folder(tool: ModuleType, tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    src, _tif = make_pair(outside)
    create_directory_symlink(outside, root / "linked")
    assert tool.discover_sources(root) == []
    assert src.exists()


def test_mirrored_tif_path_cannot_pass_through_link(tool: ModuleType, tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    src, tif = make_pair(source_root / "nested")
    converted = tmp_path / "converted"
    converted.mkdir()
    copy_target = tmp_path / "tif_target"
    copy_target.mkdir()
    tif.replace(copy_target / tif.name)
    create_directory_symlink(copy_target, converted / "nested")
    result = tool.inspect_source(src, source_root, converted)
    assert not result.eligible
    assert "symlink" in result.reason
    assert src.exists()


def test_symlinked_parent_introduced_after_verification_blocks_deletion(tool: ModuleType, tmp_path: Path) -> None:
    folder = tmp_path / "original"
    src, tif = make_pair(folder)
    result = tool.inspect_source(src, folder, folder)
    assert result.eligible
    preserved = tmp_path / "preserved"
    folder.rename(preserved)
    create_directory_symlink(preserved, folder)
    assert tool.perform_deletions([result]) == (0, 1)
    assert (preserved / src.name).exists()
    assert (preserved / tif.name).exists()
