"""Console regression tests for the standalone GeoPackage layer renamer."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
from colorama import Fore

SCRIPT = (
    Path(__file__).absolute().parents[3]
    / "ryan-scripts"
    / "TUFLOW-python"
    / "gis_processing"
    / "rename_geopackage_layer_to_filename.py"
)


@pytest.fixture
def renamer() -> ModuleType:
    pytest.importorskip("geopandas")
    pytest.importorskip("pyogrio")
    spec = importlib.util.spec_from_file_location("rename_geopackage_layer_to_filename_tested", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_initialiser_uses_absolute_scan_root_and_distinct_colours(
    renamer: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(renamer, "init", lambda: None)
    root, success, skipped, failed = renamer.initialize(str(tmp_path))
    assert root == tmp_path.absolute()
    assert (success, skipped, failed) == (Fore.GREEN, Fore.YELLOW, Fore.RED)
    assert len({success, skipped, failed}) == 3


def test_console_paths_are_relative_and_failures_distinct(
    renamer: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "model gis"
    nested = root / "nested"
    nested.mkdir(parents=True)
    for filename in (
        root / "correct.gpkg",
        nested / "rename.gpkg",
        nested / "multiple.gpkg",
        root / "inspect_error.gpkg",
        nested / "read_error.gpkg",
        root / "empty.gpkg",
    ):
        filename.touch()

    def list_layers(path: str) -> list[list[str]]:
        stem = Path(path).stem
        if stem == "inspect_error":
            message = "inspection error"
            raise OSError(message)
        if stem == "empty":
            return []
        if stem == "multiple":
            return [["first", "Polygon"], ["second", "Polygon"]]
        return [[stem if stem == "correct" else "old_name", "Polygon"]]

    reads: list[str] = []
    replacements: list[tuple[str, str]] = []
    writes: list[tuple[str, str]] = []

    class FakeGeoDataFrame:
        def to_file(self, *, filename: str, layer: str, driver: str) -> None:
            assert driver == "GPKG"
            writes.append((filename, layer))

    def read_file(*, filename: str) -> FakeGeoDataFrame:
        reads.append(filename)
        if Path(filename).stem == "read_error":
            message = "read error"
            raise OSError(message)
        return FakeGeoDataFrame()

    def move(*, src: str, dst: str) -> None:
        replacements.append((src, dst))

    monkeypatch.setattr(renamer.pyogrio, "list_layers", list_layers)
    monkeypatch.setattr(renamer.gpd, "read_file", read_file)
    monkeypatch.setattr(renamer.shutil, "move", move)
    monkeypatch.setattr(renamer, "SEARCH_RECURSIVELY", True)

    renamer.process_geopackage_files(
        base_dir=root, success_colour=Fore.GREEN, skip_colour=Fore.YELLOW, fail_colour=Fore.RED
    )
    out = capsys.readouterr().out

    assert f"{Fore.GREEN}Successfully renamed layer in {Path('nested') / 'rename.gpkg'} to rename" in out
    assert f"{Fore.YELLOW}Skipping correct.gpkg as the layer name is already correct." in out
    assert f"{Fore.YELLOW}Skipping {Path('nested') / 'multiple.gpkg'} as it contains multiple layers:" in out
    assert f"{Fore.RED}Failed to inspect inspect_error.gpkg: inspection error" in out
    assert f"{Fore.RED}Failed to inspect empty.gpkg: no layers found." in out
    assert f"{Fore.RED}Failed to process {Path('nested') / 'read_error.gpkg'}: read error" in out
    assert str(root.absolute()) not in out
    assert str(nested / "rename.gpkg") in reads
    assert str(nested / "read_error.gpkg") in reads
    assert len(writes) == 1
    assert writes[0][1] == "rename"
    assert len(replacements) == 1
    assert replacements[0][1] == str(nested / "rename.gpkg")
