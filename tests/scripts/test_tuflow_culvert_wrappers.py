"""Regression tests for migrated TUFLOW culvert wrappers."""

import runpy
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pytest

from ryan_library.functions.culvert.tuflow_engines import TuflowCircularCulvert

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAXIMUMS_SCRIPT = PROJECT_ROOT / "ryan-scripts" / "tuflow" / "tuflow_culvert_from_maximums.py"
NWK_SCRIPT = PROJECT_ROOT / "ryan-scripts" / "tuflow" / "tuflow_culvert_from_1d_nwk.py"


def _maximums_namespace() -> dict[str, Any]:
    return runpy.run_path(str(MAXIMUMS_SCRIPT))


def _nwk_namespace() -> dict[str, Any]:
    return runpy.run_path(str(NWK_SCRIPT))


def test_maximums_mapping_uses_canonical_barrel_count_and_preserves_zero_invert() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "Chan ID": "C01",
            "Flags": "C",
            "Height": 1.2,
            "Length": 30.0,
            "US Invert": 0.0,
            "DS Invert": -0.2,
            "n or Cd": 0.024,
            "Num_barrels": 3,
        }
    )

    assert definition.barrels == 3
    assert definition.inlet_invert_m == pytest.approx(0.0)
    assert definition.outlet_invert_m == pytest.approx(-0.2)


def test_maximums_applies_numeric_blockage_to_circular_diameter() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "Chan ID": "C01",
            "Flags": "C",
            "Height": 1.2,
            "Length": 30.0,
            "US Invert": 10.0,
            "DS Invert": 9.8,
            "n or Cd": 0.024,
            "Num_barrels": 1,
            "pBlockage": 50.0,
        }
    )

    assert definition.diameter_m == pytest.approx(1.2 * (0.5**0.5))


def test_maximums_rejects_unresolved_category_blockage() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="numeric percentage"):
        build_definition(
            {
                "Chan ID": "C01",
                "Flags": "C",
                "Height": 1.2,
                "Length": 30.0,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "Num_barrels": 1,
                "pBlockage": "B",
            }
        )


def test_maximums_mapping_rejects_rectangular_flag() -> None:
    namespace = _maximums_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any]], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="circular 'C' rows only"):
        build_definition(
            {
                "Chan ID": "BOX01",
                "Flags": "R",
                "Height": 1.2,
                "Length": 30.0,
                "US Invert": 10.0,
                "DS Invert": 9.8,
                "n or Cd": 0.013,
                "Num_barrels": 1,
            }
        )


def test_maximums_selection_keeps_governing_row_intact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _maximums_namespace()
    select_rows = cast(
        "Callable[[Path, str, str | None], list[dict[str, Any]]]",
        namespace["_selected_rows"],
    )
    source = pd.DataFrame(
        [
            {
                "Chan ID": "C01",
                "aep_text": "1%",
                "Flags": "C",
                "Height": 1.2,
                "Q": 10.0,
                "US_h": None,
                "DS_h": None,
            },
            {
                "Chan ID": "C01",
                "aep_text": "1%",
                "Flags": "C",
                "Height": 1.2,
                "Q": 9.0,
                "US_h": 11.5,
                "DS_h": 10.7,
            },
        ]
    )

    def read_excel(*_args: object, **_kwargs: object) -> pd.DataFrame:
        return source.copy()

    monkeypatch.setattr(pd, "read_excel", read_excel)

    rows = select_rows(Path("maximums.xlsx"), "Maximums", None)

    assert len(rows) == 1
    assert rows[0]["Q"] == pytest.approx(10.0)
    assert pd.isna(rows[0]["US_h"])
    assert pd.isna(rows[0]["DS_h"])


def test_1d_nwk_skips_ignored_features() -> None:
    namespace = _nwk_namespace()
    select_active_rows = cast(
        "Callable[[list[dict[str, Any]], str | None], list[dict[str, Any]]]",
        namespace["_select_active_rows"],
    )
    rows: list[dict[str, Any]] = [
        {"ID": "IGNORED", "Ignore": "T"},
        {"ID": "ACTIVE", "Ignore": ""},
        {"ID": "IGNORED_Y", "Ignore": "y"},
    ]

    selected = select_active_rows(rows, None)

    assert [row["ID"] for row in selected] == ["ACTIVE"]
    with pytest.raises(ValueError, match="marked ignored"):
        select_active_rows(rows, "IGNORED")


def test_1d_nwk_negative_length_uses_digitized_geometry_length() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Width_or_D": 1.2,
            "Len_or_ANA": -1.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "Number_of": 2,
            "geometry": SimpleNamespace(length=42.5),
        },
        1,
    )

    assert definition.length_m == pytest.approx(42.5)
    assert definition.barrels == 2


def test_1d_nwk_applies_numeric_blockage_to_circular_diameter() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Width_or_D": 1.2,
            "Len_or_ANA": 30.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "n_nF_Cd": 0.013,
            "pBlockage": 50.0,
            "Number_of": 1,
        },
        1,
    )

    assert definition.diameter_m == pytest.approx(1.2 * (0.5**0.5))
    assert definition.inlet_invert_m == pytest.approx(10.0)


def test_1d_nwk_rejects_category_blockage_without_resolved_percentage() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="numeric percentage"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": 10.0,
                "DS_Invert": 9.8,
                "pBlockage": "B",
                "Number_of": 1,
            },
            1,
        )


def test_1d_nwk_zero_number_of_defaults_to_one_barrel() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    definition = build_definition(
        {
            "ID": "C01",
            "Type": "C",
            "Width_or_D": 1.2,
            "Len_or_ANA": 30.0,
            "US_Invert": 10.0,
            "DS_Invert": 9.8,
            "Number_of": 0,
        },
        1,
    )

    assert definition.barrels == 1


def test_1d_nwk_rejects_unresolved_invert_sentinel() -> None:
    namespace = _nwk_namespace()
    build_definition = cast(
        "Callable[[dict[str, Any], int], TuflowCircularCulvert]",
        namespace["_definition"],
    )

    with pytest.raises(ValueError, match="-99999 sentinel"):
        build_definition(
            {
                "ID": "C01",
                "Type": "C",
                "Width_or_D": 1.2,
                "Len_or_ANA": 30.0,
                "US_Invert": -99999,
                "DS_Invert": 9.8,
                "Number_of": 1,
            },
            1,
        )


@pytest.mark.parametrize("script_path", [MAXIMUMS_SCRIPT, NWK_SCRIPT])
def test_wrappers_require_explicit_overwrite(script_path: Path, tmp_path: Path) -> None:
    namespace = runpy.run_path(str(script_path))
    ensure_output = namespace["_ensure_output_available"]
    output = tmp_path / "results.csv"
    output.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        ensure_output(output, overwrite=False)

    ensure_output(output, overwrite=True)


def test_maximums_hy8_workspaces_are_unique_by_aep_and_require_overwrite(tmp_path: Path) -> None:
    namespace = _maximums_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "C01", "1%", "Q @ DS_h TW", overwrite=False)
    second = workspace(tmp_path, "C01", "2%", "Q @ DS_h TW", overwrite=False)

    assert first is not None
    assert second is not None
    assert first != second

    marker = first / "retained.txt"
    marker.write_text("old run", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        workspace(tmp_path, "C01", "1%", "Q @ DS_h TW", overwrite=False)

    replaced = workspace(tmp_path, "C01", "1%", "Q @ DS_h TW", overwrite=True)
    assert replaced == first
    assert not marker.exists()


def test_1d_nwk_hy8_workspace_requires_explicit_overwrite(tmp_path: Path) -> None:
    namespace = _nwk_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "C01", "HW:D = 1.5", overwrite=False)
    assert first is not None

    marker = first / "retained.txt"
    marker.write_text("old run", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        workspace(tmp_path, "C01", "HW:D = 1.5", overwrite=False)

    replaced = workspace(tmp_path, "C01", "HW:D = 1.5", overwrite=True)
    assert replaced == first
    assert not marker.exists()


def test_maximums_workspace_sanitization_is_collision_safe(tmp_path: Path) -> None:
    namespace = _maximums_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "A B", "1%", "Q @ DS_h TW", overwrite=False)
    second = workspace(tmp_path, "A_B", "1%", "Q @ DS_h TW", overwrite=False)

    assert first is not None
    assert second is not None
    assert first != second
    assert first.exists()
    assert second.exists()


def test_1d_nwk_workspace_sanitization_is_collision_safe(tmp_path: Path) -> None:
    namespace = _nwk_namespace()
    workspace = namespace["_workspace"]

    first = workspace(tmp_path, "A B", "HW:D = 1.5", overwrite=False)
    second = workspace(tmp_path, "A_B", "HW:D = 1.5", overwrite=False)

    assert first is not None
    assert second is not None
    assert first != second
    assert first.exists()
    assert second.exists()


@pytest.mark.parametrize("explicit_workspace", [False, True])
def test_native_maximums_does_not_create_hy8_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    explicit_workspace: bool,
) -> None:
    namespace = _maximums_namespace()
    monkeypatch.chdir(tmp_path)
    workbook = tmp_path / "maximums.xlsx"
    pd.DataFrame(
        [
            {
                "Chan ID": "C01",
                "Flags": "C",
                "Height": 1.2,
                "Q": 2.0,
                "US Invert": 10.0,
                "DS Invert": 9.5,
            }
        ]
    ).to_excel(workbook, sheet_name="Maximums", index=False)  # pyright: ignore[reportUnknownMemberType]
    workspace = tmp_path / "explicit-workspace"
    cli = [str(workbook), "--engine", "ryan-culverts", "--keep-workspace"]
    if explicit_workspace:
        cli.extend(["--workspace", str(workspace)])
    args = namespace["_parser"]().parse_args(cli)
    assert namespace["run"](args) == 0
    assert args.output_csv.exists()
    assert not workspace.exists()
    assert not (tmp_path / "hy8-workspaces").exists()
