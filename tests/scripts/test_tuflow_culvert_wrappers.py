"""Regression tests for migrated TUFLOW culvert wrappers."""

from collections.abc import Callable
from pathlib import Path
import runpy
from typing import Any, cast

import pandas as pd
import pytest

from ryan_library.functions.culvert.tuflow_engines import TuflowCircularCulvert

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAXIMUMS_SCRIPT = PROJECT_ROOT / "ryan-scripts" / "tuflow" / "tuflow_culvert_from_maximums.py"


def _maximums_namespace() -> dict[str, Any]:
    return runpy.run_path(str(MAXIMUMS_SCRIPT))


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
    monkeypatch.setattr(pd, "read_excel", lambda *_args, **_kwargs: source.copy())

    rows = select_rows(Path("maximums.xlsx"), "Maximums", None)

    assert len(rows) == 1
    assert rows[0]["Q"] == pytest.approx(10.0)
    assert pd.isna(rows[0]["US_h"])
    assert pd.isna(rows[0]["DS_h"])
