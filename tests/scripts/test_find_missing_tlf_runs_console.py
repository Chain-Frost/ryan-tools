"""Console formatting regression for successive missing TLF scenario tables."""

from __future__ import annotations

import runpy
from pathlib import Path
from typing import Callable, cast

import pytest

from ryan_library.functions.tlf_ensemble_inventory import EnsembleInventory, TPCountTable

_WRAPPER = Path(__file__).absolute().parents[2] / "ryan-scripts/TUFLOW-python/model_management/find_missing_tlf_runs.py"


def test_successive_scenario_tables_have_one_blank_line_without_changing_summaries(
    capsys: pytest.CaptureFixture[str],
) -> None:
    namespace = runpy.run_path(str(_WRAPPER))
    show = cast(Callable[[EnsembleInventory], None], namespace["_show_inventory"])
    tables = (
        TPCountTable("Scenario A", ("00.2p",), ("01800m",), ((9,),), 10, ("00.2p: 01800m TP01",)),
        TPCountTable("Scenario B", ("00.2p",), ("01800m",), ((10,),), 10),
        TPCountTable("Scenario C", ("00.2p",), ("01800m",), ((8,),), 10, ("00.2p: 01800m TP01-02",)),
    )
    inventory = EnsembleInventory(
        log_count=3,
        run_count=3,
        expected_count=30,
        family_count=1,
        missing=(),
        tp_tables=tables,
    )

    show(inventory)
    content = capsys.readouterr().out
    assert content.count("Missing: ") == 3
    assert "Missing: 00.2p: 01800m TP01\n\n" in content
    assert "Missing: none\n\n" in content
    assert "Missing: 00.2p: 01800m TP01-02\n" in content
    assert "Scenario A" in content
    assert "Scenario B" in content
    assert "Scenario C" in content
    assert content.index("Missing: 00.2p: 01800m TP01") < content.index("Scenario B")
    assert content.index("Missing: none") < content.index("Scenario C")
    assert inventory.tp_tables == tables
