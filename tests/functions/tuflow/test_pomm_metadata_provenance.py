"""Regression test for the PyPI distribution name in POMM report provenance."""

# Tests deliberately call the internal metadata builder to validate the report contract.
# pyright: reportPrivateUsage=false

from pandas import DataFrame

from ryan_library.functions.tuflow.pomm_utils import _build_metadata_rows


def test_pomm_metadata_names_current_distribution() -> None:
    frame = DataFrame()
    rows = _build_metadata_rows(
        timestamp="2026-10-18",
        include_pomm=False,
        aep_dur_max=frame,
        aep_max=frame,
        aggregated_df=frame,
        aep_dur_sheet_name="AEP / duration",
        aep_sheet_name="AEP",
    )
    assert "ryan-tools version" in rows
    assert "ryan_functions version" not in rows
