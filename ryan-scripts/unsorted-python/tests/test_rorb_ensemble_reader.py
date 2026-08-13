from __future__ import annotations

import pandas as pd

from rorb_ensemble_reader import calculate_closure_times, calculate_peak_flows


def test_calculate_peak_flows() -> None:
    data = pd.DataFrame(
        {
            "Model": ["A", "A", "A", "A"],
            "Location": ["Loc1", "Loc1", "Loc1", "Loc1"],
            "AEP": [10, 10, 20, 20],
            "Duration": [1, 1, 1, 1],
            "TP": [1, 2, 1, 2],
            "Flow": [100.0, 150.0, 200.0, 180.0],
        }
    )

    result = calculate_peak_flows(data, group_cols=["Model", "Location", "AEP", "Duration"])

    assert result["PeakFlow"].tolist() == [150.0, 200.0]


def test_calculate_closure_times_uses_first_to_last_exceedance_without_mutating_input() -> None:
    data = pd.DataFrame(
        {
            "Model": ["A", "A", "A", "A"],
            "Time": [0.0, 0.5, 1.0, 1.5],
            "Flow": [80.0, 40.0, 100.0, 40.0],
        }
    )

    result = calculate_closure_times(data, threshold=76.0, group_cols=["Model"])

    assert result["ClosureTime"].tolist() == [1.0]
    assert "_exceeds" not in data.columns
