import pandas as pd
import pytest

from ryan_library.processors.rorb.ensemble_reader import calculate_closure_times, calculate_peak_flows


def test_calculate_peak_flows():
    data = {
        "Model": ["A", "A", "A", "A"],
        "Location": ["Loc1", "Loc1", "Loc1", "Loc1"],
        "AEP": [10, 10, 20, 20],
        "Duration": [1, 1, 1, 1],
        "TP": [1, 2, 1, 2],
        "Flow": [100.0, 150.0, 200.0, 180.0],
    }
    df = pd.DataFrame(data)
    group_cols = ["Model", "Location", "AEP", "Duration"]
    
    result = calculate_peak_flows(df, group_cols=group_cols)
    
    assert len(result) == 2
    assert result[(result["AEP"] == 10)]["PeakFlow"].iloc[0] == 150.0
    assert result[(result["AEP"] == 20)]["PeakFlow"].iloc[0] == 200.0


def test_calculate_closure_times():
    data = {
        "Model": ["A", "A", "A", "A"],
        "Location": ["Loc1", "Loc1", "Loc1", "Loc1"],
        "AEP": [10, 10, 10, 10],
        "Time": [0.0, 0.5, 1.0, 1.5], # dt = 0.5
        "Flow": [50.0, 100.0, 80.0, 40.0],
    }
    df = pd.DataFrame(data)
    group_cols = ["Model", "Location", "AEP"]
    
    # threshold = 76. Flows exceeding: 100.0, 80.0. That's 2 timesteps.
    # closure time = 2 * 0.5 = 1.0 hour.
    result = calculate_closure_times(df, threshold=76.0, group_cols=group_cols)
    
    assert len(result) == 1
    assert result["ClosureTime"].iloc[0] == 1.0
