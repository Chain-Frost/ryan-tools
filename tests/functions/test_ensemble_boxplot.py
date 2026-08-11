from pathlib import Path

import pandas as pd
import pytest

from ryan_library.functions.plotting.ensemble_boxplot import plot_exceedance_duration, plot_peak_flow_distribution


def test_plot_peak_flow_distribution(tmp_path: Path):
    df = pd.DataFrame({
        "AEP": [10, 10, 20, 20],
        "Duration": [1, 2, 1, 2],
        "PeakFlow": [100.0, 120.0, 150.0, 160.0]
    })
    out_path = tmp_path / "test_peak.png"
    
    plot_peak_flow_distribution(df, out_path, "TestLoc")
    assert out_path.exists()


def test_plot_exceedance_duration(tmp_path: Path):
    df = pd.DataFrame({
        "AEP": [10, 10, 20, 20],
        "ClosureTime": [1.0, 1.5, 2.0, 2.5],
        "CC": ["Current", "Future", "Current", "Future"]
    })
    out_path = tmp_path / "test_closure.png"
    
    plot_exceedance_duration(df, out_path, "TestLoc")
    assert out_path.exists()
