from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ensemble_boxplot import plot_exceedance_duration, plot_peak_flow_distribution


def test_plot_peak_flow_distribution(tmp_path: Path) -> None:
    data = pd.DataFrame(
        {
            "AEP": [10, 10, 20, 20],
            "Duration": [1, 2, 1, 2],
            "PeakFlow": [100.0, 120.0, 150.0, 160.0],
        }
    )
    output_path = tmp_path / "test_peak.png"

    plot_peak_flow_distribution(data, output_path, "TestLoc")

    assert output_path.is_file()


def test_plot_requires_expected_columns(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Missing plotting columns"):
        plot_peak_flow_distribution(pd.DataFrame({"AEP": [10]}), tmp_path / "invalid.png", "TestLoc")


def test_plot_exceedance_duration(tmp_path: Path) -> None:
    data = pd.DataFrame(
        {
            "AEP": [10, 10, 20, 20],
            "ClosureTime": [1.0, 1.5, 2.0, 2.5],
            "CC": ["Current", "Future", "Current", "Future"],
        }
    )
    output_path = tmp_path / "test_closure.png"

    plot_exceedance_duration(data, output_path, "TestLoc")

    assert output_path.is_file()
