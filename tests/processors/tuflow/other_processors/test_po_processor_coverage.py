"""Coverage tests for POProcessor."""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from pathlib import Path

from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor


class TestPOProcessorEdgeCases:
    @patch("ryan_library.processors.tuflow.other_processors.POProcessor.pd.read_csv")
    def test_process_empty_tidy_df(self, mock_read_csv):
        proc = POProcessor(Path("test_PO.csv"))
        # Mock _parse_point_output directly to return empty
        with patch.object(proc, "_parse_point_output", return_value=pd.DataFrame()):
            proc.process()
        assert proc.df.empty
        assert not proc.processed

    @patch("ryan_library.processors.tuflow.other_processors.POProcessor.pd.read_csv")
    def test_process_validation_fails(self, mock_read_csv):
        proc = POProcessor(Path("test_PO.csv"))

        # Valid tidy df
        tidy_df = pd.DataFrame({"Time": [1.0], "Location": ["L1"], "Type": ["Q"], "Value": [10.0]})
        with patch.object(proc, "_parse_point_output", return_value=tidy_df):
            with patch.object(proc, "validate_data", return_value=False):
                proc.process()
                assert not proc.processed

    def test_parse_shape_too_small(self):
        proc = POProcessor(Path("test_PO.csv"))
        df = pd.DataFrame({"A": [1]})
        res = proc._parse_point_output(df)
        assert res.empty

    def test_parse_missing_headers(self):
        proc = POProcessor(Path("test_PO.csv"))
        df = pd.DataFrame({0: ["Time", "Time", "0"], 1: [pd.NA, pd.NA, 10.0]})
        # After drop 0, it has 1 column.
        res = proc._parse_point_output(df)
        assert res.empty

    def test_parse_time_column_no_numeric(self):
        proc = POProcessor(Path("test_PO.csv"))
        df = pd.DataFrame({0: ["Col1", "Col1", "Time"], 1: ["Time", "Time", "Invalid"], 2: ["Q", "Loc", "Text"]})
        res = proc._parse_point_output(df)
        assert res.empty

    def test_parse_empty_measurement_or_location(self):
        proc = POProcessor(Path("test_PO.csv"))
        df = pd.DataFrame(
            {
                0: ["dummy", "dummy", "dummy"],
                1: ["Time", "Time", "1.0"],
                2: ["", "Loc", "10.0"],  # missing measurement
                3: ["Q", "", "20.0"],  # missing location
            }
        )
        res = proc._parse_point_output(df)
        # Should skip col 2 and 3, yielding empty
        assert res.empty

    def test_parse_all_na(self):
        proc = POProcessor(Path("test_PO.csv"))
        df = pd.DataFrame(
            {
                0: ["dummy", "dummy", "dummy"],
                1: ["Time", "Time", "1.0"],
                2: ["Q", "Loc", "Invalid"],  # NaNs after numeric coerce
            }
        )
        res = proc._parse_point_output(df)
        assert res.empty

    def test_parse_empty_after_dropna(self):
        proc = POProcessor(Path("test_PO.csv"))
        df = pd.DataFrame(
            {
                0: ["dummy", "dummy", "dummy", "dummy"],
                1: ["Time", "Time", "1.0", "2.0"],
                2: ["Q", "Loc", "Invalid", "Invalid"],
            }
        )
        res = proc._parse_point_output(df)
        assert res.empty

    def test_locate_time_column_none(self):
        proc = POProcessor(Path("test_PO.csv"))
        measurement = pd.Series(["A", "B"])
        location = pd.Series(["C", "D"])
        assert proc._locate_time_column(measurement, location) is None
