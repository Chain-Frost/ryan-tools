"""Tests for ryan_library.functions.hy8.run_hy8_bridge."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ryan_library.functions.hy8 import run_hy8_bridge


class TestRecordParsing:
    def test_from_mapping_valid(self) -> None:
        """Test parsing a valid row."""
        row = {
            "trim_runcode": "Run1",
            "internalName": "Int1",
            "Chan ID": "Chan1",
            "Q": 10.0,
            "DS_h": 5.0,
            "DS Invert": 2.0,
            "US Invert": 3.0,
            "Height": 1.5,
            "Length": 20.0,
            "Flags": "C",
            "DS Obvert": 3.5,
            "US Obvert": 4.5,
            "US_h": 6.0,
            "V": 2.0,
            "pSlope": 1.0,
            "pBlockage": 0.0,
            "n or Cd": 0.013,
            "Area_Culv": 2.25,
        }
        record = run_hy8_bridge.CulvertMaximumRecord.from_mapping(row, row_index=1)
        assert record is not None
        assert record.trim_runcode == "Run1"
        assert record.flow_q == 10.0
        assert record.height == 1.5
        assert record.flag == "C"

    def test_from_mapping_missing_required(self) -> None:
        """Test parsing a row with missing required fields."""
        row = {
            "trim_runcode": "Run1",
        }
        record = run_hy8_bridge.CulvertMaximumRecord.from_mapping(row, row_index=1)
        assert record is None


class TestBridgeLogic:
    @pytest.fixture
    def sample_record(self) -> run_hy8_bridge.CulvertMaximumRecord:
        return run_hy8_bridge.CulvertMaximumRecord(
            row_index=1,
            trim_runcode="Run1",
            internal_name="Int1",
            chan_id="Chan1",
            flow_q=10.0,
            ds_headwater=5.0,
            ds_invert=2.0,
            us_invert=3.0,
            height=1.5,
            length=20.0,
            flag="C",
            mannings_n=0.013,
        )

    @patch("ryan_library.functions.hy8.run_hy8_bridge.CulvertCrossing")
    def test_build_crossing_from_record(
        self,
        mock_crossing_cls: MagicMock,
        sample_record: run_hy8_bridge.CulvertMaximumRecord,
    ) -> None:
        """Test building a crossing object."""
        mock_crossing = MagicMock()
        mock_crossing_cls.return_value = mock_crossing

        res = run_hy8_bridge.build_crossing_from_record(sample_record)

        assert res == mock_crossing
        mock_crossing_cls.assert_called_once()
        # Check if properties were set
        assert mock_crossing.flow is not None
        assert mock_crossing.tailwater is not None
        assert len(mock_crossing.culverts) == 1

    @patch("ryan_library.functions.hy8.run_hy8_bridge.build_crossing_from_record")
    @patch.object(run_hy8_bridge.CulvertMaximumRecord, "from_mapping")
    def test_maximums_dataframe_to_crossings(
        self,
        mock_from_mapping: MagicMock,
        mock_build_crossing: MagicMock,
        sample_record: run_hy8_bridge.CulvertMaximumRecord,
    ) -> None:
        maximums = pd.DataFrame([{"Q": 10.0}, {"Q": None}], index=[10, 11])
        expected_crossing = MagicMock()
        mock_from_mapping.side_effect = [sample_record, None]
        mock_build_crossing.return_value = expected_crossing

        crossings = run_hy8_bridge.maximums_dataframe_to_crossings(maximums)

        assert crossings == [expected_crossing]
        assert mock_from_mapping.call_count == 2
        assert mock_from_mapping.call_args_list[0].kwargs["row_index"] == 10
        assert mock_from_mapping.call_args_list[1].kwargs["row_index"] == 11
        mock_build_crossing.assert_called_once_with(
            sample_record,
            options=run_hy8_bridge.Hy8CulvertOptions(),
        )


class TestHelpers:
    def test_resolve_crossing_name(self) -> None:
        record = MagicMock()
        record.internal_name = "Int1"
        record.chan_id = "Chan1"
        record.trim_runcode = "Run1"
        options = run_hy8_bridge.Hy8CulvertOptions()

        name = run_hy8_bridge._resolve_crossing_name(record, options)
        assert name == "Int1_Chan1"

    def test_build_flow(self) -> None:
        record = MagicMock()
        record.flow_q = 10.0
        options = run_hy8_bridge.Hy8CulvertOptions()

        flow = run_hy8_bridge._build_flow(record, options)
        assert flow.design >= 10.0
