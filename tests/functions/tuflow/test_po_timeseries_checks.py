"""Tests for ryan_library.functions.tuflow.po_timeseries_checks."""

import pytest
import pandas as pd
from pathlib import Path
from ryan_library.functions.tuflow import po_timeseries_checks as ptc


class TestFilters:
    def test_normalize_value(self) -> None:
        assert ptc._normalize_value(" FOO ", False) == "foo"
        assert ptc._normalize_value(" FOO ", True) == "FOO"

    def test_datatype_allowed(self) -> None:
        include_set = {"flow", "level"}
        assert ptc._datatype_allowed("Flow", include_set, False) is True
        assert ptc._datatype_allowed("Flow", include_set, True) is False
        assert ptc._datatype_allowed("velocity", include_set, False) is False

    def test_location_allowed(self) -> None:
        include_set = {"loc1", "loc2"}
        exclude_set = {"loc2"}
        assert ptc._location_allowed("loc1", include_set, exclude_set, False) is True
        assert ptc._location_allowed("loc2", include_set, exclude_set, False) is False
        assert ptc._location_allowed("loc3", include_set, exclude_set, False) is False

        # No include set means all included
        assert ptc._location_allowed("loc3", set(), exclude_set, False) is True


class TestCsvParsing:
    def test_parse_po_csv_empty(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")
        data, status, should_emit = ptc._parse_po_csv(csv_file)
        assert data is None
        assert status == "EMPTY_FILE"
        assert should_emit is True

    def test_parse_po_csv_valid(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "valid.csv"
        # TUFLOW PO CSVs often have an empty first column or a column that is skipped,
        # then Time, then the data.
        csv_content_po = "Dummy,Time,Q,V\n" ", (h),loc1,loc2\n" "0, 0.0,1.0,2.0\n" "1, 0.1,1.1,2.1\n" "2, 0.2,1.2,2.2\n"
        csv_file.write_text(csv_content_po)
        data, status, should_emit = ptc._parse_po_csv(csv_file)
        assert data is not None
        assert status is None
        assert should_emit is False
        assert data.end_hours == 0.2
        # df should have Time, Q, and V since only Dummy was removed
        assert data.df.shape[1] == 3

    def test_parse_q_csv_valid(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "q_valid.csv"
        csv_content_q = "Time, Q_loc1, Q_loc2\n" "0.0, 1.0, 2.0\n" "0.1, 1.1, 2.1\n"
        csv_file.write_text(csv_content_q)
        data, status, should_emit = ptc._parse_q_csv(csv_file)
        assert data is not None
        assert status is None
        assert should_emit is False
        assert list(data.df.columns) == ["Time", "Q_loc1", "Q_loc2"]
        assert data.time_hours.iloc[-1] == 0.1


class TestAnalysis:
    def test_analyze_peak_csv_empty(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "empty_PO.csv"
        csv_file.write_text("")
        config = ptc.PeakCheckConfig(
            datatype_include=["Q"],
            datatype_case_sensitive=False,
            location_include=[],
            location_exclude=[],
            location_case_sensitive=False,
            warn_2hours=2.0,
            warn_1hour=1.0,
            flat_tol=0.01,
        )
        results = ptc.analyze_peak_csv(csv_file, config)
        assert len(results) == 1
        assert results[0].status == "EMPTY_FILE"

    def test_analyze_stability_csv_empty(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "empty_PO.csv"
        csv_file.write_text("")
        config = ptc.StabilityCheckConfig(
            datatype_include=["Q"],
            datatype_case_sensitive=False,
            location_include=[],
            location_exclude=[],
            location_case_sensitive=False,
            flat_tol=0.01,
            diff_rel_tol=0.05,
            diff_abs_tol=0.1,
            max_sign_changes=10,
            min_points=10,
        )
        results = ptc.analyze_stability_csv(csv_file, config)
        assert len(results) == 1
        assert results[0].status == "EMPTY_FILE"

    def test_parse_run_meta(self) -> None:
        path = Path("Q100_2hr_001_PO.csv")
        meta = ptc.parse_run_meta_from_filename(path)
        assert meta["trim_run_code"] == "Q100_001"

    def test_evaluate_stability_series(self) -> None:
        values = pd.Series([1.0, 1.1, 1.2, 1.1, 1.0, 0.9])
        times = pd.Series([0.0, 0.1, 0.2, 0.3, 0.4, 0.5])
        config = ptc.StabilityCheckConfig(
            datatype_include=["Q"],
            datatype_case_sensitive=False,
            location_include=[],
            location_exclude=[],
            location_case_sensitive=False,
            flat_tol=0.01,
            diff_rel_tol=0.05,
            diff_abs_tol=0.1,
            max_sign_changes=2,
            min_points=3,
        )
        result = ptc._evaluate_stability_series(
            path=Path("test.csv"),
            run_code="test",
            run_meta={},
            datatype="Q",
            location="loc",
            values_raw=values,
            time_hours=times,
            config=config,
        )
        assert result.status == "OK"
        assert result.points == 6
        assert result.value_min == 0.9
        assert result.value_max == 1.2
        assert result.sign_changes == 1

    def test_analyze_peak_csv_valid(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "valid_PO.csv"
        csv_content_po = "Dummy,Time,Q,V\n" ", (h),loc1,loc2\n" "0, 0.0,1.0,2.0\n" "1, 0.1,5.0,2.1\n" "2, 5.0,1.2,2.2\n"
        csv_file.write_text(csv_content_po)
        config = ptc.PeakCheckConfig(
            datatype_include=["Q", "V"],
            datatype_case_sensitive=False,
            location_include=[],
            location_exclude=[],
            location_case_sensitive=False,
            warn_2hours=2.0,
            warn_1hour=1.0,
            flat_tol=0.01,
        )
        results = ptc.analyze_peak_csv(csv_file, config)
        # Should have results for Q_loc1 and V_loc2
        assert len(results) == 2
        q_res = next(r for r in results if r.location == "loc1" and r.datatype == "Q")
        assert q_res.status == "OK"
        assert q_res.peak_value == 5.0
        assert q_res.peak_time == 0.1

    def test_analyze_stability_csv_valid(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "valid_PO.csv"
        csv_content_po = (
            "Dummy,Time,Q,V\n"
            ", (h),loc1,loc2\n"
            "0, 0.0,1.0,2.0\n"
            "1, 0.1,1.1,2.1\n"
            "2, 0.2,1.2,2.2\n"
            "3, 0.3,1.3,2.3\n"
        )
        csv_file.write_text(csv_content_po)
        config = ptc.StabilityCheckConfig(
            datatype_include=["Q", "V"],
            datatype_case_sensitive=False,
            location_include=[],
            location_exclude=[],
            location_case_sensitive=False,
            flat_tol=0.01,
            diff_rel_tol=0.05,
            diff_abs_tol=0.1,
            max_sign_changes=2,
            min_points=3,
        )
        results = ptc.analyze_stability_csv(csv_file, config)
        assert len(results) == 2
        for res in results:
            assert res.status == "OK"
            assert res.points == 4

    def test_parse_run_meta_full(self) -> None:
        path = Path("Q100_2hr_001_PO.csv")
        # Mocking or simulating TuflowStringParser behaviour might be tricky if we don't mock it,
        # but let's test a string that gives more details if possible, or mock the parser.
        from unittest.mock import patch

        with patch("ryan_library.functions.tuflow.po_timeseries_checks.tsc.TuflowStringParser") as MockParser:
            instance = MockParser.return_value
            instance.data_type = "PO"
            instance.raw_run_code = "Q100_2hr_001"
            instance.trim_run_code = "Q100_001"
            instance.tp.text_repr = "2hr"
            instance.tp.numeric_value = 2.0
            instance.duration.text_repr = "2hr"
            instance.duration.numeric_value = 120.0
            instance.aep.text_repr = "1% AEP"
            instance.aep.numeric_value = 1.0
            instance.run_code_parts = {"Event": "Q100"}

            meta = ptc.parse_run_meta_from_filename(path)
            assert meta["data_type"] == "PO"
            assert meta["TP"] == "2hr"
            assert meta["TP_num"] == "2.0"
            assert meta["Duration"] == "2hr"
            assert meta["Duration_m"] == "120.0"
            assert meta["AEP"] == "1% AEP"
            assert meta["AEP_value"] == "1.0"
            assert meta["Event"] == "Q100"

    def test_parse_po_csv_errors(self, tmp_path: Path) -> None:
        import pandas as pd

        # Test NO_COLUMNS
        csv_file = tmp_path / "no_cols.csv"
        csv_file.write_text("\n")
        data, status, _ = ptc._parse_po_csv(csv_file)
        assert status in ("NO_COLUMNS", "BAD_HEADER", "CSV_PARSE_FAIL")

        # Test BAD_HEADER / TIME_PARSE_FAIL
        csv_file.write_text("A,B,C\n1,2,3")
        data, status, _ = ptc._parse_po_csv(csv_file)
        assert status in ("BAD_HEADER", "TIME_PARSE_FAIL")

        # Test TIME_PARSE_FAIL
        csv_file.write_text("Dummy,Time\n,(h)\n0,invalid\n1,invalid")
        data, status, _ = ptc._parse_po_csv(csv_file)
        assert status == "TIME_PARSE_FAIL"

    def test_parse_q_csv_errors(self, tmp_path: Path) -> None:
        # NO_DATA
        csv_file = tmp_path / "no_data.csv"
        csv_file.write_text("Time,Q\n")
        data, status, _ = ptc._parse_q_csv(csv_file)
        assert status == "NO_DATA"

        # TIME_PARSE_FAIL missing col
        csv_file.write_text("NotTime,Q\n1,2")
        data, status, _ = ptc._parse_q_csv(csv_file)
        assert status == "TIME_PARSE_FAIL"

        # TIME_PARSE_FAIL bad data
        csv_file.write_text("Time,Q\ninvalid,2")
        data, status, _ = ptc._parse_q_csv(csv_file)
        assert status == "TIME_PARSE_FAIL"

    def test_evaluate_stability_series_edge_cases(self) -> None:
        config = ptc.StabilityCheckConfig(
            datatype_include=["Q"],
            datatype_case_sensitive=False,
            location_include=[],
            location_exclude=[],
            location_case_sensitive=False,
            flat_tol=0.01,
            diff_rel_tol=0.05,
            diff_abs_tol=0.1,
            max_sign_changes=2,
            min_points=3,
        )

        # NO_DATA
        res = ptc._evaluate_stability_series(
            path=Path("t.csv"),
            run_code="t",
            run_meta={},
            datatype="Q",
            location="L",
            values_raw=pd.Series([pd.NA]),
            time_hours=pd.Series([pd.NA]),
            config=config,
        )
        assert res.status == "NO_DATA"

        # INSUFFICIENT_POINTS
        res = ptc._evaluate_stability_series(
            path=Path("t.csv"),
            run_code="t",
            run_meta={},
            datatype="Q",
            location="L",
            values_raw=pd.Series([1.0, pd.NA]),
            time_hours=pd.Series([0.0, 1.0]),
            config=config,
        )
        assert res.status == "INSUFFICIENT_POINTS"

        # FLAT
        res = ptc._evaluate_stability_series(
            path=Path("t.csv"),
            run_code="t",
            run_meta={},
            datatype="Q",
            location="L",
            values_raw=pd.Series([1.0, 1.001, 1.0]),
            time_hours=pd.Series([0.0, 1.0, 2.0]),
            config=config,
        )
        assert res.status == "FLAT"

        # UNSTABLE
        res = ptc._evaluate_stability_series(
            path=Path("t.csv"),
            run_code="t",
            run_meta={},
            datatype="Q",
            location="L",
            values_raw=pd.Series([1.0, 2.0, 1.0, 2.0, 1.0]),
            time_hours=pd.Series([0.0, 1.0, 2.0, 3.0, 4.0]),
            config=config,
        )
        assert res.status == "UNSTABLE"
        assert res.sign_changes == 3

    def test_analyze_stability_q_csv(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "valid_Q.csv"
        csv_content = "Time, Q_loc1, Q_loc2\n" "0.0, 1.0, 2.0\n" "0.1, 1.1, 2.1\n" "0.2, 1.2, 2.2\n" "0.3, 1.3, 2.3\n"
        csv_file.write_text(csv_content)
        config = ptc.StabilityCheckConfig(
            datatype_include=["Q"],
            datatype_case_sensitive=False,
            location_include=[],
            location_exclude=[],
            location_case_sensitive=False,
            flat_tol=0.01,
            diff_rel_tol=0.05,
            diff_abs_tol=0.1,
            max_sign_changes=2,
            min_points=3,
        )
        results = ptc.analyze_stability_q_csv(csv_file, config)
        assert len(results) == 2
        for res in results:
            assert res.status == "OK"
            assert res.datatype == "Q"

    def test_flatten_results(self) -> None:
        res = ptc.PeakCheckResult(
            file="f",
            run_code="r",
            run_meta={"A": "1"},
            datatype="Q",
            location="L",
            peak_kind="max",
            peak_value=1.0,
            peak_time=1.0,
            end_time=2.0,
            hours_from_end=1.0,
            start_value=0.0,
            end_value=0.5,
            end_minus_start=0.5,
            peak_above_start=1.0,
            end_pct_of_peak=50.0,
            status="OK",
        )
        flat = ptc.flatten_peak_results([res])
        assert flat[0]["file"] == "f"
        assert flat[0]["A"] == "1"
