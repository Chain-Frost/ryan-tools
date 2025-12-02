"""Tests for ryan_library.functions.tlf_missing_runs."""

import pandas as pd
import pytest
from ryan_library.functions import tlf_missing_runs

class TestHelpers:
    def test_standardize_tp(self):
        assert tlf_missing_runs._standardize_tp("TP01") == "TP01"
        assert tlf_missing_runs._standardize_tp("tp1") == "TP01"
        assert tlf_missing_runs._standardize_tp("1") == "TP01"
        assert tlf_missing_runs._standardize_tp("10") == "TP10"
        assert tlf_missing_runs._standardize_tp("11") == ""
        assert tlf_missing_runs._standardize_tp(None) == ""
        assert tlf_missing_runs._standardize_tp("invalid") == ""

    def test_normalize_columns(self):
        df = pd.DataFrame({
            "AEP": [1],
            "Duration": [2],
            "TP": ["1"],
            "trim_run_code": ["R1"]
        })
        norm = tlf_missing_runs._normalize_columns(df)
        assert list(norm.columns) == ["AEP", "Duration", "TP", "trim_run_code"]
        assert norm.iloc[0]["TP"] == "TP01"

    def test_normalize_columns_missing(self):
        df = pd.DataFrame({"AEP": [1]})
        with pytest.raises(KeyError):
            tlf_missing_runs._normalize_columns(df)

    def test_unique_sorted(self):
        s = pd.Series(["10", "2", "1"])
        assert tlf_missing_runs._unique_sorted(s) == ["1", "2", "10"]
        
        s2 = pd.Series(["b", "a"])
        assert tlf_missing_runs._unique_sorted(s2) == ["a", "b"]

class TestAnalysis:
    def test_analyze_missing_runs_complete(self):
        # Create a complete set for one run/dur/aep
        tps = [f"TP{i:02d}" for i in range(1, 11)]
        df = pd.DataFrame({
            "trim_run_code": ["R1"] * 10,
            "Duration": ["D1"] * 10,
            "AEP": ["A1"] * 10,
            "TP": tps
        })
        
        result = tlf_missing_runs.analyze_missing_runs(df)
        assert not result.no_sets
        assert len(result.completed_sets) == 1
        assert len(result.outstanding_sets) == 0
        assert result.completed_sets[0].trim_run_code == "R1"

    def test_analyze_missing_runs_incomplete(self):
        # We need at least one COMPLETE set for outstanding sets to be reported (business rule)
        tps_complete = [f"TP{i:02d}" for i in range(1, 11)]
        tps_incomplete = [f"TP{i:02d}" for i in range(1, 10)] # Missing TP10
        
        df = pd.DataFrame({
            "trim_run_code": ["R1"] * 10 + ["R2"] * 9,
            "Duration": ["D1"] * 10 + ["D1"] * 9,
            "AEP": ["A1"] * 10 + ["A1"] * 9,
            "TP": tps_complete + tps_incomplete
        })
        
        result = tlf_missing_runs.analyze_missing_runs(df)
        assert not result.no_sets
        assert len(result.completed_sets) == 1
        assert len(result.outstanding_sets) == 1
        assert result.outstanding_sets[0].trim_run_code == "R2"
        assert result.outstanding_sets[0].missing_tps == ("TP10",)

    def test_analyze_missing_runs_no_sets(self):
        df = pd.DataFrame(columns=["trim_run_code", "Duration", "AEP", "TP"])
        result = tlf_missing_runs.analyze_missing_runs(df)
        assert result.no_sets
        assert len(result.completed_sets) == 0
        assert len(result.outstanding_sets) == 0

class TestReporting:
    def test_to_summary_frames(self):
        # Setup result manually
        completed = [tlf_missing_runs.CompletedSet("R1", "D1", "A1")]
        outstanding = [tlf_missing_runs.OutstandingSet("R2", "D2", "A2", ("TP01",))]
        result = tlf_missing_runs.AnalysisResult(False, completed, outstanding)
        
        frames = tlf_missing_runs.to_summary_frames(result)
        
        assert "completed_sets" in frames
        assert len(frames["completed_sets"]) == 1
        assert frames["completed_sets"].iloc[0]["trim_run_code"] == "R1"
        
        assert "outstanding_missing_tps" in frames
        assert len(frames["outstanding_missing_tps"]) == 1
        assert frames["outstanding_missing_tps"].iloc[0]["MissingTP"] == "TP01"
        
        assert "per_trim_run_code_counts" in frames
        counts = frames["per_trim_run_code_counts"]
        r1 = counts[counts["trim_run_code"] == "R1"].iloc[0]
        assert r1["completed_sets"] == 1
        assert r1["outstanding_sets"] == 0

    def test_summarize_for_cli(self):
        tps = [f"TP{i:02d}" for i in range(1, 11)]
        df = pd.DataFrame({
            "trim_run_code": ["R1"] * 10,
            "Duration": ["D1"] * 10,
            "AEP": ["A1"] * 10,
            "TP": tps
        })
        
        text, table = tlf_missing_runs.summarize_for_cli(df)
        assert "=== R1 ===" in text
        assert "AEPs: ['A1']" in text
        assert not table.empty
