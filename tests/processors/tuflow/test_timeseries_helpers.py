
"""Unit tests for ryan_library.processors.tuflow.timeseries_helpers."""

import pytest
import pandas as pd
from ryan_library.processors.tuflow.timeseries_helpers import reshape_h_timeseries

def test_reshape_h_timeseries_success():
    """Test successful reshaping of H timeseries data."""
    df = pd.DataFrame({
        "Time": [0.0, 1.0],
        "C1.1": [10.0, 11.0],
        "C1.2": [9.0, 8.0],
        "C2.1": [20.0, 21.0],
        # C2.2 missing, should be NaN
    })
    
    reshaped = reshape_h_timeseries(df, category_type="Chan ID", file_label="TestFile")
    
    assert len(reshaped) == 4 # 2 times * 2 channels
    assert "Time" in reshaped.columns
    assert "Chan ID" in reshaped.columns
    assert "US_H" in reshaped.columns
    assert "DS_H" in reshaped.columns
    
    # Check C1 at time 0.0
    c1_t0 = reshaped[(reshaped["Chan ID"] == "C1") & (reshaped["Time"] == 0.0)].iloc[0]
    assert c1_t0["US_H"] == 10.0
    assert c1_t0["DS_H"] == 9.0
    
    # Check C2 at time 0.0 (DS_H should be NaN)
    c2_t0 = reshaped[(reshaped["Chan ID"] == "C2") & (reshaped["Time"] == 0.0)].iloc[0]
    assert c2_t0["US_H"] == 20.0
    assert pd.isna(c2_t0["DS_H"])

def test_reshape_h_timeseries_missing_time():
    """Test failure when Time column is missing."""
    df = pd.DataFrame({
        "C1.1": [10.0]
    })
    
    with pytest.raises(ValueError, match="must contain a 'Time' column"):
        reshape_h_timeseries(df, category_type="Chan ID", file_label="TestFile")

def test_reshape_h_timeseries_no_value_columns():
    """Test failure when no value columns exist."""
    df = pd.DataFrame({
        "Time": [0.0]
    })
    
    with pytest.raises(ValueError, match="No value columns found"):
        reshape_h_timeseries(df, category_type="Chan ID", file_label="TestFile")

def test_reshape_h_timeseries_no_valid_suffixes():
    """Test failure when no columns have .1 or .2 suffixes."""
    df = pd.DataFrame({
        "Time": [0.0],
        "C1": [10.0], # Invalid suffix
        "C2.3": [20.0] # Invalid suffix
    })
    
    with pytest.raises(ValueError, match="No columns with valid"):
        reshape_h_timeseries(df, category_type="Chan ID", file_label="TestFile")

def test_reshape_h_timeseries_mixed_suffixes():
    """Test that invalid suffixes are ignored."""
    df = pd.DataFrame({
        "Time": [0.0],
        "C1.1": [10.0],
        "C1.3": [5.0] # Should be ignored
    })
    
    reshaped = reshape_h_timeseries(df, category_type="Chan ID", file_label="TestFile")
    
    assert len(reshaped) == 1
    assert reshaped.iloc[0]["US_H"] == 10.0
    assert pd.isna(reshaped.iloc[0]["DS_H"])
