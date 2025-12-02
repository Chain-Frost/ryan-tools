"""Unit tests for ryan_library.functions.dataframe_helpers."""

import pandas as pd
import pytest
from ryan_library.functions.dataframe_helpers import (
    merge_and_sort_data,
    reorder_columns,
    reorder_long_columns,
    reset_categorical_ordering,
)


class TestMergeAndSortData:
    """Tests for merge_and_sort_data function."""

    def test_merge_basic(self):
        """Test merging two simple DataFrames."""
        df1 = pd.DataFrame({"A": [1, 2]})
        df2 = pd.DataFrame({"A": [3, 4]})
        result = merge_and_sort_data([df1, df2])
        assert len(result) == 4
        assert result["A"].tolist() == [1, 2, 3, 4]

    def test_merge_and_sort(self):
        """Test merging and sorting."""
        df1 = pd.DataFrame({"A": [2, 1]})
        df2 = pd.DataFrame({"A": [4, 3]})
        result = merge_and_sort_data([df1, df2], sort_column="A", ascending=True)
        assert result["A"].tolist() == [1, 2, 3, 4]

    def test_merge_empty_list(self):
        """Test merging an empty list of frames."""
        result = merge_and_sort_data([])
        assert result.empty

    def test_sort_column_missing(self):
        """Test sorting by a non-existent column (should warn but not fail)."""
        df1 = pd.DataFrame({"A": [1]})
        result = merge_and_sort_data([df1], sort_column="B")
        assert len(result) == 1
        assert "A" in result.columns


class TestReorderColumns:
    """Tests for reorder_columns function."""

    def test_prioritized_columns(self):
        """Test that prioritized columns come first."""
        df = pd.DataFrame({"C": [1], "A": [1], "B": [1]})
        result = reorder_columns(df, prioritized_columns=["A", "B"])
        assert result.columns.tolist() == ["A", "B", "C"]

    def test_prefix_order(self):
        """Test ordering by prefix."""
        df = pd.DataFrame({"other": [1], "pre_2": [1], "pre_1": [1]})
        result = reorder_columns(df, prefix_order=["pre_"])
        # pre_1 and pre_2 should be sorted alphabetically within the prefix group
        assert result.columns.tolist()[:2] == ["pre_1", "pre_2"]
        assert result.columns.tolist()[2] == "other"

    def test_columns_to_end(self):
        """Test moving columns to the end."""
        df = pd.DataFrame({"Z": [1], "A": [1], "B": [1]})
        result = reorder_columns(df, columns_to_end=["A"])
        assert result.columns.tolist() == ["B", "Z", "A"]  # B and Z sorted alphabetically in remaining

    def test_missing_columns_ignored(self):
        """Test that missing columns in priority lists are ignored."""
        df = pd.DataFrame({"A": [1]})
        result = reorder_columns(df, prioritized_columns=["X", "A"])
        assert result.columns.tolist() == ["A"]


class TestReorderLongColumns:
    """Tests for reorder_long_columns function."""

    def test_reorder_long(self):
        """Test that specific columns are moved to the end."""
        df = pd.DataFrame({
            "path": [1],
            "Value": [1],
            "file": [1]
        })
        result = reorder_long_columns(df)
        # file and path should be at the end
        assert result.columns.tolist()[-2:] == ["file", "path"]
        assert result.columns.tolist()[0] == "Value"


class TestResetCategoricalOrdering:
    """Tests for reset_categorical_ordering function."""

    def test_reset_ordering(self):
        """Test that categories are sorted alphabetically."""
        df = pd.DataFrame({"cat": pd.Categorical(["b", "a"], categories=["b", "a"], ordered=True)})
        # Initially b < a is False if order is [b, a]? No, index 0 is b.
        # Let's check the categories order.
        assert df["cat"].cat.categories.tolist() == ["b", "a"]
        
        result = reset_categorical_ordering(df)
        assert result["cat"].cat.categories.tolist() == ["a", "b"]
