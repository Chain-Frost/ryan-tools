"""Regression tests for Excel export column-label handling."""

import pandas as pd

from ryan_library.functions.excel_export import ExcelExporter, build_data_dictionary


def test_integer_column_labels_are_normalized_for_metadata_and_widths() -> None:
    """Non-string labels remain usable for metadata lookup and positional data access."""

    frame = pd.DataFrame(data=[[123]], columns=[7])

    dictionary: pd.DataFrame = build_data_dictionary(sheet_frames={"results": frame})
    widths: dict[str, float] = ExcelExporter().calculate_column_widths(frame)

    assert dictionary.loc[0, "column"] == "7"
    assert dictionary.loc[0, "pandas_dtype"] == str(frame.dtypes.iloc[0])
    assert widths == {"A": 5}
