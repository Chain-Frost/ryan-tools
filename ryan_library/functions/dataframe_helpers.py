# ryan_library\functions\dataframe_helpers.py

import pandas as pd
from loguru import logger


def merge_and_sort_data(
    frames: list[pd.DataFrame],
    sort_column: str | None = None,
    ascending: bool = False,
) -> pd.DataFrame:
    """
    Merges a list of DataFrames and optionally sorts them by a given column.

    Parameters:
        frames (list[pd.DataFrame]): List of DataFrames to merge.
        sort_column (Optional[str]): Column to sort by. If None, no sorting is applied.
        ascending (bool): Whether to sort in ascending order. Defaults to descending.

    Returns:
        pd.DataFrame: The merged (and optionally sorted) DataFrame.
    """
    if not frames:
        return pd.DataFrame()

    # Merge all frames
    merged_df = pd.concat(frames, ignore_index=True)

    # Sort if sort_column is specified and exists in DataFrame
    if sort_column:
        if sort_column in merged_df.columns:
            merged_df: pd.DataFrame = merged_df.sort_values(
                by=sort_column, ascending=ascending, ignore_index=True
            )
        else:
            logger.warning(
                f"Sort column '{sort_column}' not found in the merged DataFrame."
            )

    return merged_df


def reorder_columns(
    data_frame: pd.DataFrame,
    prioritized_columns: list[str] | None = None,
    prefix_order: list[str] | None = None,
    columns_to_end: list[str] | None = None,
) -> pd.DataFrame:
    """
    Reorders columns in a DataFrame based on specified priorities, prefix rules,
    and columns to move to the end.

    Parameters:
        data_frame (pd.DataFrame): The DataFrame to reorder.
        prioritized_columns (Optional[list[str]]): A list of columns to place at the start in order.
            Columns not in the DataFrame will be ignored.
        prefix_order (Optional[list[str]]): A list of prefixes to group and order after prioritized columns.
            Columns matching prefixes will appear in the same order as the prefixes.
        columns_to_move (Optional[list[str]]): A list of columns to move to the end in order.
            Columns not in the DataFrame will be ignored.

    Returns:
        pd.DataFrame: A new DataFrame with reordered columns.
    """
    # Ensure prioritized_columns, prefix_order, and columns_to_move are valid
    if prioritized_columns is None:
        prioritized_columns = []
    if prefix_order is None:
        prefix_order = []
    if columns_to_end is None:
        columns_to_end = []

    # Start with prioritized columns, only include ones in the DataFrame
    ordered_columns = [col for col in prioritized_columns if col in data_frame.columns]

    # Add columns matching the specified prefixes, in prefix order
    for prefix in prefix_order:
        prefixed_cols = sorted(
            [col for col in data_frame.columns if col.startswith(prefix)]
        )
        ordered_columns.extend(prefixed_cols)

    # Exclude columns that are in `columns_to_move` before appending remaining columns
    remaining_cols = [
        col
        for col in data_frame.columns
        if col not in ordered_columns and col not in columns_to_end
    ]
    ordered_columns.extend(sorted(remaining_cols))

    # Finally, append columns_to_move, only including ones in the DataFrame
    ordered_columns.extend([col for col in columns_to_end if col in data_frame.columns])

    # Return DataFrame with reordered columns
    return data_frame[ordered_columns]


def reorder_long_columns(df: pd.DataFrame) -> pd.DataFrame:
    # specific implementation for the culvert and associated processing scripts
    columns_to_move = [
        "file",
        "rel_directory",
        "rel_path",
        "directory_path",
        "path",
    ]

    # move large width column names to the right side
    df = reorder_columns(
        data_frame=df,
        columns_to_end=columns_to_move,
    )
    return df
