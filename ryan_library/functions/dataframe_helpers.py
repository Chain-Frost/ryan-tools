# ryan_library/functions/dataframe_helpers.py

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
            merged_df: pd.DataFrame = merged_df.sort_values(by=sort_column, ascending=ascending, ignore_index=True)
        else:
            logger.warning(f"Sort column '{sort_column}' not found in the merged DataFrame.")

    return merged_df


def reorder_columns(
    data_frame: pd.DataFrame,
    prioritized_columns: list[str] | None = None,
    prefix_order: list[str] | None = None,
    second_priority_columns: list[str] | None = None,
    columns_to_end: list[str] | None = None,
) -> pd.DataFrame:
    """Reorders columns in a DataFrame based on specified priorities, prefix rules,
    second priority columns, and columns to move to the end.
    Parameters:
        data_frame (pd.DataFrame): The DataFrame to reorder.
        prioritized_columns (Optional[list[str]]): A list of columns to place at the start in order.
            Columns not in the DataFrame will be ignored.
        prefix_order (Optional[list[str]]): A list of prefixes to group and order after prioritized columns.
            Columns matching prefixes will appear in the same order as the prefixes.
        second_priority_columns (Optional[list[str]]): A list of columns to place after prefix-based columns.
            Columns not in the DataFrame will be ignored.
        columns_to_end (Optional[list[str]]): A list of columns to move to the end in order.
            Columns not in the DataFrame will be ignored.
    Returns:
        pd.DataFrame: A new DataFrame with reordered columns."""
    # Ensure all parameters are lists
    if prioritized_columns is None:
        prioritized_columns = []
    if prefix_order is None:
        prefix_order = []
    if second_priority_columns is None:
        second_priority_columns = []
    if columns_to_end is None:
        columns_to_end = []

    # Step 1: Start with prioritized columns, only include ones in the DataFrame
    ordered_columns: list[str] = [col for col in prioritized_columns if col in data_frame.columns]

    # Step 2: Add columns matching the specified prefixes, in prefix order
    for prefix in prefix_order:
        # Match columns that start with the prefix
        prefixed_cols: list[str] = sorted([col for col in data_frame.columns if col.startswith(prefix)])
        ordered_columns.extend(prefixed_cols)

    # Step 3: Add second priority columns, only if they exist
    ordered_columns.extend([col for col in second_priority_columns if col in data_frame.columns])

    # Step 4: Exclude columns that are in `columns_to_end` before appending remaining columns
    remaining_cols: list[str] = [
        col for col in data_frame.columns if col not in ordered_columns and col not in columns_to_end
    ]
    ordered_columns.extend(sorted(remaining_cols))

    # Step 5: Finally, append columns_to_end, only including ones in the DataFrame
    ordered_columns.extend([col for col in columns_to_end if col in data_frame.columns])

    # Log the final column order for debugging
    logger.debug(f"Final column order: {ordered_columns}")

    # Return DataFrame with reordered columns accordingly
    return data_frame[ordered_columns]


def reorder_long_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Specific implementation for the culvert and associated processing scripts.
    Moves large width column names to the right side.

    Parameters:
        df (pd.DataFrame): The DataFrame to reorder.

    Returns:
        pd.DataFrame: The reordered DataFrame."""
    columns_to_move: list[str] = [
        "file",
        "rel_directory",
        "rel_path",
        "directory_path",
        "path",
    ]

    # Move specified columns to the end
    df = reorder_columns(
        data_frame=df,
        columns_to_end=columns_to_move,
    )
    return df


def reset_categorical_ordering(df: pd.DataFrame) -> pd.DataFrame:
    """Reset categorical ordering for all categorical columns by sorting categories alphabetically.

    Args:
        df (pd.DataFrame): The DataFrame to reset categorical ordering.

    Returns:
        pd.DataFrame: DataFrame with reset categorical ordering."""
    for col in df.select_dtypes(include="category").columns:
        sorted_categories: list[str] = sorted(df[col].cat.categories)
        df[col] = df[col].cat.set_categories(new_categories=sorted_categories, ordered=True)
        logger.debug(f"Column '{col}' ordered alphabetically with categories: {sorted_categories}")

    # Reset missing values if necessary
    df.fillna(value=pd.NA, inplace=True)  # pyright: ignore[reportUnknownMemberType]
    logger.debug("Filled missing values with pd.NA.")
    return df
