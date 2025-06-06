# ryan_library/processors/tuflow/processor_collection.py

from loguru import logger
import pandas as pd
from pandas import DataFrame
from ryan_library.functions.dataframe_helpers import reorder_long_columns
from ryan_library.processors.tuflow.base_processor import BaseProcessor


class ProcessorCollection:
    """A collection of BaseProcessor instances, allowing combined operations based on different scenarios.

    This class holds one or more processed BaseProcessor instances and provides methods to combine their DataFrames
    according to specific merging strategies."""

    def __init__(self) -> None:
        """Initialize an empty ProcessorCollection."""
        self.processors: list[BaseProcessor] = []

    def add_processor(self, processor: BaseProcessor) -> None:
        """Add a processed BaseProcessor instance to the collection.

        Args:
            processor (BaseProcessor): A processed BaseProcessor instance."""
        if processor.processed:
            self.processors.append(processor)
            logger.debug(f"Added processor: {processor.file_name}")
        else:
            logger.warning(
                f"Attempted to add unprocessed processor: {processor.file_name}"
            )

    def reset_categorical_ordering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reset categorical ordering for all categorical columns by sorting categories alphabetically.

        Args:
            df (pd.DataFrame): The DataFrame to reset categorical ordering.

        Returns:
            pd.DataFrame: DataFrame with reset categorical ordering."""
        for col in df.select_dtypes(include="category").columns:
            sorted_categories = sorted(df[col].cat.categories)
            df[col] = df[col].cat.set_categories(sorted_categories, ordered=True)
            logger.debug(
                f"Column '{col}' ordered alphabetically with categories: {sorted_categories}"
            )

        # Reset missing values if necessary
        df.fillna(value=pd.NA, inplace=True)
        logger.debug("Filled missing values with pd.NA.")
        return df

    def combine_1d_timeseries(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'Timeseries'.
        Group data based on 'internalName', 'Chan ID', and 'Time'.

        Returns:
            pd.DataFrame: Combined and grouped DataFrame."""
        logger.debug("Combining 1D Timeseries data.")

        # Filter processors with dataformat 'Timeseries'
        timeseries_processors = [
            p for p in self.processors if p.dataformat.lower() == "timeseries"
        ]

        if not timeseries_processors:
            logger.warning("No processors with dataformat 'Timeseries' found.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df: pd.DataFrame = pd.concat(
            [p.df for p in timeseries_processors if not p.df.empty], ignore_index=True
        )
        logger.debug(f"Combined Timeseries DataFrame with {len(combined_df)} rows.")

        # Columns to drop
        columns_to_drop: list[str] = ["file", "rel_path", "path", "directory_path"]

        # Check for existing columns and drop them
        existing_columns_to_drop: list[str] = [
            col for col in columns_to_drop if col in combined_df.columns
        ]
        if existing_columns_to_drop:
            combined_df.drop(columns=existing_columns_to_drop, inplace=True)
            logger.debug(f"Dropped columns {existing_columns_to_drop} from DataFrame.")

        combined_df = self.reset_categorical_ordering(df=combined_df)
        # Reset categorical ordering
        # Group by 'internalName', 'Chan ID', and 'Time'
        group_keys: list[str] = ["internalName", "Chan ID", "Time"]
        missing_keys: list[str] = [
            key for key in group_keys if key not in combined_df.columns
        ]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Timeseries data.")
            return pd.DataFrame()

        combined_df = reorder_long_columns(combined_df)

        grouped_df: DataFrame = combined_df.groupby(group_keys).agg("max").reset_index()
        logger.debug(
            f"Grouped {len(timeseries_processors)} Timeseries DataFrame with {len(grouped_df)} rows."
        )

        return grouped_df

    def combine_1d_maximums(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'Maximums' or 'ccA'.
        Drop the 'Time' column.
        Group data based on 'internalName' and 'Chan ID'.

        Returns:
            pd.DataFrame: Combined and grouped DataFrame."""
        logger.debug("Combining 1D Maximums and ccA data.")

        # Filter processors with dataformat 'Maximums' or 'ccA'
        maximums_processors: list[BaseProcessor] = [
            p for p in self.processors if p.dataformat.lower() in ["maximums", "cca"]
        ]

        if not maximums_processors:
            logger.warning("No processors with dataformat 'Maximums' or 'ccA' found.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = pd.concat(
            [p.df for p in maximums_processors if not p.df.empty], ignore_index=True
        )
        logger.debug(f"Combined Maximums/ccA DataFrame with {len(combined_df)} rows.")

        # Columns to drop
        columns_to_drop: list[str] = ["file", "rel_path", "path", "Time"]

        # Check for existing columns and drop them
        existing_columns_to_drop: list[str] = [
            col for col in columns_to_drop if col in combined_df.columns
        ]
        if existing_columns_to_drop:
            combined_df.drop(columns=existing_columns_to_drop, inplace=True)
            logger.debug(f"Dropped columns {existing_columns_to_drop} from DataFrame.")

        combined_df = reorder_long_columns(df=combined_df)
        # Reset categorical ordering
        combined_df = self.reset_categorical_ordering(combined_df)

        # Group by 'internalName' and 'Chan ID'
        group_keys: list[str] = ["internalName", "Chan ID"]
        missing_keys: list[str] = [
            key for key in group_keys if key not in combined_df.columns
        ]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Maximums/ccA data.")
            return pd.DataFrame()

        grouped_df: DataFrame = combined_df.groupby(group_keys).agg("max").reset_index()
        logger.debug(
            f"Grouped {len(maximums_processors)} Maximums/ccA DataFrame with {len(grouped_df)} rows."
        )

        return grouped_df

    def combine_raw(self) -> pd.DataFrame:
        """Concatenate all DataFrames together without any grouping.

        Returns:
            pd.DataFrame: Concatenated DataFrame."""
        logger.debug("Combining raw data without grouping.")

        # Concatenate all DataFrames
        combined_df: DataFrame = pd.concat(
            [p.df for p in self.processors if not p.df.empty], ignore_index=True
        )
        logger.debug(f"Combined Raw DataFrame with {len(combined_df)} rows.")

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        combined_df = self.reset_categorical_ordering(combined_df)

        return combined_df

    def pomm_combine(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'POMM'.
        No grouping required as DataFrames are already in the correct format.

        Returns:
            pd.DataFrame: Combined DataFrame."""
        logger.debug("Combining POMM data.")

        # Filter processors with dataformat 'POMM'
        pomm_processors: list[BaseProcessor] = [
            p for p in self.processors if p.dataformat.lower() == "pomm"
        ]

        if not pomm_processors:
            logger.warning("No processors with dataformat 'POMM' found.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = pd.concat(
            [p.df for p in pomm_processors if not p.df.empty], ignore_index=True
        )
        logger.debug(
            f"Combined {len(pomm_processors)}  POMM DataFrame with {len(combined_df)} rows."
        )

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        combined_df = self.reset_categorical_ordering(combined_df)

        return combined_df

    def po_combine(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'PO'.
        No grouping required as DataFrames are already in the correct format.

        Returns:
            pd.DataFrame: Combined DataFrame."""
        logger.debug("Combining PO data.")

        # Filter processors with dataformat 'PO'
        po_processors: list[BaseProcessor] = [
            p for p in self.processors if p.dataformat.lower() == "po"
        ]

        if not po_processors:
            logger.warning("No processors with dataformat 'PO' found.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df = pd.concat(
            [p.df for p in po_processors if not p.df.empty], ignore_index=True
        )
        logger.debug(
            f"Combined {len(po_processors)} PO DataFrame with {len(combined_df)} rows."
        )

        combined_df: DataFrame = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        combined_df = self.reset_categorical_ordering(combined_df)

        return combined_df

    def get_processors_by_data_type(
        self, data_types: list[str] | str
    ) -> "ProcessorCollection":
        """Retrieve processors matching a specific data_type or list of data_types.

        Args:
            data_types (list[str] | str): A data_type or list of data_types to match.

        Returns:
            ProcessorCollection: A new collection of processors with matching data_type(s).
        """

        # Ensure it's always a list for uniform processing
        if isinstance(data_types, str):
            data_types = [data_types]

        filtered_collection = ProcessorCollection()
        for processor in self.processors:
            if processor.data_type in data_types:
                filtered_collection.add_processor(processor)

        logger.debug(
            f"Filtered ProcessorCollection created with {len(filtered_collection.processors)} processors matching data types {data_types}."
        )
        return filtered_collection
