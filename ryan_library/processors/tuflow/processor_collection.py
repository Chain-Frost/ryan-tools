# ryan_library/processors/tuflow/processor_collection.py

from collections.abc import Collection
from loguru import logger
import pandas as pd
from pandas import DataFrame, Series
from ryan_library.functions.dataframe_helpers import (
    reorder_columns,
    reorder_long_columns,
    reset_categorical_ordering,
)
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
        if processor.processed and not processor.df.empty:
            self.processors.append(processor)
            logger.debug(f"Added processor: {processor.file_name}")
        elif processor.processed:
            logger.info(
                f"{processor.file_name}: Processor completed but has no rows after filtering; skipping add to collection."
            )
        else:
            logger.warning(f"Attempted to add unprocessed processor: {processor.file_name}")

    def filter_locations(self, locations: Collection[str] | None) -> frozenset[str]:
        """Apply a location filter to all processors in the collection.

        Args:
            locations: Collection of location identifiers to retain.

        Returns:
            frozenset[str]: Normalized location identifiers that were applied.
        """

        normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations)
        if not normalized_locations:
            return normalized_locations

        total_before: int = sum(len(processor.df) for processor in self.processors)

        for processor in self.processors:
            if processor.applied_location_filter == normalized_locations:
                continue
            processor.filter_locations(normalized_locations)

        filtered_processors: list[BaseProcessor] = [
            processor for processor in self.processors if not processor.df.empty
        ]
        removed_processors: int = len(self.processors) - len(filtered_processors)
        self.processors = filtered_processors

        total_after: int = sum(len(processor.df) for processor in self.processors)

        log_method = logger.debug if total_before == total_after and removed_processors == 0 else logger.info
        log_method(
            "Applied location filter to {processor_count} processors. Rows reduced from {before} to {after}.",
            processor_count=len(self.processors),
            before=total_before,
            after=total_after,
        )

        if removed_processors:
            logger.info(
                "Removed {removed} processors with no remaining rows after location filtering.",
                removed=removed_processors,
            )

        return normalized_locations

    def combine_1d_timeseries(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'Timeseries'.
        Group data based on 'internalName', 'Chan ID', and 'Time'.

        Returns:
            pd.DataFrame: Combined and grouped DataFrame."""
        logger.debug("Combining 1D Timeseries data.")

        # Filter processors with dataformat 'Timeseries'
        timeseries_processors: list[BaseProcessor] = [
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
        existing_columns_to_drop: list[str] = [col for col in columns_to_drop if col in combined_df.columns]
        if existing_columns_to_drop:
            combined_df.drop(columns=existing_columns_to_drop, inplace=True)
            logger.debug(f"Dropped columns {existing_columns_to_drop} from DataFrame.")

        combined_df = reset_categorical_ordering(df=combined_df)
        # Reset categorical ordering
        # Group by 'internalName', 'Chan ID', and 'Time'
        group_keys: list[str] = ["internalName", "Chan ID", "Time"]
        missing_keys: list[str] = [key for key in group_keys if key not in combined_df.columns]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Timeseries data.")
            return pd.DataFrame()

        combined_df = reorder_long_columns(df=combined_df)

        grouped_df: DataFrame = combined_df.groupby(group_keys).agg("max").reset_index()
        logger.debug(f"Grouped {len(timeseries_processors)} Timeseries DataFrame with {len(grouped_df)} rows.")

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
        combined_df: DataFrame = pd.concat([p.df for p in maximums_processors if not p.df.empty], ignore_index=True)
        logger.debug(f"Combined Maximums/ccA DataFrame with {len(combined_df)} rows.")

        # Columns to drop
        columns_to_drop: list[str] = ["file", "rel_path", "path", "Time"]

        # Check for existing columns and drop them
        existing_columns_to_drop: list[str] = [col for col in columns_to_drop if col in combined_df.columns]
        if existing_columns_to_drop:
            combined_df.drop(columns=existing_columns_to_drop, inplace=True)
            logger.debug(f"Dropped columns {existing_columns_to_drop} from DataFrame.")

        combined_df = reorder_long_columns(df=combined_df)
        combined_df = self._ensure_location_identifier(df=combined_df)
        # Reset categorical ordering
        combined_df = reset_categorical_ordering(combined_df)

        if "Location ID" not in combined_df.columns:
            logger.error("Location ID column is missing after maximums preprocessing.")
            return pd.DataFrame()

        missing_location_mask: Series[bool] = combined_df["Location ID"].isna()
        if missing_location_mask.any():
            logger.warning(
                "Dropping {count} rows without a location identifier from Maximums/ccA data.",
                count=int(missing_location_mask.sum()),
            )
            combined_df = combined_df[~missing_location_mask]
        if combined_df.empty:
            logger.error("No Maximums/ccA data remaining after removing rows without location identifiers.")
            return pd.DataFrame()

        # Group by 'internalName' and the derived 'Location ID'
        group_keys: list[str] = ["internalName", "Location ID"]
        missing_keys: list[str] = [key for key in group_keys if key not in combined_df.columns]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Maximums/ccA data.")
            return pd.DataFrame()

        grouped_df: DataFrame = combined_df.groupby(by=group_keys, observed=False).agg("max").reset_index()
        p1_col: list[str] = [
            "trim_runcode",
            "aep_text",
            "duration_text",
            "tp_text",
            "Location ID",
            "Chan ID",
            "ID",
            "Q",
            "V",
            "DS_h",
            "US_h",
            "US Invert",
            "US Obvert",
            "DS Invert",
            "Flags",
            "Height",
            "Length",
        ]

        p2_col: list[str] = [
            "aep_numeric",
            "duration_numeric",
            "tp_numeric",
            "internalName",
            "pBlockage",
            "pSlope",
            "n or Cd",
        ]
        logger.debug("adjusting df columns")
        grouped_df = reorder_columns(
            data_frame=grouped_df,
            prioritized_columns=p1_col,
            prefix_order=["R"],
            second_priority_columns=p2_col,
        )
        logger.debug(f"Grouped {len(maximums_processors)} Maximums/ccA DataFrame with {len(grouped_df)} rows.")
        logger.debug("line157")
        return grouped_df

    def _ensure_location_identifier(self, df: DataFrame) -> DataFrame:
        """Ensure a 'Location ID' column exists for grouping maximum datasets."""
        if df.empty:
            df["Location ID"] = pd.Series(dtype="string")
            return df

        candidate_columns: list[str] = ["Chan ID", "ID", "Location"]
        available_columns: list[str] = [col for col in candidate_columns if col in df.columns]

        if not available_columns:
            logger.error("No location-based columns available to derive 'Location ID'.")
            df["Location ID"] = pd.Series(pd.NA, index=df.index, dtype="string")
            return df

        location_series: pd.Series = pd.Series(pd.NA, index=df.index, dtype="string")
        for column in available_columns:
            source_values: Series[str] = df[column].astype("string")
            mask: Series[bool] = location_series.isna() & source_values.notna()
            if mask.any():
                location_series.loc[mask] = source_values.loc[mask]

        df["Location ID"] = location_series
        return df

    def combine_raw(self) -> pd.DataFrame:
        """Concatenate all DataFrames together without any grouping.

        Returns:
            pd.DataFrame: Concatenated DataFrame."""
        logger.debug("Combining raw data without grouping.")

        # Concatenate all DataFrames
        combined_df: DataFrame = pd.concat([p.df for p in self.processors if not p.df.empty], ignore_index=True)
        logger.debug(f"Combined Raw DataFrame with {len(combined_df)} rows.")

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        combined_df = reset_categorical_ordering(combined_df)

        return combined_df

    def pomm_combine(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'POMM'.
        No grouping required as DataFrames are already in the correct format.

        Returns:
            pd.DataFrame: Combined DataFrame."""
        logger.debug("Combining POMM data.")

        # Filter processors with dataformat 'POMM'
        pomm_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "pomm"]

        if not pomm_processors:
            logger.warning("No processors with dataformat 'POMM' found.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = pd.concat([p.df for p in pomm_processors if not p.df.empty], ignore_index=True)
        logger.debug(f"Combined {len(pomm_processors)}  POMM DataFrame with {len(combined_df)} rows.")

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        combined_df = reset_categorical_ordering(combined_df)

        return combined_df

    def po_combine(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'PO'.
        No grouping required as DataFrames are already in the correct format.

        Returns:
            pd.DataFrame: Combined DataFrame."""
        logger.debug("Combining PO data.")

        # Filter processors with dataformat 'PO'
        po_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "po"]

        if not po_processors:
            logger.warning("No processors with dataformat 'PO' found.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df = pd.concat([p.df for p in po_processors if not p.df.empty], ignore_index=True)
        logger.debug(f"Combined {len(po_processors)} PO DataFrame with {len(combined_df)} rows.")

        combined_df: DataFrame = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        combined_df = reset_categorical_ordering(combined_df)

        return combined_df

    def get_processors_by_data_type(self, data_types: list[str] | str) -> "ProcessorCollection":
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

    def check_duplicates(self) -> dict[tuple[str, str], list[BaseProcessor]]:
        """Identify processors that share the same run-code (internalName) and data_type.

        Returns:
            A dict mapping (run_code, data_type) → list of processors. Only entries
            where more than one processor share the same key are returned.

        # coll = ProcessorCollection()
        # for p in processors:
        #     coll.add_processor(p)

        # dupes = coll.check_duplicates()
        # if dupes:
        #     # maybe raise, or filter them out, or alert the user"""
        from collections import defaultdict

        groups: dict[tuple[str, str], list[BaseProcessor]] = defaultdict(list)

        for proc in self.processors:
            # use the raw run code as the internalName
            run_code: str = proc.name_parser.raw_run_code
            key: tuple[str, str] = (run_code, proc.data_type)
            groups[key].append(proc)

        # filter to only “duplicates”
        duplicates = {k: v for k, v in groups.items() if len(v) > 1}

        if duplicates:
            for (run_code, dtype), procs in duplicates.items():
                files = ", ".join(p.file_name for p in procs)
                logger.warning(
                    f"Potential duplicate group: run_code='{run_code}', " f"data_type='{dtype}' found in files: {files}"
                )
        else:
            logger.debug("No duplicate processors found by run_code & data_type.")

        return duplicates
