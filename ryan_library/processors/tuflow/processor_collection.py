# ryan_library/processors/tuflow/processor_collection.py

from collections.abc import Collection
from math import pi
from loguru import logger
import pandas as pd
from pandas import DataFrame, Series
from pandas import Index
from ryan_library.functions.dataframe_helpers import (
    reorder_columns,
    reorder_long_columns,
    reset_categorical_ordering,
)
from .base_processor import BaseProcessor


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

        grouped_df: DataFrame = (
            combined_df.groupby(group_keys, observed=True)
            .agg("max")
            .reset_index()  # pyright: ignore[reportUnknownMemberType]
        )
        logger.debug(f"Grouped {len(timeseries_processors)} Timeseries DataFrame with {len(grouped_df)} rows.")

        return grouped_df

    def combine_1d_maximums(self) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'Maximums' or 'ccA'.
        Drop the 'Time' column.
        Group data based on 'internalName' and 'Chan ID'.

        Returns:
            pd.DataFrame: Combined and grouped DataFrame."""
        logger.debug("Combining 1D Maximums/ccA data.")

        # Filter processors with dataformat 'Maximums' or 'ccA'
        maximums_processors: list[BaseProcessor] = [
            p for p in self.processors if p.dataformat.lower() in ["maximums", "cca"]
        ]

        if not maximums_processors:
            logger.warning("No processors with dataformat 'Maximums' or 'ccA' found.")
            return pd.DataFrame()

        # Identify EOF processors for merging
        eof_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "eof"]
        eof_map: dict[str, DataFrame] = {p.name_parser.raw_run_code: p.df for p in eof_processors}
        merged_run_codes: set[str] = set()

        dfs_to_concat: list[DataFrame] = []
        for processor in maximums_processors:
            df: DataFrame = processor.df
            if df.empty:
                continue

            run_code: str = processor.name_parser.raw_run_code
            eof_df: DataFrame | None = eof_map.get(run_code)
            if eof_df is not None:
                df = self._merge_with_eof_data(
                    source_df=df,
                    eof_df=eof_df,
                    source_label=processor.data_type,
                    run_code=run_code,
                )
                merged_run_codes.add(run_code)

            dfs_to_concat.append(df)

        # Ensure EOF-only runs (no matching maximums/ccA files) are still retained so geometry is not lost.
        # TODO - that comment does not make sense - we do not keep any geometry anyway. only attributes. check logic and workflow.
        for run_code, eof_df in eof_map.items():
            if eof_df.empty or run_code in merged_run_codes:
                continue
            logger.info(f"Including EOF-only data for run code {run_code} with no associated maximum datasets.")
            dfs_to_concat.append(eof_df)

        if not dfs_to_concat:
            logger.warning("No data to concatenate after filtering.")
            return pd.DataFrame()

        # Concatenate DataFrames (with EOF geometry merged in above when available)
        # TODO - there is no geometry? there should only be data columns from the attributes
        combined_df: DataFrame = pd.concat(dfs_to_concat, ignore_index=True)
        logger.debug(f"Combined Maximums/ccA DataFrame with {len(combined_df)} rows.")

        # Columns to drop
        columns_to_drop: list[str] = ["file", "rel_path", "path", "Time"]

        # Check for existing columns and drop them
        existing_columns_to_drop: list[str] = [col for col in columns_to_drop if col in combined_df.columns]
        if existing_columns_to_drop:
            combined_df.drop(columns=existing_columns_to_drop, inplace=True)
            logger.debug(f"Dropped columns {existing_columns_to_drop} from DataFrame.")

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        combined_df = reset_categorical_ordering(combined_df)

        # Group by 'internalName' and 'Chan ID'
        group_keys: list[str] = ["internalName", "Chan ID"]
        missing_keys: list[str] = [key for key in group_keys if key not in combined_df.columns]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Maximums/ccA data.")
            return pd.DataFrame()

        grouped_df: DataFrame = (
            combined_df.groupby(by=group_keys, observed=False)  # pyright: ignore[reportUnknownMemberType]
            .agg("max")
            .reset_index()
        )
        grouped_df = self._calculate_hw_d_ratio(df=grouped_df)
        p1_col: list[str] = [
            "trim_runcode",
            "aep_text",
            "duration_text",
            "tp_text",
            "Chan ID",
            "Q",
            "V",
            "US_h",
            "DS_h",
            "US Invert",
            "US Obvert",
            "DS Invert",
            "Flags",
            "Diam_Width",
            "Height",
            "Num_barrels",
            "HW_D",
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
            "pFull_Max",
            "pTime_Full",
            "Area_Culv",
            "Dur_Full",
            "Dur_10pFull",
        ]
        logger.debug("adjusting df columns")
        grouped_df = reorder_columns(
            data_frame=grouped_df,
            prioritized_columns=p1_col,
            prefix_order=["R"],
            second_priority_columns=p2_col,
        )
        logger.debug(f"Grouped {len(maximums_processors)} Maximums/ccA DataFrame with {len(grouped_df)} rows.")
        return grouped_df

    def _calculate_hw_d_ratio(self, df: DataFrame) -> DataFrame:
        """Calculate the HW_D ratio = (US_h - US Invert) / Height."""
        required_columns: set[str] = {"US_h", "US Invert", "Height"}
        missing_columns: set[str] = required_columns - set(df.columns)
        if missing_columns:
            logger.debug(f"Skipping HW_D calculation; missing columns: {sorted(missing_columns)}")
            return df

        if df.empty:
            df["HW_D"] = pd.Series(dtype="Float64")
            return df

        us_h_series: Series[float] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
            arg=df["US_h"], errors="coerce"
        )
        us_invert_series: Series = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
            arg=df["US Invert"], errors="coerce"
        )
        height_series: Series = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
            arg=df["Height"], errors="coerce"
        )

        valid_mask: Series[bool] = (
            us_h_series.notna() & us_invert_series.notna() & height_series.notna() & (height_series != 0)
        )

        hw_d_series: Series = pd.Series(data=pd.NA, index=df.index, dtype="Float64")
        valid_count: int = int(valid_mask.sum())

        if not valid_mask.any():
            df["HW_D"] = hw_d_series
            logger.debug("HW_D calculation skipped; insufficient valid data.")
            return df

        hw_d_series.loc[valid_mask] = (
            us_h_series.loc[valid_mask] - us_invert_series.loc[valid_mask]
        ) / height_series.loc[valid_mask]

        df["HW_D"] = hw_d_series
        logger.debug(f"Calculated HW_D ratio for {valid_count} of {df['Chan ID'].count()} rows.")
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
        """Combine processed PO timeseries files into a single tidy DataFrame."""
        logger.debug("Combining PO timeseries data.")

        # Filter processors with dataformat 'PO'
        po_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "po"]

        if not po_processors:
            logger.warning("No PO processors available for combination.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = pd.concat([p.df for p in po_processors if not p.df.empty], ignore_index=True)
        logger.debug(f"Combined {len(po_processors)} PO DataFrame with {len(combined_df)} rows.")

        if combined_df.empty:
            logger.warning("PO DataFrames are empty after concatenation.")
            return combined_df

        combined_df = reset_categorical_ordering(df=combined_df)
        combined_df = reorder_long_columns(df=combined_df)

        sort_columns: list[str] = [
            column for column in ["internalName", "Location", "Type", "Time"] if column in combined_df.columns
        ]
        if sort_columns:
            combined_df.sort_values(by=sort_columns, inplace=True)
            combined_df.reset_index(drop=True, inplace=True)

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
            A dict mapping (run_code, data_type) -> list of processors. Only entries
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
        duplicates: dict[tuple[str, str], list[BaseProcessor]] = {k: v for k, v in groups.items() if len(v) > 1}

        if duplicates:
            for (run_code, dtype), procs in duplicates.items():
                files: str = ", ".join(p.file_name for p in procs)
                logger.warning(
                    f"Potential duplicate group: run_code='{run_code}', data_type='{dtype}' found in files: {files}"
                )
        else:
            logger.debug("No duplicate processors found by run_code & data_type.")

        return duplicates

    def _merge_with_eof_data(
        self, source_df: pd.DataFrame, eof_df: pd.DataFrame, *, source_label: str, run_code: str
    ) -> pd.DataFrame:
        """Merge EOF geometry/metadata into another maximum dataset.

        EOF files often contain authoritative culvert geometry that should be
        carried into maximum datasets regardless of the originating processor.
        """
        if source_df.empty:
            return source_df
        if eof_df.empty:
            return source_df

        if "Chan ID" not in source_df.columns:
            logger.debug(
                "Skipping EOF merge for {} (run code {}): 'Chan ID' not present in source columns {}",
                source_label,
                run_code,
                list(source_df.columns),
            )
            return source_df

        if "Chan ID" not in eof_df.columns:
            logger.warning(f"EOF dataset for run code {run_code} missing 'Chan ID'; cannot merge.")
            return source_df

        merged_df: pd.DataFrame = self._merge_chan_and_eof(chan_df=source_df, eof_df=eof_df)
        logger.info(
            f"Merged EOF data into {source_label} dataset for run code {run_code}; row count now {len(merged_df)}."
        )
        return merged_df

    @staticmethod
    def _merge_chan_and_eof(chan_df: pd.DataFrame, eof_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge a maximum dataset with EOF data keyed by ``Chan ID`` (EOF wins).

        Args:
            chan_df (pd.DataFrame): DataFrame from a maximums-style processor.
            eof_df (pd.DataFrame): DataFrame from EofProcessor.

        Returns:
            pd.DataFrame: Merged DataFrame.
        """
        if chan_df.empty:
            return eof_df
        if eof_df.empty:
            return chan_df

        if "Chan ID" not in chan_df.columns or "Chan ID" not in eof_df.columns:
            logger.warning("Chan ID missing in one of the dataframes, cannot merge.")
            return chan_df

        # Set index to Chan ID for both
        chan_indexed: DataFrame = chan_df.set_index(keys="Chan ID")
        eof_indexed: DataFrame = eof_df.set_index(keys="Chan ID")

        # combine_first: updates null elements in 'other' with value in 'caller'.
        # We want EOF to overwrite Chan.
        # So we call eof.combine_first(chan).
        # This keeps EOF values if present (even if Chan has value).
        # If EOF is null, it takes from Chan.
        # If EOF row is missing, it takes row from Chan.

        merged_indexed: DataFrame = eof_indexed.combine_first(chan_indexed)

        # Reset index
        merged_df: DataFrame = merged_indexed.reset_index()

        return merged_df
