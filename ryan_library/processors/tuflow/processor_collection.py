# ryan_library/processors/tuflow/processor_collection.py

from collections.abc import Collection
import copy
import json
from pathlib import Path
from typing import Any
from loguru import logger
import pandas as pd
from pandas import DataFrame, Series
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

    _BATCH_SIZE: int = 500
    BASIC_INFO_COLUMNS: tuple[str, ...] = ("file", "rel_path", "path", "directory_path", "rel_directory")

    def __init__(self) -> None:
        """Initialize an empty ProcessorCollection."""
        self.processors: list[BaseProcessor] = []
        self.basic_info_lookup: DataFrame | None = None

    def copy(self) -> "ProcessorCollection":
        """Return a deep copy of the collection."""
        new_collection = ProcessorCollection()
        # Deep copy processors to ensure isolation
        new_collection.processors = [copy.deepcopy(p) for p in self.processors]
        if self.basic_info_lookup is not None:
            new_collection.basic_info_lookup = self.basic_info_lookup.copy(deep=True)
        return new_collection

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

    def build_basic_info_lookup(
        self,
        columns: Collection[str] | None = None,
        id_column: str = "processor_id",
    ) -> DataFrame:
        """Build a lookup table for file/path metadata keyed by processor id."""
        if columns is None:
            columns = self.BASIC_INFO_COLUMNS

        rows: list[dict[str, Any]] = []
        for idx, processor in enumerate(self.processors):
            payload = processor.build_basic_info_payload()
            row: dict[str, Any] = {id_column: idx}
            for column in columns:
                if column in payload:
                    row[column] = payload[column]
            rows.append(row)

        lookup_df = pd.DataFrame(data=rows)
        if not lookup_df.empty and id_column in lookup_df.columns:
            lookup_df[id_column] = lookup_df[id_column].astype("Int32")
        return lookup_df

    def compact_basic_info_columns(
        self,
        columns: Collection[str] | None = None,
        id_column: str = "processor_id",
    ) -> DataFrame:
        """Replace repeated file/path columns with a compact processor id reference."""
        if columns is None:
            columns = self.BASIC_INFO_COLUMNS
        columns_to_drop: set[str] = set(columns)

        lookup_df: DataFrame = self.build_basic_info_lookup(columns=columns, id_column=id_column)

        for idx, processor in enumerate(self.processors):
            if processor.df.empty:
                continue
            if id_column in processor.df.columns:
                logger.warning(
                    f"{processor.file_name}: '{id_column}' already present; skipping compaction for this processor."
                )
                continue
            processor.df[id_column] = pd.Series(idx, index=processor.df.index, dtype="Int32")
            drop_columns: list[str] = [column for column in columns_to_drop if column in processor.df.columns]
            if drop_columns:
                processor.df = processor.df.drop(columns=drop_columns)

        self.basic_info_lookup = lookup_df
        return lookup_df

    def attach_basic_info(
        self,
        df: DataFrame,
        id_column: str = "processor_id",
        drop_id: bool = False,
    ) -> DataFrame:
        """Attach compacted file/path columns back onto a DataFrame."""
        if df.empty or self.basic_info_lookup is None or self.basic_info_lookup.empty:
            return df
        if id_column not in df.columns:
            logger.debug(
                "Skipping basic info attach; '{id_column}' column is missing.",
                id_column=id_column,
            )
            return df

        merged_df: DataFrame = df.merge(right=self.basic_info_lookup, on=id_column, how="left")
        if drop_id and id_column in merged_df.columns:
            merged_df.drop(columns=[id_column], inplace=True)
        return merged_df

    def discard_raw_dataframes(self) -> int:
        """Discard raw DataFrames for all processors to reduce memory usage."""
        discarded = 0
        for processor in self.processors:
            processor.discard_raw_dataframe()
            discarded += 1
        return discarded

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

    def _concat_in_batches(self, frames: list[DataFrame]) -> DataFrame:
        """Concatenate frames in smaller batches to reduce peak memory."""
        if not frames:
            return DataFrame()
        batches: list[DataFrame] = []
        for i in range(0, len(frames), self._BATCH_SIZE):
            batch: list[DataFrame] = frames[i : i + self._BATCH_SIZE]
            batches.append(pd.concat(batch, ignore_index=True, copy=False, sort=False))
        if len(batches) == 1:
            return batches[0]
        return pd.concat(batches, ignore_index=True, copy=False, sort=False)

    def combine_1d_timeseries(self, reset_categoricals: bool = True) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'Timeseries'.
        Group data based on 'internalName', 'Chan ID', and 'Time'.

        Args:
            reset_categoricals: Whether to normalize categorical ordering before grouping.

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
        # Prepare static data (EOF + Chan)
        eof_processors: list[BaseProcessor] = [p for p in self.processors if p.data_type == "EOF"]
        chan_processors: list[BaseProcessor] = [p for p in self.processors if p.data_type == "Chan"]

        # Map run_code -> processor/df
        eof_map: dict[str, DataFrame] = {p.name_parser.raw_run_code: p.df for p in eof_processors}
        chan_map: dict[str, DataFrame] = {p.name_parser.raw_run_code: p.df for p in chan_processors}

        dfs_to_concat: list[DataFrame] = []
        for p in timeseries_processors:
            df: DataFrame = p.df
            if df.empty:
                continue

            run_code: str = p.name_parser.raw_run_code

            # Get static data
            eof_df: DataFrame | None = eof_map.get(run_code)
            chan_df: DataFrame | None = chan_map.get(run_code)

            static_df: DataFrame | None = None
            if eof_df is not None and chan_df is not None:
                static_df = self._merge_chan_and_eof(chan_df=chan_df, eof_df=eof_df)
            elif eof_df is not None:
                static_df = eof_df
            elif chan_df is not None:
                static_df = chan_df

            if static_df is not None and "Chan ID" in static_df.columns and "Chan ID" in df.columns:
                # Merge static data into timeseries
                # Use left join to keep all timeseries rows
                # We drop columns from static that are already in timeseries (except join key) to avoid suffixes
                cols_to_use = [c for c in static_df.columns if c not in df.columns or c == "Chan ID"]
                df = df.merge(right=static_df[cols_to_use], on="Chan ID", how="left")

            dfs_to_concat.append(df)

        if not dfs_to_concat:
            logger.warning("No Timeseries data to concatenate.")
            return pd.DataFrame()

        combined_df: pd.DataFrame = self._concat_in_batches(frames=dfs_to_concat)
        logger.debug(f"Combined Timeseries DataFrame with {len(combined_df)} rows.")

        # Columns to drop
        columns_to_drop: list[str] = [
            "file",
            "rel_path",
            "path",
            "directory_path",
            "rel_directory",
            "processor_id",
        ]

        # Check for existing columns and drop them
        existing_columns_to_drop: list[str] = [col for col in columns_to_drop if col in combined_df.columns]
        if existing_columns_to_drop:
            combined_df.drop(columns=existing_columns_to_drop, inplace=True)
            logger.debug(f"Dropped columns {existing_columns_to_drop} from DataFrame.")

        if reset_categoricals:
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
            combined_df.groupby(by=group_keys, observed=True)  # pyright: ignore[reportUnknownMemberType]
            .agg(func="max")
            .reset_index()
        )

        grouped_df = self._calculate_hw_d_ratio(df=grouped_df)

        p1_col: list[str] = [
            "trim_runcode",
            "aep_text",
            "duration_text",
            "tp_text",
            "Chan ID",
            "Time",
            "Q",
            "V",
            "US_h",
            "DS_h",
            "US Invert",
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
        ]

        grouped_df = reorder_columns(
            data_frame=grouped_df,
            prioritized_columns=p1_col,
            prefix_order=["R"],
            second_priority_columns=p2_col,
        )

        logger.debug(f"Grouped {len(timeseries_processors)} Timeseries DataFrame with {len(grouped_df)} rows.")

        return grouped_df

    def combine_1d_maximums(self, reset_categoricals: bool = True) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'Maximums' or 'ccA'.
        Drop the 'Time' column.
        Group data based on 'internalName' and 'Chan ID'.

        Args:
            reset_categoricals: Whether to normalize categorical ordering before grouping.

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

        columns_to_drop: list[str] = [
            "file",
            "rel_path",
            "path",
            "directory_path",
            "rel_directory",
            "processor_id",
            "Time",
        ]
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

            # Drop bulky columns early to save memory before concatenation
            existing_columns_to_drop: list[str] = [col for col in columns_to_drop if col in df.columns]
            if existing_columns_to_drop:
                df = df.drop(columns=existing_columns_to_drop)
            logger.debug(f"Dropped columns {existing_columns_to_drop} from DataFrame.")

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
        combined_df: DataFrame = self._concat_in_batches(frames=dfs_to_concat)
        logger.debug(f"Combined Maximums/ccA DataFrame with {len(combined_df)} rows.")

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        if reset_categoricals:
            combined_df = reset_categorical_ordering(combined_df)

        # Group by 'internalName' and 'Chan ID'
        group_keys: list[str] = ["internalName", "Chan ID"]
        missing_keys: list[str] = [key for key in group_keys if key not in combined_df.columns]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Maximums/ccA data.")
            return pd.DataFrame()

        grouped_df: DataFrame = (
            combined_df.groupby(by=group_keys, observed=True)  # pyright: ignore[reportUnknownMemberType]
            .agg(func="max")
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
        # Determine which column to use for Headwater Level
        us_h_col: str = "US_h" if "US_h" in df.columns else "US_H"

        required_columns: set[str] = {us_h_col, "US Invert", "Height"}
        missing_columns: set[str] = required_columns - set(df.columns)
        if missing_columns:
            logger.debug(f"Skipping HW_D calculation; missing columns: {sorted(missing_columns)}")
            return df

        if df.empty:
            df["HW_D"] = pd.Series(dtype="Float64")
            return df

        us_h_series: Series[float] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
            arg=df[us_h_col], errors="coerce"
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

    def combine_raw(self, reset_categoricals: bool = True) -> pd.DataFrame:
        """Concatenate all DataFrames together without any grouping.

        Args:
            reset_categoricals: Whether to normalize categorical ordering after concatenation.

        Returns:
            pd.DataFrame: Concatenated DataFrame."""
        logger.debug("Combining raw data without grouping.")

        # Concatenate all DataFrames
        combined_df: DataFrame = self._concat_in_batches(frames=[p.df for p in self.processors if not p.df.empty])
        logger.debug(f"Combined Raw DataFrame with {len(combined_df)} rows.")

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        if reset_categoricals:
            combined_df = reset_categorical_ordering(combined_df)

        return combined_df

    def pomm_combine(self, reset_categoricals: bool = True) -> pd.DataFrame:
        """Combine DataFrames where dataformat is 'POMM'.
        No grouping required as DataFrames are already in the correct format.

        Args:
            reset_categoricals: Whether to normalize categorical ordering after concatenation.

        Returns:
            pd.DataFrame: Combined DataFrame."""
        logger.debug("Combining POMM data.")

        # Filter processors with dataformat 'POMM'
        pomm_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "pomm"]

        if not pomm_processors:
            logger.warning("No processors with dataformat 'POMM' found.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = self._concat_in_batches(frames=[p.df for p in pomm_processors if not p.df.empty])
        logger.debug(f"Combined {len(pomm_processors)}  POMM DataFrame with {len(combined_df)} rows.")

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        if reset_categoricals:
            combined_df = reset_categorical_ordering(combined_df)

        return combined_df

    def po_combine(self, reset_categoricals: bool = True) -> pd.DataFrame:
        """Combine processed PO timeseries files into a single tidy DataFrame.

        Args:
            reset_categoricals: Whether to normalize categorical ordering after concatenation.
        """
        logger.debug("Combining PO timeseries data.")

        # Filter processors with dataformat 'PO'
        po_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "po"]

        if not po_processors:
            logger.warning("No PO processors available for combination.")
            return pd.DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = self._concat_in_batches(frames=[p.df for p in po_processors if not p.df.empty])
        logger.debug(f"Combined {len(po_processors)} PO DataFrame with {len(combined_df)} rows.")

        if combined_df.empty:
            logger.warning("PO DataFrames are empty after concatenation.")
            return combined_df

        if reset_categoricals:
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

    def to_hdf(self, file_path: Path | str) -> None:
        """Save the collection to a single HDF5 file using pandas.HDFStore.

        Args:
            file_path: Destination HDF5 file path.
        """

        file_path = Path(file_path)
        # Ensure parent exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        metadata: list[dict[str, Any]] = []

        # User requested blosc:zstd level 9
        # complib type hint in pandas can be strict, but 'blosc:zstd' is valid at runtime.
        with pd.HDFStore(
            str(file_path), mode="w", complevel=9, complib="blosc:zstd"  # pyright: ignore[reportArgumentType]
        ) as store:
            for idx, proc in enumerate(self.processors):
                # We need a unique key for each processor df
                key = f"proc_{idx:04d}"

                proc_meta = {
                    "file_path": str(proc.file_path),
                    "class_name": proc.__class__.__name__,
                    "processor_module": proc.processor_module,
                    "dataformat": proc.dataformat,
                    "hdf_key": key,
                    "data_type": proc.data_type,
                    "applied_location_filter": (
                        list(proc.applied_location_filter) if proc.applied_location_filter else None
                    ),
                    "applied_entity_filter": (list(proc.applied_entity_filter) if proc.applied_entity_filter else None),
                }
                metadata.append(proc_meta)

                if not proc.df.empty:
                    # Using fixed format for speed as "cache".
                    # If querying is needed, 'table' is better but slower/larger.
                    store.put(key, proc.df, format="fixed")

            meta_df = pd.DataFrame({"json": [json.dumps(metadata)]})
            store.put("metadata", meta_df)

        logger.info(f"Saved {len(self.processors)} processors to {file_path}")

    @staticmethod
    def from_hdf(file_path: Path | str, locations: Collection[str] | None = None) -> "ProcessorCollection":
        """Load a collection from a single HDF5 file.

        Args:
            file_path: Source HDF5 file.
            locations: Optional location filter.

        Returns:
            ProcessorCollection: Rehydrated collection.
        """

        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"HDF5 file not found: {file_path}")

        collection = ProcessorCollection()

        with pd.HDFStore(str(file_path), mode="r") as store:
            if "metadata" not in store:
                raise KeyError("HDF5 file missing 'metadata' key.")

            meta_df = store.get("metadata")
            metadata: list[dict[str, Any]] = json.loads(
                meta_df.iloc[0]["json"]
            )  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]

            for item in metadata:
                file_path_str = item["file_path"]
                class_name = item["class_name"]
                hdf_key = item["hdf_key"]

                try:
                    proc_cls = BaseProcessor.get_processor_class(
                        class_name=class_name,
                        processor_module=item.get("processor_module"),
                        dataformat=item.get("dataformat"),
                    )

                    proc = proc_cls(file_path=Path(file_path_str))

                    if hdf_key in store:
                        proc.df = store.get(hdf_key)  # pyright: ignore[reportUnknownMemberType]
                        proc.processed = True
                    else:
                        proc.df = pd.DataFrame()
                        proc.processed = True

                    if item.get("applied_location_filter"):
                        proc.applied_location_filter = frozenset(item["applied_location_filter"])
                    if item.get("applied_entity_filter"):
                        proc.applied_entity_filter = frozenset(item["applied_entity_filter"])

                    collection.add_processor(proc)

                except Exception as e:
                    logger.error(f"Failed to rehydrate processor for {file_path_str}: {e}")
                    continue

        if locations:
            collection.filter_locations(locations)

        logger.info(f"Loaded {len(collection.processors)} processors from HDF5: {file_path}")
        return collection

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
        logger.debug(
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
