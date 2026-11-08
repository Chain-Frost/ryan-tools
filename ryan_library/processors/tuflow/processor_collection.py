# ryan_library/processors/tuflow/processor_collection.py

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]
import copy
import hashlib
import importlib
import json
import os
import re
import tempfile
from collections.abc import Collection, Hashable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from loguru import logger
from pandas import NA, DataFrame, MultiIndex, Series, concat, to_numeric

from ryan_library.functions.dataframe_helpers import (
    reorder_columns,
    reorder_long_columns,
    reset_categorical_ordering,
)

from .base_processor import BaseProcessor


class ProcessorCollection:
    """A collection of BaseProcessor instances, allowing combined operations based on different scenarios.

    This class holds one or more processed BaseProcessor instances and provides methods to combine their DataFrames
    according to specific merging strategies.
    """

    _BATCH_SIZE: int = 500
    BASIC_INFO_COLUMNS: tuple[str, ...] = ("file", "rel_path", "path", "directory_path", "rel_directory")

    def __init__(self) -> None:
        """Initialize an empty ProcessorCollection."""
        self.processors: list[BaseProcessor] = []
        self.basic_info_lookup: DataFrame | None = None

    def copy(self) -> ProcessorCollection:
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
            processor (BaseProcessor): A processed BaseProcessor instance.
        """
        if processor.processed and not processor.df.empty:
            self.processors.append(processor)
            logger.debug("Added processor: {}", processor.file_name)
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

        lookup_df = DataFrame(data=rows)
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
            processor.df[id_column] = Series(idx, index=processor.df.index, dtype="Int32")
            drop_columns: list[str] = [column for column in columns_to_drop if column in processor.df.columns]
            if drop_columns:
                processor.df = processor.df.drop(columns=drop_columns)

        self.basic_info_lookup = lookup_df
        return lookup_df

    def align_eof_channel_ids(self) -> int:
        """Replace shortened EOF channel IDs with full IDs from matching run outputs.

        TUFLOW EOF reports can truncate long channel IDs when writing the text
        report table. In the MLTP data this appears as values such as
        ``MLETP 159.975`` being printed as ``MLETP 159.97``. The CSV/GPKG result
        outputs retain the full model ID, so we use those result IDs as the
        authoritative labels and only use the truncated EOF label as a lookup
        key. This prevents duplicate short-ID rows in combined culvert summaries.
        """
        source_maps: dict[str, dict[str, str]] = {}
        ambiguous_keys: dict[str, set[str]] = {}

        # First pass: build a per-run lookup from "what the EOF report would
        # print" to the full channel ID seen in non-EOF result files. If two
        # different full IDs collapse to the same printed key, leave that key
        # unresolved rather than guessing.
        for processor in self.processors:
            if processor.data_type == "EOF" or "Chan ID" not in processor.df.columns:
                continue

            run_code: str = processor.name_parser.raw_run_code
            run_map: dict[str, str] = source_maps.setdefault(run_code, {})
            run_ambiguous: set[str] = ambiguous_keys.setdefault(run_code, set())

            seen_values: set[str] = set()
            for raw_value in processor.df["Chan ID"].to_numpy(dtype=object):
                if raw_value is None or raw_value is NA:
                    continue
                value = str(raw_value).strip()
                if not value or value in seen_values:
                    continue
                seen_values.add(value)

                key: str | None = self._channel_match_key(value=value)
                if key is None or key in run_ambiguous:
                    continue

                existing_value: str | None = run_map.get(key)
                if existing_value is None:
                    run_map[key] = value
                elif existing_value != value:
                    run_map.pop(key, None)
                    run_ambiguous.add(key)

        # Second pass: rewrite only EOF processor IDs. Non-EOF processors already
        # contain the full model labels and remain unchanged.
        changed_count = 0
        for processor in self.processors:
            if processor.data_type != "EOF" or "Chan ID" not in processor.df.columns:
                continue
            if processor.applied_location_filter:
                # Selection may have removed conflicting full IDs. Retain the
                # alignment established before filtering instead of guessing.
                continue

            run_code = processor.name_parser.raw_run_code
            run_map = source_maps.get(run_code, {})
            if not run_map:
                continue

            aligned_values: list[object] = []
            processor_changed_count = 0
            for value in processor.df["Chan ID"].astype("string").to_numpy(dtype=object):
                if value is None or value is NA:
                    aligned_values.append(NA)
                    continue

                text_value = str(value).strip()
                key = self._channel_match_key(value=text_value)
                aligned_value: str = run_map.get(key, text_value) if key is not None else text_value
                if aligned_value != text_value:
                    processor_changed_count += 1
                aligned_values.append(aligned_value)

            if processor_changed_count:
                processor.df["Chan ID"] = Series(data=aligned_values, index=processor.df.index, dtype="string")
                changed_count += processor_changed_count
                logger.info(
                    f"{processor.file_name}: Aligned {processor_changed_count} EOF channel IDs to full result IDs."
                )

        return changed_count

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
            merged_df = merged_df.drop(columns=[id_column])
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

        if any(processor.data_type == "EOF" for processor in self.processors):
            # Resolve shortened labels against all available channel IDs before
            # removing rows, while ambiguity information is still complete.
            self.align_eof_channel_ids()

        total_before: int = sum(len(processor.df) for processor in self.processors)

        for processor in self.processors:
            if processor.applied_location_filter == normalized_locations:
                continue
            processor.filter_locations(normalized_locations)

        filtered_processors: list[BaseProcessor] = [
            processor
            for processor in self.processors
            if not processor.df.empty or (processor.selected_unusable and processor.input_health == "INVALID")
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
                f"Removed {removed_processors} processors with no remaining rows after location filtering.",
            )

        return normalized_locations

    def _concat_in_batches(self, frames: list[DataFrame]) -> DataFrame:
        """Concatenate frames in smaller batches to reduce peak memory."""
        if not frames:
            return DataFrame()
        batches: list[DataFrame] = []
        for i in range(0, len(frames), self._BATCH_SIZE):
            batch: list[DataFrame] = frames[i : i + self._BATCH_SIZE]
            batches.append(concat(batch, ignore_index=True, sort=False))
        if len(batches) == 1:
            return batches[0]
        return concat(batches, ignore_index=True, sort=False)

    def combine_1d_timeseries(self, reset_categoricals: bool = True) -> DataFrame:
        """Combine DataFrames where dataformat is 'Timeseries'.
        Group data based on 'internalName', 'Chan ID', and 'Time'.

        Args:
            reset_categoricals: Whether to normalize categorical ordering before grouping.

        Returns:
            DataFrame: Combined and grouped DataFrame.
        """
        logger.debug("Combining 1D Timeseries data.")
        self.align_eof_channel_ids()

        # Filter processors with dataformat 'Timeseries'
        timeseries_processors: list[BaseProcessor] = [
            p for p in self.processors if p.dataformat.lower() == "timeseries"
        ]

        if not timeseries_processors:
            logger.warning("No processors with dataformat 'Timeseries' found.")
            return DataFrame()

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
            return DataFrame()

        combined_df: DataFrame = self._concat_in_batches(frames=dfs_to_concat)
        logger.debug("Combined Timeseries DataFrame with {} rows.", len(combined_df))

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
            combined_df = combined_df.drop(columns=existing_columns_to_drop)
            logger.debug("Dropped columns {} from DataFrame.", existing_columns_to_drop)

        if reset_categoricals:
            combined_df = reset_categorical_ordering(df=combined_df)
        # Reset categorical ordering
        # Group by 'internalName', 'Chan ID', and 'Time'
        group_keys: list[str] = ["internalName", "Chan ID", "Time"]
        missing_keys: list[str] = [key for key in group_keys if key not in combined_df.columns]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Timeseries data.")
            return DataFrame()

        combined_df = reorder_long_columns(df=combined_df)

        grouped_df: DataFrame = combined_df.groupby(by=group_keys, observed=True).agg(func="max").reset_index()

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

        logger.debug("Grouped {} Timeseries DataFrame with {} rows.", len(timeseries_processors), len(grouped_df))

        return grouped_df

    def combine_1d_maximums(self, reset_categoricals: bool = True) -> DataFrame:
        """Combine DataFrames where dataformat is 'Maximums' or 'ccA'.
        Drop the 'Time' column.
        Group data based on 'internalName' and 'Chan ID'.

        Args:
            reset_categoricals: Whether to normalize categorical ordering before grouping.

        Returns:
            DataFrame: Combined and grouped DataFrame.
        """
        logger.debug("Combining 1D Maximums/ccA data.")
        self.align_eof_channel_ids()

        # Filter processors with dataformat 'Maximums' or 'ccA'
        maximums_processors: list[BaseProcessor] = [
            p for p in self.processors if p.dataformat.lower() in ["maximums", "cca"]
        ]

        if not maximums_processors:
            logger.warning("No processors with dataformat 'Maximums' or 'ccA' found.")
            return DataFrame()

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
            logger.debug("Dropped columns {} from DataFrame.", existing_columns_to_drop)

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
            return DataFrame()

        # Concatenate DataFrames (with EOF geometry merged in above when available)
        # TODO - there is no geometry? there should only be data columns from the attributes
        combined_df: DataFrame = self._concat_in_batches(frames=dfs_to_concat)
        logger.debug("Combined Maximums/ccA DataFrame with {} rows.", len(combined_df))

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        if reset_categoricals:
            combined_df = reset_categorical_ordering(combined_df)

        # Group by 'internalName' and 'Chan ID'
        group_keys: list[str] = ["internalName", "Chan ID"]
        missing_keys: list[str] = [key for key in group_keys if key not in combined_df.columns]
        if missing_keys:
            logger.error(f"Missing group keys {missing_keys} in Maximums/ccA data.")
            return DataFrame()

        grouped_df: DataFrame = combined_df.groupby(by=group_keys, observed=True).agg(func="max").reset_index()
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
        logger.debug("Grouped {} Maximums/ccA DataFrame with {} rows.", len(maximums_processors), len(grouped_df))
        return grouped_df

    @staticmethod
    def _channel_match_key(value: object) -> str | None:
        """Return the comparable printed-ID key for a channel label.

        This is deliberately a truncation key, not a rounding key. We do not
        assume TUFLOW numerically evaluates the channel ID string; we mirror the
        observed text report behaviour where only the leading two decimal places
        of the numeric suffix are printed. The prefix is retained so east/west or
        otherwise named roads with the same numeric suffix do not collide.
        """
        if value is None or value is NA:
            return None

        text_value: str = str(value).strip()
        if not text_value or text_value.lower() in {"nan", "nat", "<na>"}:
            return None
        match: re.Match[str] | None = re.match(
            pattern=r"^(?P<prefix>.*?)(?P<number>-?\d+(?:\.\d+)?)$",
            string=text_value,
        )
        if match is None:
            return None

        prefix: str = re.sub(pattern=r"\s+", repl=" ", string=match.group("prefix").strip()).upper()
        number: str = ProcessorCollection._truncate_decimal_text(value=match.group("number"), decimal_places=2)
        return f"{prefix}|{number}"

    @staticmethod
    def _truncate_decimal_text(value: str, decimal_places: int) -> str:
        """Return ``value`` with its decimal text truncated, not rounded.

        Channel IDs are labels, not measured values, so this intentionally works
        on the matched text rather than converting through ``float`` or
        ``Decimal``. Examples from the EOF output are mirrored directly:
        ``159.975`` becomes ``159.97`` and ``202.231`` becomes ``202.23``.
        """
        integer_part, separator, fractional_part = value.partition(".")
        if not separator:
            return f"{integer_part}.{'0' * decimal_places}"
        truncated_fraction: str = fractional_part[:decimal_places].ljust(decimal_places, "0")
        return f"{integer_part}.{truncated_fraction}"

    def _calculate_hw_d_ratio(self, df: DataFrame) -> DataFrame:
        """Calculate the HW_D ratio = (US_h - US Invert) / Height."""
        # Determine which column to use for Headwater Level
        us_h_col: str = "US_h" if "US_h" in df.columns else "US_H"

        required_columns: set[str] = {us_h_col, "US Invert", "Height"}
        missing_columns: set[str] = required_columns - set(df.columns)
        if missing_columns:
            logger.debug("Skipping HW_D calculation; missing columns: {}", sorted(missing_columns))
            return df

        if df.empty:
            df["HW_D"] = Series(dtype="Float64")
            return df

        us_h_series: Series[float] = to_numeric(arg=df[us_h_col], errors="coerce")
        us_invert_series: Series = to_numeric(arg=df["US Invert"], errors="coerce")
        height_series: Series = to_numeric(arg=df["Height"], errors="coerce")

        valid_mask: Series[bool] = (
            us_h_series.notna() & us_invert_series.notna() & height_series.notna() & (height_series != 0)
        )

        hw_d_series: Series = Series(data=NA, index=df.index, dtype="Float64")
        valid_count: int = int(valid_mask.sum())

        if not valid_mask.any():
            df["HW_D"] = hw_d_series
            logger.debug("HW_D calculation skipped; insufficient valid data.")
            return df

        hw_d_series.loc[valid_mask] = (
            us_h_series.loc[valid_mask] - us_invert_series.loc[valid_mask]
        ) / height_series.loc[valid_mask]

        df["HW_D"] = hw_d_series
        logger.debug("Calculated HW_D ratio for {} of {} rows.", valid_count, df["Chan ID"].count())
        return df

    def combine_raw(self, reset_categoricals: bool = True) -> DataFrame:
        """Concatenate all DataFrames together without any grouping.

        Args:
            reset_categoricals: Whether to normalize categorical ordering after concatenation.

        Returns:
            DataFrame: Concatenated DataFrame.
        """
        logger.debug("Combining raw data without grouping.")

        # Concatenate all DataFrames
        combined_df: DataFrame = self._concat_in_batches(frames=[p.df for p in self.processors if not p.df.empty])
        logger.debug("Combined Raw DataFrame with {} rows.", len(combined_df))

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        if reset_categoricals:
            combined_df = reset_categorical_ordering(combined_df)

        return combined_df

    def pomm_combine(self, reset_categoricals: bool = True) -> DataFrame:
        """Combine DataFrames where dataformat is 'POMM'.
        No grouping required as DataFrames are already in the correct format.

        Args:
            reset_categoricals: Whether to normalize categorical ordering after concatenation.

        Returns:
            DataFrame: Combined DataFrame.
        """
        logger.debug("Combining POMM data.")

        # Filter processors with dataformat 'POMM'
        pomm_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "pomm"]

        if not pomm_processors:
            logger.warning("No processors with dataformat 'POMM' found.")
            return DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = self._concat_in_batches(frames=[p.df for p in pomm_processors if not p.df.empty])
        logger.debug("Combined {}  POMM DataFrame with {} rows.", len(pomm_processors), len(combined_df))

        combined_df = reorder_long_columns(df=combined_df)

        # Reset categorical ordering
        if reset_categoricals:
            combined_df = reset_categorical_ordering(combined_df)

        return combined_df

    def po_combine(self, reset_categoricals: bool = True) -> DataFrame:
        """Combine processed PO timeseries files into a single tidy DataFrame.

        Args:
            reset_categoricals: Whether to normalize categorical ordering after concatenation.
        """
        logger.debug("Combining PO timeseries data.")

        # Filter processors with dataformat 'PO'
        po_processors: list[BaseProcessor] = [p for p in self.processors if p.dataformat.lower() == "po"]

        if not po_processors:
            logger.warning("No PO processors available for combination.")
            return DataFrame()

        # Concatenate DataFrames
        combined_df: DataFrame = self._concat_in_batches(frames=[p.df for p in po_processors if not p.df.empty])
        logger.debug("Combined {} PO DataFrame with {} rows.", len(po_processors), len(combined_df))

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
            combined_df = combined_df.sort_values(by=sort_columns)
            combined_df = combined_df.reset_index(drop=True)

        return combined_df

    def get_processors_by_data_type(self, data_types: list[str] | str) -> ProcessorCollection:
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
                if processor.selected_unusable and processor.input_health == "INVALID":
                    filtered_collection.processors.append(processor)
                else:
                    filtered_collection.add_processor(processor)

        logger.debug(
            "Filtered ProcessorCollection created with {} processors matching data types {}.",
            len(filtered_collection.processors),
            data_types,
        )
        return filtered_collection

    @staticmethod
    def _hdf_registry_digest() -> str:
        """Fingerprint the processor schema used to interpret cached frames."""
        registry = Path(__file__).parents[2] / "classes" / "tuflow_results_validation_and_datatypes.json"
        return hashlib.sha256(registry.read_bytes()).hexdigest()

    @staticmethod
    def _hdf_source_signature(path: Path) -> dict[str, Any]:
        """Record lightweight source identity; this is not a content hash."""
        absolute = path.absolute()
        stat = absolute.stat()
        if not absolute.is_file():
            msg = f"Processor source is not a file: {absolute}"
            raise ValueError(msg)
        signature: dict[str, Any] = {
            "path": str(absolute),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
        # A GeoPackage's latest committed results may still be in its WAL.
        if absolute.suffix.lower() == ".gpkg":
            wal = absolute.with_name(absolute.name + "-wal")
            if wal.exists():
                wal_stat = wal.stat()
                signature["wal"] = {"size": wal_stat.st_size, "mtime_ns": wal_stat.st_mtime_ns}
            else:
                signature["wal"] = None
        return signature

    @staticmethod
    def _hdf_frame_bytes(frame: DataFrame) -> bytes:
        """Encode a pandas frame and its extension dtypes using Arrow IPC, not pickle."""
        import numpy as np

        pa: Any = importlib.import_module("pyarrow")

        # POMM's promoted header can leave the column Index name as np.int64.
        # Arrow's pandas metadata is JSON and rejects NumPy scalars in axis names.
        # Normalise only the copied axis metadata; do not alter source frames.
        portable = frame.copy(deep=False)
        if isinstance(portable.columns, MultiIndex):
            names: list[Hashable | None] = [
                cast("Hashable | None", name.item()) if isinstance(name, np.generic) else name
                for name in portable.columns.names
            ]
            portable.columns = portable.columns.set_names(names)
        else:
            name = portable.columns.name
            if isinstance(name, np.generic):
                portable.columns = portable.columns.rename(cast("Hashable | None", name.item()))

        # Arrow encodes object-typed pd.NA as a null and normally restores
        # it as None, which changes the processed DataFrame's value contract.
        # Preserve the exact pandas missing sentinel alongside Arrow metadata.
        na_positions: dict[str, list[int]] = {}
        for col_idx in range(frame.shape[1]):
            if frame.dtypes.iloc[col_idx] == "object":
                positions = [row_idx for row_idx, value in enumerate(frame.iloc[:, col_idx]) if value is NA]
                if positions:
                    na_positions[str(col_idx)] = positions

        sink = pa.BufferOutputStream()
        table = pa.Table.from_pandas(portable, preserve_index=True)
        if na_positions:
            schema_metadata: dict[bytes, bytes] = dict(table.schema.metadata or {})
            schema_metadata[b"ryan_tools:pd_na_positions"] = json.dumps(na_positions, sort_keys=True).encode("utf-8")
            table = table.replace_schema_metadata(schema_metadata)
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        return bytes(sink.getvalue().to_pybytes())

    @staticmethod
    def _hdf_frame_from_bytes(payload: bytes) -> DataFrame:
        """Restore pandas/index metadata, including categories and nullable types."""
        pa: Any = importlib.import_module("pyarrow")

        table = pa.ipc.open_file(pa.BufferReader(payload)).read_all()
        frame: Any = table.to_pandas()
        if not isinstance(frame, DataFrame):
            msg = "Cached Arrow payload did not contain a DataFrame"
            raise TypeError(msg)

        schema_metadata = cast("dict[bytes, bytes] | None", table.schema.metadata)
        extra = schema_metadata.get(b"ryan_tools:pd_na_positions") if schema_metadata is not None else None
        if extra is not None:
            raw_positions: Any = json.loads(extra.decode("utf-8"))
            if not isinstance(raw_positions, dict):
                msg = "Cached Arrow missing-value metadata is malformed"
                raise ValueError(msg)
            positions_by_column = cast("dict[str, Any]", raw_positions)
            for column, raw_column_positions in positions_by_column.items():
                if not isinstance(raw_column_positions, list):
                    msg = "Cached Arrow missing-value metadata is malformed"
                    raise ValueError(msg)
                positions = cast("list[Any]", raw_column_positions)
                col_idx = int(column)
                if not 0 <= col_idx < frame.shape[1]:
                    msg = "Cached Arrow missing-value column index is invalid"
                    raise ValueError(msg)
                for row_idx in positions:
                    if not isinstance(row_idx, int) or not 0 <= row_idx < len(frame):
                        msg = "Cached Arrow missing-value row index is invalid"
                        raise ValueError(msg)
                    frame.iloc[row_idx, col_idx] = NA
        return frame

    @staticmethod
    def _hdf_put_bytes(store: Any, name: str, payload: bytes, filters: Any) -> None:
        """Write one named binary payload as a compressed HDF5 byte array."""
        import numpy as np

        tables: Any = importlib.import_module("tables")

        data = np.frombuffer(payload, dtype=np.uint8)
        node = store.create_carray(store.root, name, atom=tables.UInt8Atom(), shape=(len(data),), filters=filters)
        node[:] = data

    @staticmethod
    def _hdf_get_bytes(store: Any, name: str) -> bytes:
        """Reject a missing cache member rather than returning partial results."""
        tables: Any = importlib.import_module("tables")

        try:
            node = store.get_node(f"/{name}")
        except tables.NoSuchNodeError as exc:
            msg = f"HDF5 cache missing required member {name!r}"
            raise ValueError(msg) from exc
        return bytes(node.read())

    @staticmethod
    def _hdf_require_storage() -> None:
        """Report the optional PyTables dependency before mutating any cache paths."""
        try:
            importlib.import_module("pyarrow")
            importlib.import_module("tables")
        except ImportError as exc:
            msg = "HDF5 processor caches require pyarrow and optional PyTables: pip install 'ryan-tools[cache]'"
            raise RuntimeError(msg) from exc

    def to_hdf(self, file_path: Path | str, *, overwrite: bool = False) -> None:
        """Stage and atomically publish a versioned Arrow-in-HDF5 processor snapshot.

        The default refuses an existing cache. Set overwrite=True to deliberately
        replace it atomically. Source file metadata is captured for later freshness
        checks; the processed rows themselves are a snapshot, not live result data.
        """
        self._hdf_require_storage()
        tables: Any = importlib.import_module("tables")

        destination = Path(file_path).absolute()
        if destination.is_symlink():
            msg = f"Refusing to publish through an HDF5 cache symlink: {destination}"
            raise FileExistsError(msg)
        if destination.exists() and not overwrite:
            msg = f"HDF5 cache already exists (set overwrite=True to replace): {destination}"
            raise FileExistsError(msg)
        if destination.exists() and not destination.is_file():
            msg = f"HDF5 cache destination is not a regular file: {destination}"
            raise ValueError(msg)

        for processor in self.processors:
            if (
                destination.exists()
                and processor.resolved_file_path.exists()
                and destination.samefile(processor.resolved_file_path)
            ):
                msg = f"HDF5 cache destination aliases a processor source: {destination}"
                raise ValueError(msg)

        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".part", dir=destination.parent)
        os.close(fd)

        try:
            manifest: dict[str, Any] = {
                "cache_version": 1,
                "frame_codec": "arrow-ipc-file",
                "registry_sha256": self._hdf_registry_digest(),
                "created_at_utc": datetime.now(UTC).isoformat(),
                "processors": [],
                "has_basic_info_lookup": self.basic_info_lookup is not None,
            }
            filters = tables.Filters(complevel=9, complib="blosc:zstd")
            manifest_payload: bytes = b""
            with tables.open_file(temporary, mode="w") as store:
                for idx, processor in enumerate(self.processors):
                    if not processor.processed or processor.df.empty:
                        msg = f"Only non-empty successfully processed members may be cached: {processor.file_path}"
                        raise ValueError(msg)

                    name = f"proc_{idx:06d}"
                    payload = self._hdf_frame_bytes(processor.df)
                    self._hdf_put_bytes(store, name, payload, filters)
                    manifest["processors"].append(
                        {
                            "hdf_key": name,
                            "sha256": hashlib.sha256(payload).hexdigest(),
                            "file_path": str(processor.resolved_file_path),
                            "source": self._hdf_source_signature(processor.resolved_file_path),
                            "class_name": processor.__class__.__name__,
                            "class_module": processor.__class__.__module__,
                            "processor_module": processor.processor_module,
                            "dataformat": processor.dataformat,
                            "data_type": processor.data_type,
                            "raw_run_code": processor.name_parser.raw_run_code,
                            "include_path_columns": processor.include_path_columns,
                            "input_health": processor.input_health,
                            "rejected_samples": processor.rejected_samples,
                            "rejection_reasons": dict(processor.rejection_reasons),
                            "entity_filter": sorted(processor.entity_filter) if processor.entity_filter else [],
                            "applied_location_filter": (
                                sorted(processor.applied_location_filter) if processor.applied_location_filter else []
                            ),
                            "applied_entity_filter": (
                                sorted(processor.applied_entity_filter) if processor.applied_entity_filter else []
                            ),
                        }
                    )

                if self.basic_info_lookup is not None:
                    lookup_payload = self._hdf_frame_bytes(self.basic_info_lookup)
                    self._hdf_put_bytes(store, "basic_info_lookup", lookup_payload, filters)
                    manifest["basic_info_sha256"] = hashlib.sha256(lookup_payload).hexdigest()

                manifest_payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")
                self._hdf_put_bytes(store, "manifest", manifest_payload, filters)

            # Re-open the complete staging file before replacing any prior cache.
            with tables.open_file(temporary, mode="r") as store:
                if self._hdf_get_bytes(store, "manifest") != manifest_payload:
                    msg = "HDF5 cache staging verification failed"
                    raise ValueError(msg)

            if overwrite:
                Path(temporary).replace(destination)
            elif os.name == "nt":
                # On Windows rename is an atomic no-clobber operation.
                Path(temporary).rename(destination)
            else:
                # On POSIX rename replaces its target; hard-link is atomic no-clobber.
                os.link(temporary, destination)
                Path(temporary).unlink()
        finally:
            Path(temporary).unlink(missing_ok=True)

        logger.info("Saved {} processor snapshots to {}", len(self.processors), destination)

    @staticmethod
    def from_hdf(
        file_path: Path | str,
        locations: Collection[str] | None = None,
        *,
        verify_sources: bool = True,
    ) -> ProcessorCollection:
        """Restore the complete cache or fail, checking source freshness by default.

        verify_sources=False explicitly treats the file as a historical snapshot
        even if the original sources have moved or changed.
        """
        ProcessorCollection._hdf_require_storage()
        tables: Any = importlib.import_module("tables")

        path = Path(file_path)
        if not path.is_file():
            msg = f"HDF5 cache file not found: {path}"
            raise FileNotFoundError(msg)

        collection = ProcessorCollection()
        with tables.open_file(str(path), mode="r") as store:
            try:
                raw_manifest: Any = json.loads(ProcessorCollection._hdf_get_bytes(store, "manifest").decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                msg = f"HDF5 cache manifest is corrupt: {path}"
                raise ValueError(msg) from exc

            if not isinstance(raw_manifest, dict):
                msg = f"Unsupported HDF5 processor cache version: {path}"
                raise ValueError(msg)
            manifest = cast("dict[str, Any]", raw_manifest)
            if manifest.get("cache_version") != 1:
                msg = f"Unsupported HDF5 processor cache version: {path}"
                raise ValueError(msg)
            if manifest.get("frame_codec") != "arrow-ipc-file":
                msg = f"Unsupported HDF5 processor cache frame codec: {path}"
                raise ValueError(msg)
            if manifest.get("registry_sha256") != ProcessorCollection._hdf_registry_digest():
                msg = f"HDF5 cache processor registry changed since snapshot: {path}"
                raise ValueError(msg)
            raw_entries: Any = manifest.get("processors")
            if not isinstance(raw_entries, list):
                msg = f"HDF5 cache processor inventory is malformed: {path}"
                raise ValueError(msg)

            entries = cast("list[Any]", raw_entries)
            for idx, raw_entry in enumerate(entries):
                if not isinstance(raw_entry, dict):
                    msg = f"HDF5 cache member inventory is malformed at index {idx}: {path}"
                    raise ValueError(msg)
                entry = cast("dict[str, Any]", raw_entry)
                if entry.get("hdf_key") != f"proc_{idx:06d}":
                    msg = f"HDF5 cache member inventory is malformed at index {idx}: {path}"
                    raise ValueError(msg)
                if verify_sources:
                    recorded = entry.get("source")
                    recorded_dict = cast("dict[str, Any]", recorded) if isinstance(recorded, dict) else None
                    source_path: Any = recorded_dict.get("path") if recorded_dict is not None else None
                    if not isinstance(source_path, str):
                        msg = f"HDF5 cache member lacks a valid source identity: {entry.get('file_path')}"
                        raise ValueError(msg)
                    try:
                        current_source = ProcessorCollection._hdf_source_signature(Path(source_path))
                    except OSError as exc:
                        msg = f"HDF5 cache source is unavailable: {source_path}"
                        raise ValueError(msg) from exc
                    if recorded != current_source:
                        msg = f"HDF5 cache source has changed since snapshot: {source_path}"
                        raise ValueError(msg)

                key = entry["hdf_key"]
                payload = ProcessorCollection._hdf_get_bytes(store, key)
                if hashlib.sha256(payload).hexdigest() != entry.get("sha256"):
                    msg = f"HDF5 cache member checksum mismatch: {key}"
                    raise ValueError(msg)
                frame = ProcessorCollection._hdf_frame_from_bytes(payload)
                if frame.empty:
                    msg = f"HDF5 cache member has no processed rows: {key}"
                    raise ValueError(msg)

                processor_class = BaseProcessor.get_processor_class(
                    class_name=entry["class_name"],
                    processor_module=entry.get("processor_module"),
                    dataformat=entry.get("dataformat"),
                )
                if processor_class.__module__ != entry.get("class_module"):
                    msg = f"HDF5 cache processor class has changed for {entry['class_name']}"
                    raise ValueError(msg)
                processor = processor_class(
                    file_path=Path(entry["file_path"]),
                    entity_filter=entry.get("entity_filter"),
                    include_path_columns=entry.get("include_path_columns", True),
                )
                if (
                    processor.data_type != entry.get("data_type")
                    or processor.dataformat != entry.get("dataformat")
                    or processor.name_parser.raw_run_code != entry.get("raw_run_code")
                ):
                    msg = f"HDF5 processor configuration mismatches cached source {entry['file_path']}"
                    raise ValueError(msg)

                # Newer snapshots include explicit sample-health evidence; absent
                # keys in historical version-1 caches retain the legacy OK default.
                health: Any = entry.get("input_health", "OK")
                rejection_count: Any = entry.get("rejected_samples", 0)
                raw_rejections: Any = entry.get("rejection_reasons", {})
                valid_health = isinstance(health, str) and health in {
                    "OK",
                    "PARTIAL",
                    "INVALID",
                    "IO_FAILURE",
                    "INCOMPLETE",
                }
                valid_count = type(rejection_count) is int and rejection_count >= 0
                valid_reasons = isinstance(raw_rejections, dict) and all(
                    isinstance(reason_name, str) and type(count) is int and count >= 0
                    for reason_name, count in cast("dict[object, object]", raw_rejections).items()
                )
                if not valid_health or not valid_count or not valid_reasons:
                    msg = f"HDF5 cache member has invalid sample-health metadata: {key}"
                    raise ValueError(msg)
                reasons = cast("dict[str, int]", raw_rejections)
                if sum(reasons.values()) != rejection_count:
                    msg = f"HDF5 cache member has inconsistent sample rejection counts: {key}"
                    raise ValueError(msg)

                processor.df = frame
                processor.input_health = cast("str", health)
                processor.rejected_samples = cast("int", rejection_count)
                processor.rejection_reasons = reasons.copy()
                processor.applied_location_filter = frozenset(entry.get("applied_location_filter", []))
                processor.applied_entity_filter = frozenset(entry.get("applied_entity_filter", []))
                processor.processed = True
                collection.add_processor(processor)

            if manifest.get("has_basic_info_lookup"):
                payload = ProcessorCollection._hdf_get_bytes(store, "basic_info_lookup")
                if hashlib.sha256(payload).hexdigest() != manifest.get("basic_info_sha256"):
                    msg = "HDF5 cache basic-info lookup checksum mismatch"
                    raise ValueError(msg)
                collection.basic_info_lookup = ProcessorCollection._hdf_frame_from_bytes(payload)
            else:
                try:
                    store.get_node("/basic_info_lookup")
                except tables.NoSuchNodeError:
                    pass
                else:
                    msg = "HDF5 cache has an unrecorded basic-info lookup"
                    raise ValueError(msg)

        if locations:
            collection.filter_locations(locations)

        logger.info("Restored {} processors from HDF5 snapshot {}", len(collection.processors), path)
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
        #     # maybe raise, or filter them out, or alert the user
        """
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
        self, source_df: DataFrame, eof_df: DataFrame, *, source_label: str, run_code: str
    ) -> DataFrame:
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

        merged_df: DataFrame = self._merge_chan_and_eof(chan_df=source_df, eof_df=eof_df)
        logger.debug(
            "Merged EOF data into {} dataset for run code {}; row count now {}.", source_label, run_code, len(merged_df)
        )
        return merged_df

    @staticmethod
    def _merge_chan_and_eof(chan_df: DataFrame, eof_df: DataFrame) -> DataFrame:
        """Merge a maximum dataset with EOF data keyed by ``Chan ID`` (EOF wins).

        Args:
            chan_df (DataFrame): DataFrame from a maximums-style processor.
            eof_df (DataFrame): DataFrame from EofProcessor.

        Returns:
            DataFrame: Merged DataFrame.
        """
        if chan_df.empty:
            return eof_df
        if eof_df.empty:
            return chan_df

        if "Chan ID" not in chan_df.columns or "Chan ID" not in eof_df.columns:
            logger.warning("Chan ID missing in one of the dataframes, cannot merge.")
            return chan_df

        # Check for specific Chan ID values in chan_df that are missing in eof_df
        missing_in_eof = set(chan_df["Chan ID"]) - set(eof_df["Chan ID"])
        if missing_in_eof:
            logger.warning(
                f"Warning: {len(missing_in_eof)} nodes in timeseries/maximums were not found in the EOF geometry table."
            )

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
