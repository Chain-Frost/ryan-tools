# ryan_library/processors/tuflow/timeseries_processor.py

__lazy_modules__ = ["numpy", "pandas"]

from abc import abstractmethod
from collections.abc import Collection
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from ...functions.tuflow.sample_validity import clean_hydraulic_samples, has_consistent_csv_widths
from .base_processor import (
    BaseProcessor,
    DataValidationError,
    ProcessorError,
    ProcessorStatus,
)
from .timeseries_helpers import reshape_h_timeseries


class TimeSeriesProcessor(BaseProcessor):
    """Base class for processors that operate on TUFLOW timeseries outputs.

    The class centralises the shared read → clean → reshape pipeline required by
    every timeseries dataset before delegating to
    :meth:`process_timeseries_raw_dataframe` for format-specific transforms.
    """

    # Canonical channels named by the original supported CSV headers, including
    # entities whose numeric samples disappear during normalisation.
    source_channels: frozenset[str] = frozenset()
    # Channels with usable values before any entity or collection selection.
    usable_source_channels: frozenset[str] = frozenset()

    @staticmethod
    def _source_channel_ids(columns: pd.Index, data_type: str) -> frozenset[str]:
        """Return source channels before missing-data removal or H pivoting."""
        channels: set[str] = set()
        for raw_name in columns:
            label = str(raw_name).strip()
            if not label or label == "Time":
                continue
            if data_type == "H":
                if not label.endswith((".1", ".2")):
                    continue
                label = label[:-2].strip()
            if label:
                channels.add(label)
        return frozenset(channels)

    def _clean_h_source_samples(self, frame: pd.DataFrame) -> None:
        """Count missing H observations before pivoting removes empty channels."""
        for raw_column in frame.columns:
            column = str(raw_column)
            if not column.endswith((".1", ".2")):
                continue
            measurement = "US_H" if column.endswith(".1") else "DS_H"
            cleaned, rejected = clean_hydraulic_samples(frame[column], measurement=measurement)
            frame[column] = cleaned
            self.record_rejections(rejected)

    def filter_locations(self, locations: Collection[str] | None) -> frozenset[str]:
        """Classify source-confirmed, all-rejected channels as invalid, not absent."""
        selected = super().filter_locations(locations)
        if not selected:
            return selected
        if self.df.empty:
            self.applied_location_filter = selected
            if selected & (self.source_channels - self.usable_source_channels):
                self.selected_unusable = True
                self.filtered_empty = False
                self.input_health = "INVALID"
            else:
                self.selected_unusable = False
                self.filtered_empty = True
        else:
            self.selected_unusable = False
        return selected

    def _mark_absent_source_selection(self) -> bool:
        """Classify genuinely absent IDs after otherwise readable data ran out."""
        requested = self.entity_filter
        if not requested or not self.source_channels or not requested.isdisjoint(self.source_channels):
            return False
        if not has_consistent_csv_widths(self.file_path):
            logger.warning("{}: CSV record widths are inconsistent; absent selection is not a SKIP.", self.file_name)
            return False
        self.applied_entity_filter = requested
        self.filtered_empty = True
        logger.info("{}: No requested timeseries entities were present in source headers.", self.file_name)
        return True

    def _finish_failed_step(self, *, data_type: str, stage: str, status: ProcessorStatus) -> None:
        """Keep absent selection distinct from failed source processing."""
        no_usable_rows = self._empty_after_reshape if stage == "read" else status is ProcessorStatus.EMPTY_DATAFRAME
        if no_usable_rows and self._mark_absent_source_selection():
            self.df = pd.DataFrame()
            return
        if stage == "read":
            logger.error(f"Processing aborted for file: {self.log_path} while reading and reshaping {data_type} data.")
        else:
            logger.error(f"Processing aborted for file: {self.log_path} during {data_type} post-processing step.")
        self.input_health = "INVALID"
        self.df = pd.DataFrame()

    def process(self) -> None:
        """Execute the standard timeseries processing pipeline."""
        return self._process_timeseries_pipeline(data_type=self.data_type)

    def _process_timeseries_pipeline(self, data_type: str) -> None:
        """Run the shared processing pipeline for a timeseries dataset."""
        self.reset_processing_state()
        self.source_channels = frozenset()
        self.usable_source_channels = frozenset()
        self._empty_after_reshape = False
        # H pivoting drops entirely blank source channels, so retain header evidence.
        self._selected_h_source_present = False
        logger.info(f"Starting processing of {data_type} file: {self.log_path}")

        try:
            status: ProcessorStatus = self.read_and_process_timeseries_csv(data_type=data_type)
            if status is not ProcessorStatus.SUCCESS:
                self._finish_failed_step(data_type=data_type, stage="read", status=status)
                return

            # Retain entity existence before subclass normalisation drops invalid
            # numeric samples. An existing all-invalid selection is FAIL, not SKIP.
            selected_before_cleaning = self._selected_h_source_present or bool(
                self.entity_filter and self.source_channels & self.entity_filter
            )
            if self.entity_filter:
                id_column = next((name for name in ("Chan ID", "Location") if name in self.df.columns), None)
                if id_column is not None:
                    identifiers = self.df[id_column].astype("string").str.strip()
                    selected_before_cleaning = selected_before_cleaning or bool(
                        identifiers.isin(self.entity_filter).any()
                    )

            status = self.process_timeseries_raw_dataframe()
            if status is not ProcessorStatus.SUCCESS:
                self._finish_failed_step(data_type=data_type, stage="normalise", status=status)
                return

            # Record actual usable channels before a user selection discards
            # valid rows. Header existence alone cannot prove invalid input
            # when a later filter switches to a previously excluded channel.
            id_column = next((name for name in ("Chan ID", "Location") if name in self.df.columns), None)
            if id_column is not None:
                usable_labels: list[object] = self.df[id_column].tolist()
                self.usable_source_channels = frozenset(
                    str(label).strip() for label in usable_labels if str(label).strip()
                )

            self.apply_entity_filter()
            if self.applied_entity_filter and self.df.empty:
                if selected_before_cleaning:
                    self.input_health = "INVALID"
                    logger.warning("{}: Selected entities contain no usable timeseries samples.", self.file_name)
                else:
                    self.filtered_empty = True
                    logger.info("{}: No requested timeseries entities found; skipping file.", self.file_name)
                return
            self.add_common_columns()
            self.apply_output_transformations()

            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed for {data_type} data.")
                self.input_health = "INVALID"
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of {data_type} file: {self.log_path}")
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(f"Failed to process {data_type} file {self.log_path}: {exc}")
            self.input_health = "INVALID"
            self.df = pd.DataFrame()
            return

    @abstractmethod
    def process_timeseries_raw_dataframe(self) -> ProcessorStatus:
        """Transform :attr:`self.df` after the common pipeline finishes.

        Implementations should update :attr:`self.df` in place and return a
        :class:`~ryan_library.processors.tuflow.base_processor.ProcessorStatus`
        describing the outcome of the subclass-specific processing.

        Returns:
            ProcessorStatus: Status flag indicating success or the failure reason.
        """
        raise NotImplementedError

    def read_and_process_timeseries_csv(self, data_type: str) -> ProcessorStatus:
        """Read, clean and reshape a timeseries CSV into :attr:`self.df`.

        Args:
            data_type: Abbreviation describing the numeric values contained in the
                file (for example ``"H"`` or ``"Q"``). The identifier drives how
                the DataFrame is reshaped and which value columns are created.

        Returns:
            ProcessorStatus: Status code following the same convention as
            :meth:`process_timeseries_raw_dataframe`.
        """
        try:
            df_full: pd.DataFrame = self._read_csv(file_path=self.file_path)
            if df_full.empty:
                logger.error(f"{self.file_name}: No data found in file: {self.log_path}")
                return ProcessorStatus.EMPTY_DATAFRAME

            df_clean: pd.DataFrame = self._clean_headers(df=df_full, data_type=data_type)
            if df_clean.empty:
                logger.error(f"{self.file_name}: DataFrame is empty after cleaning headers.")
                return ProcessorStatus.EMPTY_DATAFRAME

            self.source_channels = self._source_channel_ids(df_clean.columns, data_type)
            selected_filter = self.entity_filter
            if data_type == "H" and selected_filter:
                # Keep canonical .1/.2 channel presence before pivot_table
                # removes rows whose upstream/downstream samples are all blank.
                self._selected_h_source_present = any(
                    label.endswith((".1", ".2")) and label[:-2].strip() in selected_filter for label in df_clean.columns
                )

            if data_type == "H":
                self._clean_h_source_samples(df_clean)

            df_melted: pd.DataFrame = self._reshape_timeseries_df(df=df_clean, data_type=data_type)
            if df_melted.empty:
                logger.error(f"{self.file_name}: No data found after reshaping.")
                return ProcessorStatus.EMPTY_DATAFRAME

            self.df = df_melted
            if data_type in {"Q", "V"}:
                numeric, rejected = clean_hydraulic_samples(self.df[data_type], measurement=data_type)
                self.df[data_type] = numeric
                self.record_rejections(rejected)
            self._apply_final_transformations(data_type=data_type)
            logger.info(f"{self.file_name}: Timeseries CSV processed successfully.")
            return ProcessorStatus.SUCCESS
        except (ProcessorError, DataValidationError) as exc:
            logger.error(f"{self.file_name}: Processing error: {exc}")
            return ProcessorStatus.FAILURE
        except Exception as exc:
            logger.exception(f"{self.file_name}: Unexpected error: {exc}")
            return ProcessorStatus.FAILURE

    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        """Return the raw timeseries CSV as a DataFrame using shared options.

        Args:
            file_path: Location of the CSV produced by TUFLOW.

        Returns:
            pandas.DataFrame: Raw data read from ``file_path``.

        Raises:
            ProcessorError: If :mod:`pandas` fails to load the file.
        """
        try:
            df: pd.DataFrame = pd.read_csv(
                filepath_or_buffer=file_path,
                header=0,
                skipinitialspace=True,
                encoding="utf-8",
            )
            logger.debug("CSV file '{}' read successfully with {} rows.", self.file_name, len(df))
            return df
        except Exception as exc:
            logger.exception(f"{self.file_name}: Failed to read CSV file '{self.log_path}': {exc}")
            msg = f"Failed to read CSV file '{file_path}': {exc}"
            raise ProcessorError(msg) from exc

    def _clean_headers(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Normalise the header row before reshaping timeseries data.

        The exported CSVs include a blank descriptor column and occasionally use
        ``"Time (h)"`` in place of ``"Time"``. This helper removes the redundant
        column, ensures a ``"Time"`` field exists and trims the ``data_type`` prefix
        as well as any unit suffixes.

        Args:
            df: Raw DataFrame returned by :meth:`_read_csv`.
            data_type: Identifier for the value columns within ``df``.

        Returns:
            pandas.DataFrame: DataFrame with consistent headers ready for reshaping.

        Raises:
            ProcessorError: If the DataFrame cannot be cleaned or lacks a ``"Time"``
                column.
        """
        try:
            df = df.drop(labels=df.columns[0], axis=1)
            logger.debug("Dropped the first column from '{}'.", self.log_path)

            time_column_aliases: dict[str, str] = {"Time (h)": "Time", "Time(h)": "Time"}
            rename_columns: dict[str, str] = {
                original: alias for original, alias in time_column_aliases.items() if original in df.columns
            }
            if rename_columns:
                df = df.rename(columns=rename_columns)
                logger.debug("Renamed time columns: {}.", rename_columns)

            if "Time" not in df.columns:
                logger.error(f"{self.file_name}: 'Time' column is missing after cleaning headers.")
                msg = "'Time' column is missing after cleaning headers."
                raise DataValidationError(msg)

            time_values = pd.to_numeric(df["Time"], errors="coerce")
            if not np.isfinite(time_values.to_numpy(dtype="float64", na_value=np.nan)).all():
                message = "Timeseries contains missing or nonfinite timestamps."
                raise DataValidationError(message)
            if (time_values.diff().iloc[1:] <= 0).any():
                message = "Timeseries timestamps must increase strictly without duplicates."
                raise DataValidationError(message)
            df["Time"] = time_values

            cleaned_columns: list[str] = self._clean_column_names(columns=df.columns, data_type=data_type)
            df.columns = cleaned_columns
            logger.debug("Cleaned headers: {}", cleaned_columns)
            return df
        except Exception as exc:
            logger.exception(f"{self.file_name}: Failed to clean headers: {exc}")
            msg = f"Failed to clean headers: {exc}"
            raise ProcessorError(msg) from exc

    def _clean_column_names(self, columns: pd.Index, data_type: str) -> list[str]:
        """Strip prefixes and unit suffixes from a sequence of column names.

        Args:
            columns: Columns to normalise.
            data_type: Identifier prefix included in the exported headers.

        Returns:
            list[str]: Cleaned column names with redundant descriptors removed.
        """
        cleaned_columns: list[str] = []
        for col in columns:
            if col.startswith(f"{data_type} "):
                col_clean: str = col[len(data_type) + 1 :]
            else:
                col_clean = col

            if "[" in col_clean and "]" in col_clean:
                # Example: "Q ds1 [M11_5m_001]" becomes "ds1"
                col_clean = col_clean.split("[")[0].strip()

            cleaned_columns.append(col_clean)
        return cleaned_columns

    def _reshape_timeseries_df(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Reshape the cleaned DataFrame into a tidy, long-form structure.

        Args:
            df: Cleaned DataFrame containing a ``"Time"`` column and value columns.
            data_type: Identifier for the numeric series in ``df``.

        Returns:
            pandas.DataFrame: Melted DataFrame with consistent column ordering.

        Raises:
            ProcessorError: If melting fails for any reason.
            DataValidationError: If the resulting DataFrame is empty or the headers
                do not match the expected structure.
        """
        is_1d: bool = "_1d_" in self.file_name.lower()
        category_type: str = "Chan ID" if is_1d else "Location"
        logger.debug(
            "{}: {} filename detected; using '{}' as category type.",
            self.file_name,
            "1D" if is_1d else "2D",
            category_type,
        )

        try:
            if data_type == "H":
                df_melted: pd.DataFrame = reshape_h_timeseries(
                    df=df, category_type=category_type, file_label=self.file_name
                )
            else:
                df_melted = df.melt(id_vars=["Time"], var_name=category_type, value_name=data_type)
                logger.debug("Reshaped DataFrame to long format with {} rows.", len(df_melted))
        except Exception as exc:
            logger.exception(f"{self.file_name}: Failed to reshape DataFrame: {exc}")
            msg = f"Failed to reshape DataFrame: {exc}"
            raise ProcessorError(msg) from exc

        if df_melted.empty:
            self._empty_after_reshape = True
            logger.error(f"{self.file_name}: No data found after reshaping.")
            msg = "No data found after reshaping."
            raise DataValidationError(msg)

        expected_headers: list[str] = (
            ["Time", category_type, "US_H", "DS_H"] if data_type == "H" else ["Time", category_type, data_type]
        )
        # Build the expected header list dynamically. It always starts with "Time" and the
        # category column, which switches between "Chan ID" for 1D results and "Location" for
        # 2D results. For "H" data types, both "H_US" and "H_DS" are expected; otherwise the
        # single data type column is used. ``check_headers_match`` validates against this list.
        self.expected_in_header = expected_headers
        if not self.check_headers_match(test_headers=df_melted.columns.tolist()):
            logger.error(f"{self.file_name}: Header mismatch after reshaping.")
            msg = "Header mismatch after reshaping."
            raise DataValidationError(msg)

        return df_melted

    def _apply_final_transformations(self, data_type: str) -> None:
        """Apply dtype coercions expected by downstream consumers.

        Args:
            data_type: Identifier of the main numeric value column in ``self.df``.
        """
        col_types: dict[str, str] = {
            "Time": "float64",
        }

        if data_type == "H":
            col_types.update({"US_H": "float64", "DS_H": "float64"})
        else:
            col_types[data_type] = "float64"

        self.apply_dtype_mapping(dtype_mapping=col_types, context="final_transformations")

    def _normalise_value_dataframe(self, value_column: str) -> ProcessorStatus:
        """Normalise a long-form timeseries DataFrame for a single value column.

        Args:
            value_column: Name of the numeric value column (for example ``"Q"`` or
                ``"V"``).

        Returns:
            ProcessorStatus: ``ProcessorStatus.SUCCESS`` when normalisation
            succeeds, otherwise a status code describing the failure.
        """
        try:
            logger.debug("Normalising melted DataFrame for value column '{}'.", value_column)

            required_columns: set[str] = {"Time", value_column}
            missing_columns: set[str] = required_columns - set(self.df.columns)
            if missing_columns:
                logger.error(f"{self.file_name}: Missing required columns after melt: {sorted(missing_columns)}.")
                return ProcessorStatus.FAILURE

            identifier_columns: list[str] = [col for col in self.df.columns if col not in required_columns]
            if not identifier_columns:
                logger.error(f"{self.file_name}: No identifier column found alongside '{value_column}'.")
                return ProcessorStatus.FAILURE
            if len(identifier_columns) > 1:
                identifier_error: str = (
                    f"{self.file_name}: Expected a single identifier column alongside 'Time' and "
                    f"'{value_column}', got {identifier_columns}."
                )
                logger.error(identifier_error)
                return ProcessorStatus.FAILURE

            identifier_column: str = identifier_columns[0]
            logger.debug("Using '{}' as the identifier column for '{}' values.", identifier_column, value_column)

            initial_row_count = len(self.df)
            self.df = self.df.dropna(subset=[value_column])
            dropped_rows = initial_row_count - len(self.df)
            if dropped_rows:
                logger.debug("Dropped {} rows with missing '{}' values.", dropped_rows, value_column)

            if self.df.empty:
                logger.error(
                    f"{self.file_name}: DataFrame is empty after removing rows with missing '{value_column}' values."
                )
                return ProcessorStatus.EMPTY_DATAFRAME

            expected_order: list[str] = ["Time", identifier_column, value_column]
            self.df = self.df[expected_order]

            if not self.check_headers_match(test_headers=self.df.columns.tolist()):
                logger.error(f"{self.file_name}: Header mismatch after normalising '{value_column}' DataFrame.")
                return ProcessorStatus.HEADER_MISMATCH

            logger.info(
                f"{self.file_name}: Successfully normalised '{value_column}' DataFrame for downstream processing."
            )
            return ProcessorStatus.SUCCESS
        except (DataValidationError, ProcessorError) as exc:
            logger.error(f"{self.file_name}: Processor validation error while normalising '{value_column}': {exc}")
            return ProcessorStatus.FAILURE
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(f"{self.file_name}: Unexpected error while normalising '{value_column}': {exc}")
            return ProcessorStatus.FAILURE
