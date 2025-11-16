# ryan_library/processors/tuflow/timeseries_processor.py

from abc import abstractmethod
from pathlib import Path

import pandas as pd
from loguru import logger

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

    def process(self) -> pd.DataFrame:  # type: ignore[override]
        """Execute the standard timeseries processing pipeline."""

        return self._process_timeseries_pipeline(data_type=self.data_type)

    def _process_timeseries_pipeline(self, data_type: str) -> pd.DataFrame:
        """Run the shared processing pipeline for a timeseries dataset."""

        logger.info(f"Starting processing of {data_type} file: {self.file_path}")

        try:
            status: ProcessorStatus = self.read_and_process_timeseries_csv(data_type=data_type)
            if status is not ProcessorStatus.SUCCESS:
                logger.error(
                    f"Processing aborted for file: {self.file_path} while reading and reshaping {data_type} data."
                )
                self.df = pd.DataFrame()
                return self.df

            status = self.process_timeseries_raw_dataframe()
            if status is not ProcessorStatus.SUCCESS:
                logger.error(f"Processing aborted for file: {self.file_path} during {data_type} post-processing step.")
                self.df = pd.DataFrame()
                return self.df

            self.add_common_columns()
            self.apply_output_transformations()

            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed for {data_type} data.")
                self.df = pd.DataFrame()
                return self.df

            self.processed = True
            logger.info(f"Completed processing of {data_type} file: {self.file_path}")
            return self.df
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(f"Failed to process {data_type} file {self.file_path}: {exc}")
            self.df = pd.DataFrame()
            return self.df

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
                logger.error(f"{self.file_name}: No data found in file: {self.file_path}")
                return ProcessorStatus.EMPTY_DATAFRAME

            df_clean: pd.DataFrame = self._clean_headers(df=df_full, data_type=data_type)
            if df_clean.empty:
                logger.error(f"{self.file_name}: DataFrame is empty after cleaning headers.")
                return ProcessorStatus.EMPTY_DATAFRAME

            df_melted: pd.DataFrame = self._reshape_timeseries_df(df=df_clean, data_type=data_type)
            if df_melted.empty:
                logger.error(f"{self.file_name}: No data found after reshaping.")
                return ProcessorStatus.EMPTY_DATAFRAME

            self.df = df_melted
            self._apply_final_transformations(data_type=data_type)
            self.processed = True  # Mark as processed once the shared pipeline completes
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
            df: pd.DataFrame = pd.read_csv(  # type: ignore
                filepath_or_buffer=file_path,
                header=0,
                skipinitialspace=True,
                encoding="utf-8",
            )
            logger.debug(f"CSV file '{self.file_name}' read successfully with {len(df)} rows.")
            return df
        except Exception as exc:
            logger.exception(f"{self.file_name}: Failed to read CSV file '{file_path}': {exc}")
            raise ProcessorError(f"Failed to read CSV file '{file_path}': {exc}") from exc

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
            logger.debug(f"Dropped the first column from '{self.file_path}'.")

            time_column_aliases: dict[str, str] = {"Time (h)": "Time", "Time(h)": "Time"}
            rename_columns: dict[str, str] = {
                original: alias for original, alias in time_column_aliases.items() if original in df.columns
            }
            if rename_columns:
                df.rename(columns=rename_columns, inplace=True)
                logger.debug(f"Renamed time columns: {rename_columns}.")

            if "Time" not in df.columns:
                logger.error(f"{self.file_name}: 'Time' column is missing after cleaning headers.")
                raise DataValidationError("'Time' column is missing after cleaning headers.")

            cleaned_columns: list[str] = self._clean_column_names(columns=df.columns, data_type=data_type)
            df.columns = cleaned_columns
            logger.debug(f"Cleaned headers: {cleaned_columns}")
            return df
        except Exception as exc:
            logger.exception(f"{self.file_name}: Failed to clean headers: {exc}")
            raise ProcessorError(f"Failed to clean headers: {exc}") from exc

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
            f"{self.file_name}: {'1D' if is_1d else '2D'} filename detected; using '{category_type}' as category type."
        )

        try:
            if data_type == "H":
                df_melted: pd.DataFrame = reshape_h_timeseries(
                    df=df, category_type=category_type, file_label=self.file_name
                )
            else:
                df_melted = df.melt(id_vars=["Time"], var_name=category_type, value_name=data_type)  # type: ignore
                logger.debug(f"Reshaped DataFrame to long format with {len(df_melted)} rows.")
        except Exception as exc:
            logger.exception(f"{self.file_name}: Failed to reshape DataFrame: {exc}")
            raise ProcessorError(f"Failed to reshape DataFrame: {exc}") from exc

        if df_melted.empty:
            logger.error(f"{self.file_name}: No data found after reshaping.")
            raise DataValidationError("No data found after reshaping.")

        expected_headers: list[str] = (
            ["Time", category_type, "H_US", "H_DS"] if data_type == "H" else ["Time", category_type, data_type]
        )
        # Build the expected header list dynamically. It always starts with "Time" and the
        # category column, which switches between "Chan ID" for 1D results and "Location" for
        # 2D results. For "H" data types, both "H_US" and "H_DS" are expected; otherwise the
        # single data type column is used. ``check_headers_match`` validates against this list.
        self.expected_in_header = expected_headers
        if not self.check_headers_match(test_headers=df_melted.columns.tolist()):
            logger.error(f"{self.file_name}: Header mismatch after reshaping.")
            raise DataValidationError("Header mismatch after reshaping.")

        return df_melted

    def _apply_final_transformations(self, data_type: str) -> None:
        """Apply dtype coercions expected by downstream consumers.

        Args:
            data_type: Identifier of the main numeric value column in ``self.df``.
        """
        col_types: dict[str, str] = {
            "Time": "float64",
            data_type: "float64",
        }

        if data_type == "H":
            col_types.update({"H_US": "float64", "H_DS": "float64"})
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
            logger.debug(f"Normalising melted DataFrame for value column '{value_column}'.")

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
            logger.debug(f"Using '{identifier_column}' as the identifier column for '{value_column}' values.")

            initial_row_count = len(self.df)
            self.df.dropna(subset=[value_column], inplace=True)  # type: ignore
            dropped_rows = initial_row_count - len(self.df)
            if dropped_rows:
                logger.debug(f"Dropped {dropped_rows} rows with missing '{value_column}' values.")

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
