# ryan_library/processors/tuflow/QProcessor.py

import pandas as pd  # type: ignore[import-untyped]
from loguru import logger

from .base_processor import DataValidationError, ProcessorError, ProcessorStatus
from .timeseries_processor import TimeSeriesProcessor


class QProcessor(TimeSeriesProcessor):
    """Processor for ``_Q`` timeseries CSV outputs."""

    def process(self) -> pd.DataFrame:
        """Run the shared pipeline and Q-specific reshape for a ``_Q`` CSV.

        The method orchestrates each step of the processing workflow:

        1. Invoke :meth:`read_and_process_timeseries_csv` to read and tidy the
           raw TUFLOW export.
        2. Normalise the melted discharge values via
           :meth:`process_timeseries_raw_dataframe`.
        3. Append metadata columns, apply configured dtype conversions and
           validate the resulting frame before returning it.

        Returns:
            pandas.DataFrame: Processed Q-series observations.
        """
        logger.info(f"Starting processing of Q file: {self.file_path}")

        try:
            # Step 1: Read and process the timeseries CSV with 'Q' as the data type
            status: ProcessorStatus = self.read_and_process_timeseries_csv(data_type="Q")

            if status is not ProcessorStatus.SUCCESS:
                logger.error(f"Processing aborted for file: {self.file_path} due to previous errors.")
                self.df = pd.DataFrame()
                return self.df

            # Step 2: Further process the reshaped DataFrame if necessary
            status = self.process_timeseries_raw_dataframe()
            if status is not ProcessorStatus.SUCCESS:
                logger.error(f"Processing aborted for file: {self.file_path} during raw dataframe processing.")
                self.df = pd.DataFrame()
                return self.df

            # Step 3: Add common columns (e.g., basic info, run code parts, additional attributes)
            self.add_common_columns()

            # Step 4: Apply output transformations based on configuration
            self.apply_output_transformations()

            # Step 5: Validate the processed data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return self.df

            self.processed = True
            logger.info(f"Completed processing of Q file: {self.file_path}")

            return self.df

        except Exception as e:
            logger.error(f"Failed to process Q file {self.file_path}: {e}")
            self.df = pd.DataFrame()
            return self.df

    def process_timeseries_raw_dataframe(self) -> ProcessorStatus:
        """Normalise the long-form ``Q`` DataFrame produced by the shared pipeline.

        The shared reader trims the leading ``"Q "`` prefix and any bracketed
        unit descriptors from the raw column headers (for example ``"Q ds1
        [M11_5m_001]"`` becomes ``"ds1"``).  This helper keeps whichever
        identifier column was created during the melt (``"Chan ID"`` for 1D
        exports or ``"Location"`` for 2D), removes empty discharge values and
        re-confirms the headers so downstream validation behaves predictably.

        Returns:
            ProcessorStatus: Status flag following the shared convention used by
            :class:`~ryan_library.processors.tuflow.base_processor.BaseProcessor`.
        """
        try:
            logger.debug("Normalising the melted Q DataFrame produced by the shared pipeline.")

            identifier_columns = [col for col in self.df.columns if col not in {"Time", "Q"}]
            if len(identifier_columns) != 1:
                logger.error(
                    f"{self.file_name}: Expected a single identifier column alongside 'Time' and 'Q', got {identifier_columns}."
                )
                return ProcessorStatus.FAILURE

            identifier_column: str = identifier_columns[0]
            logger.debug(f"Using '{identifier_column}' as the identifier column for Q values.")

            # Drop rows with missing Q values to avoid empty observations downstream
            initial_row_count = len(self.df)
            self.df.dropna(subset=["Q"], inplace=True)
            final_row_count = len(self.df)
            dropped_rows = initial_row_count - final_row_count
            if dropped_rows:
                logger.debug(f"Dropped {dropped_rows} rows with missing Q values.")

            if self.df.empty:
                logger.error("DataFrame is empty after removing rows with missing Q values.")
                return ProcessorStatus.EMPTY_DATAFRAME

            # Validate headers after normalisation
            if not self.check_headers_match(test_headers=self.df.columns.tolist()):
                logger.error(f"{self.file_name}: Header mismatch after melting.")
                return ProcessorStatus.HEADER_MISMATCH

            logger.info(f"{self.file_name}: Successfully normalised Q DataFrame for downstream processing.")

            return ProcessorStatus.SUCCESS

        except DataValidationError as dve:
            logger.error(f"{self.file_name}: Data validation error: {dve}")
            return ProcessorStatus.FAILURE
        except ProcessorError as pe:
            logger.error(f"{self.file_name}: Processor error: {pe}")
            return ProcessorStatus.FAILURE
        except Exception as e:
            logger.exception(f"{self.file_name}: Unexpected error during processing: {e}")
            return ProcessorStatus.FAILURE
