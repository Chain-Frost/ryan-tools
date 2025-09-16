# ryan_library/processors/tuflow/QProcessor.py

import pandas as pd
from loguru import logger

from .timeseries_processor import TimeSeriesProcessor


class QProcessor(TimeSeriesProcessor):
    """Processor for '_Q' Timeseries CSV files."""

    def process(self) -> pd.DataFrame:
        """
        Process the '_Q' timeseries CSV file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed Q data.
        """
        logger.info(f"Starting processing of Q file: {self.file_path}")

        try:
            # Step 1: Read and process the timeseries CSV using the inferred data type
            status: int = self.read_and_process_timeseries_csv()

            if status != 0:
                logger.error(f"Processing aborted for file: {self.file_path} due to previous errors.")
                self.df = pd.DataFrame()
                return self.df

            # Step 2: Further process the reshaped DataFrame if necessary
            status = self.process_timeseries_raw_dataframe()
            if status != 0:
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

    def process_timeseries_raw_dataframe(self, dropna: bool = True) -> int:
        """Validate the long-form Q timeseries DataFrame and optionally drop missing values.

        Args:
            dropna (bool): Drop rows with missing ``Q`` values when True. Defaults to True.

        Returns:
            int: Status code.
                0 - Success
                1 - Empty DataFrame after processing
                2 - Missing required columns
                3 - Processing error"""
        try:
            logger.debug("Validating long-form Q timeseries DataFrame.")

            required_columns = ["Time", "Chan ID", "Q"]
            missing = [col for col in required_columns if col not in self.df.columns]
            if missing:
                logger.error(f"{self.file_name}: Missing required columns: {missing}")
                return 2

            if dropna:
                initial_row_count = len(self.df)
                self.df.dropna(subset=["Q"], inplace=True)
                logger.debug(f"Dropped {initial_row_count - len(self.df)} rows with missing Q values.")

            if self.df.empty:
                logger.error("DataFrame is empty after processing.")
                return 1

            return 0

        except Exception as e:
            logger.exception(f"{self.file_name}: Unexpected error during processing: {e}")
            return 3
