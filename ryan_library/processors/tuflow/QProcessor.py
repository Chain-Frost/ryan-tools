# ryan_library/processors/tuflow/QProcessor.py

import pandas as pd
from loguru import logger
from .base_processor import BaseProcessor


class QProcessor(BaseProcessor):
    """
    Processor for '_Q' CSV files.
    """

    def process(self) -> pd.DataFrame:
        """
        Process the '_Q' CSV file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed Q data.
        """
        logger.info(f"Starting processing of Q file: {self.file_path}")

        try:
            # Use the common timeseries channel reader with 'Q' as the data type
            status: int = self.read_and_process_timeseries_csv(data_type="Q")

            if status != 0:
                logger.error(
                    f"Processing aborted for file: {self.file_path} due to previous errors."
                )
                self.raw_df = self.df = pd.DataFrame()

                return self.df

            status = self.process_timeseries_raw_dataframe()
            if status != 0:
                logger.error(
                    f"Processing aborted for file: {self.file_path} due to previous errors."
                )
                self.raw_df = self.df = pd.DataFrame()

                return self.df

            # Proceed with common processing steps from BaseProcessor
            self.add_common_columns()
            self.apply_output_transformations()

            # Validate data
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
