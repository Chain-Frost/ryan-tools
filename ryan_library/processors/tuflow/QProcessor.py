# ryan_library/processors/tuflow/QProcessor.py

import pandas as pd
from loguru import logger
from .base_processor import BaseProcessor, DataValidationError, ProcessorError


class QProcessor(BaseProcessor):
    """Processor for '_Q' Timeseries CSV files."""

    def process(self) -> pd.DataFrame:
        """
        Process the '_Q' timeseries CSV file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed Q data.
        """
        logger.info(f"Starting processing of Q file: {self.file_path}")

        try:
            # Step 1: Read and process the timeseries CSV with 'Q' as the data type
            status: int = self.read_and_process_timeseries_csv(data_type="Q")

            if status != 0:
                logger.error(
                    f"Processing aborted for file: {self.file_path} due to previous errors."
                )
                self.df = pd.DataFrame()
                return self.df

            # Step 2: Further process the reshaped DataFrame if necessary
            status = self.process_timeseries_raw_dataframe()
            if status != 0:
                logger.error(
                    f"Processing aborted for file: {self.file_path} during raw dataframe processing."
                )
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

    def process_timeseries_raw_dataframe(self) -> int:
        """Process the raw reshaped timeseries DataFrame by melting Q columns into a long format.

        Returns:
            int: Status code.
                0 - Success
                1 - Empty DataFrame after processing
                2 - Header mismatch after processing
                3 - Processing error"""
        try:
            logger.debug("Starting to process the raw timeseries DataFrame for Q data.")

            # Identify Q columns (e.g., "Q ds1", "Q ds2", "Q ds3")
            q_columns = [col for col in self.df.columns if col.startswith("Q ds")]
            logger.debug(f"Identified Q columns: {q_columns}")

            if not q_columns:
                logger.error("No Q columns found in the DataFrame.")
                return 3

            # Melt the Q columns to transform from wide to long format
            df_melted = self.df.melt(
                id_vars=["Time"], value_vars=q_columns, var_name="ds", value_name="Q"
            )
            logger.debug(f"Melted DataFrame shape: {df_melted.shape}")

            # Extract ds identifier (e.g., 'ds1', 'ds2', 'ds3') from the 'ds' column
            df_melted["ds"] = df_melted["ds"].str.extract(r"(ds\d+)")

            # Drop rows with missing Q values
            initial_row_count = len(df_melted)
            df_melted.dropna(subset=["Q"], inplace=True)
            final_row_count = len(df_melted)
            logger.debug(
                f"Dropped {initial_row_count - final_row_count} rows with missing Q values."
            )

            if df_melted.empty:
                logger.error("DataFrame is empty after melting Q columns.")
                return 1

            # Assign the melted DataFrame back to self.df
            self.df = df_melted

            # Validate headers after melting
            expected_headers = ["Time", "ds", "Q"]
            if not self.check_headers_match(test_headers=self.df.columns.tolist()):
                logger.error(f"{self.file_name}: Header mismatch after melting.")
                return 2

            logger.info(
                f"{self.file_name}: Successfully melted Q columns into long format."
            )

            return 0

        except DataValidationError as dve:
            logger.error(f"{self.file_name}: Data validation error: {dve}")
            return 3
        except ProcessorError as pe:
            logger.error(f"{self.file_name}: Processor error: {pe}")
            return 3
        except Exception as e:
            logger.exception(
                f"{self.file_name}: Unexpected error during processing: {e}"
            )
            return 3
