# ryan_library/processors/tuflow/timeseries_processor.py

import pandas as pd
from loguru import logger
from .base_processor import BaseProcessor, DataValidationError, ProcessorError


class TimeSeriesProcessor(BaseProcessor):
    """Intermediate base class for processing timeseries data types."""

    def process_timeseries_raw_dataframe(self, data_type: str) -> int:
        """
        Process the raw reshaped timeseries DataFrame by melting or transforming as needed.

        Returns:
            int: Status code.
        """
        # Implement the common process_timeseries_raw_dataframe logic here
        try:
            logger.debug("Starting to process the raw timeseries DataFrame.")

            # This method can be abstracted based on data_type
            if data_type == "H":
                return self._process_h_timeseries()
            elif data_type == "Q":
                return self._process_q_timeseries()
            else:
                logger.error(
                    f"Unsupported data_type '{data_type}' for timeseries processing."
                )
                return 3

        except (DataValidationError, ProcessorError) as e:
            logger.error(f"{self.file_name}: Processing error: {e}")
            return 3
        except Exception as e:
            logger.exception(f"{self.file_name}: Unexpected error: {e}")
            return 3

    def _process_q_timeseries(self) -> int:
        """Process 'Q' timeseries data."""
        try:
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

        except Exception as e:
            logger.exception(
                f"{self.file_name}: Failed to process Q timeseries data: {e}"
            )
            return 3

    def _process_h_timeseries(self) -> int:
        """Process 'H' timeseries data."""
        try:
            # Implement the H timeseries processing logic
            # This could include methods like _reshape_h_data, etc.
            # Similar to what was outlined in your original QProcessor

            # Example:
            category_type = (
                "Chan ID" if "1d" in self.name_parser.suffixes else "Location"
            )
            df_melted = self._reshape_h_data(df=self.df, category_type=category_type)
            self.df = df_melted
            self._apply_final_transformations(data_type="H")
            self.processed = True
            logger.info(f"{self.file_name}: Successfully processed H timeseries data.")

            return 0

        except Exception as e:
            logger.exception(
                f"{self.file_name}: Failed to process H timeseries data: {e}"
            )
            return 3

    # Implement any additional shared timeseries processing methods here
