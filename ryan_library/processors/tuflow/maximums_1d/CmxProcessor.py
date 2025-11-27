# ryan_library/processors/tuflow/cmx_processor.py

from __future__ import annotations

import pandas as pd
from loguru import logger

from ..base_processor import ProcessorStatus
from ..max_data_processor import MaxDataProcessor


class CmxProcessor(MaxDataProcessor):
    """Processor for '_1d_Cmx.csv' files."""

    def process(self) -> None:
        """Process the '_1d_Cmx.csv' file and save to the class df variable."""
        logger.info(f"Starting processing of CMX file: {self.file_path}")

        try:
            status: ProcessorStatus = self.read_maximums_csv()

            if status != ProcessorStatus.SUCCESS:
                logger.error(f"Processing aborted for file: {self.file_path} due to previous errors.")
                self.df = pd.DataFrame()
                return

            # Perform CMX-specific data reshaping
            self._reshape_cmx_data()

            # Handle any malformed data if necessary (e.g., all NaNs)
            self._handle_malformed_data()

            # Proceed with common processing steps from BaseProcessor
            self.add_common_columns()
            self.apply_output_transformations()

            # Validate data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of CMX file: {self.file_path}")

            return None

        except Exception as e:
            logger.error(f"Failed to process CMX file {self.file_path}: {e}")
            self.df = pd.DataFrame()
            return

    def _reshape_cmx_data(self) -> None:
        """Reshape the CMX DataFrame to have separate rows for Qmax and Vmax."""
        logger.debug("Starting CMX data reshaping.")

        # Define required columns for reshaping
        required_columns = ["Chan ID", "Time Qmax", "Qmax", "Vmax", "Time Vmax"]
        missing_columns = [col for col in required_columns if col not in self.df.columns]

        if missing_columns:
            logger.error(f"Missing required columns for reshaping in file {self.file_path}: {missing_columns}")
            self.df = pd.DataFrame()
            return

        try:
            # Create QMax DataFrame
            q_df = self.df[["Chan ID", "Time Qmax", "Qmax"]].copy()
            q_df.rename(columns={"Time Qmax": "Time", "Qmax": "Q"}, inplace=True)
            q_df["V"] = pd.Series([None] * len(q_df), dtype="float64")  # Add V column with NA

            # Create VMax DataFrame
            v_df = self.df[["Chan ID", "Time Vmax", "Vmax"]].copy()
            v_df.rename(columns={"Time Vmax": "Time", "Vmax": "V"}, inplace=True)
            v_df["Q"] = pd.Series([None] * len(v_df), dtype="float64")  # Add Q column with NA

            # Concatenate QMax and VMax DataFrames
            cleaned_df = pd.concat([q_df, v_df], ignore_index=True)
            logger.debug(f"Reshaped DataFrame:\n{cleaned_df.head()}")

            self.df = cleaned_df

        except KeyError as e:
            logger.error(f"Missing expected columns during reshaping for file {self.file_path}: {e}")
            self.df = pd.DataFrame()

    def _handle_malformed_data(self) -> None:
        """Detect and handle any malformed data entries in the DataFrame."""
        logger.debug("Checking for malformed data entries.")

        malformed_mask = self.df[["Chan ID", "Time", "Q", "V"]].isnull().all(axis=1)
        if malformed_mask.any():
            malformed_entries = self.df.loc[malformed_mask, "Chan ID"].unique()
            logger.warning(f"Malformed entries detected in file {self.file_path}: {malformed_entries}")
            # Remove malformed entries
            self.df = self.df[~malformed_mask]
            logger.debug(f"DataFrame after removing malformed entries:\n{self.df.head()}")
