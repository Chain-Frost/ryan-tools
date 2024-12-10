# ryan_library/processors/tuflow/cmx_processor.py

import pandas as pd
from loguru import logger
from .base_processor import BaseProcessor
from pathlib import Path


class CmxProcessor(BaseProcessor):
    """
    Processor for '_1d_Cmx.csv' files.
    """

    def __init__(self, file_path: Path):
        super().__init__(file_path)
        # No manual configuration needed; handled via BaseProcessor

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Cmx.csv' file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed CMX data.
        """
        logger.info(f"Starting processing of CMX file: {self.file_path}")

        # Read the CSV using the shared method
        df, status = self.read_max_csv(
            usecols=[1, 2, 3, 4, 5], dtype=self.expected_headers
        )

        if status != 0:
            # If there was an error, set self.df to empty and return
            logger.error(
                f"Processing aborted for file: {self.file_path} due to previous errors."
            )
            self.df = pd.DataFrame()
            return self.df
        self.df = df
        # Reshape the DataFrame to have separate rows for QMax and VMax
        try:
            # Create QMax DataFrame
            q_df = self.df[["Chan ID", "TimeQMax", "QMax"]].copy()
            q_df["VMax"] = pd.NA  # Add VMax column with NA

            # Create VMax DataFrame
            v_df = self.df[["Chan ID", "TimeVMax", "VMax"]].copy()
            v_df["QMax"] = pd.NA  # Add QMax column with NA

            # Concatenate QMax and VMax DataFrames
            cleaned_df = pd.concat([q_df, v_df], ignore_index=True)
            logger.debug(f"Reshaped DataFrame:\n{cleaned_df.head()}")
        except KeyError as e:
            logger.error(
                f"Missing expected columns during reshaping for file {self.file_path}: {e}"
            )
            self.df = pd.DataFrame()
            return self.df

        # Handle any malformed data if necessary (e.g., all NaNs)
        malformed_mask = (
            cleaned_df[["Chan ID", "TimeQMax", "QMax", "VMax"]].isnull().all(axis=1)
        )
        if malformed_mask.any():
            malformed_entries = cleaned_df.loc[malformed_mask, "Chan ID"].unique()
            logger.warning(
                f"Malformed entries detected in file {self.file_path}: {malformed_entries}"
            )
            # Optionally, remove malformed entries
            cleaned_df = cleaned_df[~malformed_mask]
            logger.debug(
                f"DataFrame after removing malformed entries:\n{cleaned_df.head()}"
            )

        # Assign the cleaned DataFrame to the instance variable
        self.df = cleaned_df

        logger.debug(f"Cleaned DataFrame head:\n{self.df.head()}")

        # Apply output transformations (renaming and data types)
        try:
            self.apply_output_transformations()
        except Exception as e:
            logger.error(
                f"Error applying output transformations for file {self.file_path}: {e}"
            )
            self.df = pd.DataFrame()
            return self.df

        # Mark the processing as complete and add any common columns
        self.processed = True
        self.add_common_columns()

        logger.info(f"Completed processing of CMX file: {self.file_path}")

        return self.df
