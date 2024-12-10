# ryan_library/processors/tuflow/nmx_processor.py

import pandas as pd
from loguru import logger
from .base_processor import BaseProcessor
from pathlib import Path


class NmxProcessor(BaseProcessor):
    """
    Processor for '_1d_Nmx.csv' files.
    """

    def __init__(self, file_path: Path):
        super().__init__(file_path)
        # No manual configuration needed; handled via BaseProcessor

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Nmx.csv' file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed NMX data.
        """
        logger.info(f"Starting processing of NMX file: {self.file_path}")

        # Read the CSV using the shared method
        df, status = self.read_max_csv(usecols=[1, 2, 3], dtype=self.expected_headers)

        if status != 0:
            # If there was an error, set self.df to empty and return
            logger.error(
                f"Processing aborted for file: {self.file_path} due to previous errors."
            )
            self.df = pd.DataFrame()
            return self.df

        # Extract 'Chan ID' and 'node_suffix' by splitting 'Node ID'
        try:
            df["Chan ID"] = df["Node ID"].str[:-2]
            df["node_suffix"] = df["Node ID"].str[-2:]
            logger.debug(
                f"Extracted Chan ID and node_suffix:\n{df[['Chan ID', 'node_suffix']].head()}"
            )
        except KeyError as e:
            logger.error(
                f"Missing expected columns during extraction for file {self.file_path}: {e}"
            )
            self.df = pd.DataFrame()
            return self.df

        # Identify abnormal node_suffix values
        valid_suffixes = {".1", ".2"}
        abnormal_suffix_mask = ~df["node_suffix"].isin(valid_suffixes)
        if abnormal_suffix_mask.any():
            abnormal_values = df.loc[abnormal_suffix_mask, "node_suffix"].unique()
            logger.warning(
                f"Abnormal 'node_suffix' values detected in file {self.file_path}: {abnormal_values}"
            )
            # Optionally, exclude these rows
            df = df[~abnormal_suffix_mask]
            logger.debug(f"DataFrame after removing abnormal suffixes:\n{df.head()}")

        # Create 'US_h' and 'DS_h' columns based on 'node_suffix' using vectorized operations
        df["US_h"] = df["HMax"].where(df["node_suffix"] == ".1", pd.NA)
        df["DS_h"] = df["HMax"].where(df["node_suffix"] == ".2", pd.NA)
        logger.debug(f"Created US_h and DS_h columns:\n{df[['US_h', 'DS_h']].head()}")

        # Select the required columns
        cleaned_df = df[["Chan ID", "TimeHMax", "US_h", "DS_h"]].copy()
        logger.debug(f"Selected required columns:\n{cleaned_df.head()}")

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

        logger.info(f"Completed processing of NMX file: {self.file_path}")

        return self.df
