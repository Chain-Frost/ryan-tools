# ryan_library/processors/tuflow/cmx_processor.py

import pandas as pd
from loguru import logger
from pathlib import Path
from .base_processor import BaseProcessor


class CmxProcessor(BaseProcessor):
    """
    Processor for '_1d_Cmx.csv' files.
    """

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Cmx.csv' file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed CMX data.
        """
        logger.info(f"Starting processing of CMX file: {self.file_path}")

        # Read the CSV using the dedicated method
        df, status = self.read_maximums_csv()

        if status != 0:
            # If there was an error, set self.df to empty and return
            logger.error(
                f"Processing aborted for file: {self.file_path} due to previous errors."
            )
            self.df = pd.DataFrame()
            return self.df

        # Reshape the DataFrame to have separate rows for Qmax and Vmax
        try:
            # Ensure expected columns are present
            required_columns = ["Chan ID", "Time Qmax", "Qmax", "Vmax", "Time Vmax"]
            missing_columns = [
                col for col in required_columns if col not in self.df.columns
            ]
            if missing_columns:
                logger.error(
                    f"Missing required columns for reshaping in file {self.file_path}: {missing_columns}"
                )
                self.df = pd.DataFrame()
                return self.df

            # Create QMax DataFrame
            q_df = self.df[["Chan ID", "Time Qmax", "Qmax"]].copy()
            q_df.rename(columns={"Time Qmax": "Time", "Qmax": "Q"}, inplace=True)
            q_df["V"] = pd.NA  # Add V column with NA

            # Create VMax DataFrame
            v_df = self.df[["Chan ID", "Time Vmax", "Vmax"]].copy()
            v_df.rename(columns={"Time Vmax": "Time", "Vmax": "V"}, inplace=True)
            v_df["Q"] = pd.NA  # Add Q column with NA

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
        malformed_mask = cleaned_df[["Chan ID", "Time", "Q", "V"]].isnull().all(axis=1)
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

        # Apply output transformations (data type assignments)
        try:
            self.apply_output_transformations()
        except Exception as e:
            logger.error(
                f"Error applying output transformations for file {self.file_path}: {e}"
            )
            self.df = pd.DataFrame()
            return self.df

        # Add common columns (internalName, rel_path, etc.)
        self.add_common_columns()

        # Apply data types to additional columns
        try:
            self.apply_datatypes_to_df()
        except Exception as e:
            logger.error(
                f"Error applying additional datatypes for file {self.file_path}: {e}"
            )
            self.df = pd.DataFrame()
            return self.df

        # Validate data
        if not self.validate_data():
            logger.error(f"{self.file_name}: Data validation failed.")
            self.df = pd.DataFrame()
            return self.df

        self.processed = True
        logger.info(f"Completed processing of CMX file: {self.file_path}")

        return self.df
