# ryan_library/processors/tuflow/nmx_processor.py

import pandas as pd  # type: ignore[import-untyped]
from loguru import logger
from .base_processor import BaseProcessor, ProcessorStatus


# this processor does not understand pits - only index 1 or 2 for standard culverts
class NmxProcessor(BaseProcessor):
    """
    Processor for '_1d_Nmx.csv' files.
    """

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Nmx.csv' file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed NMX data.
        """
        logger.info(f"Starting processing of NMX file: {self.file_path}")

        try:
            # Nmx is Maximums type, so use read_maximums_csv
            status: ProcessorStatus = self.read_maximums_csv()

            if status is not ProcessorStatus.SUCCESS:
                logger.error(
                    f"Processing aborted for file: {self.file_path} due to previous errors."
                )
                self.df = pd.DataFrame()
                return self.df

            # Perform NMX-specific data extraction and transformation
            self._extract_and_transform_nmx_data()

            # Proceed with common processing steps from BaseProcessor
            self.add_common_columns()
            self.apply_output_transformations()

            # Validate data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return self.df

            self.processed = True
            logger.info(f"Completed processing of NMX file: {self.file_path}")

            return self.df

        except Exception as e:
            logger.error(f"Failed to process NMX file {self.file_path}: {e}")
            self.df = pd.DataFrame()
            return self.df

    def _extract_and_transform_nmx_data(self) -> None:
        """
        Extract and transform NMX-specific data from the DataFrame.
        """
        logger.debug("Starting NMX data extraction and transformation.")

        try:
            # Split 'Node ID' into 'Chan ID' and 'node_suffix'
            self.df[["Chan ID", "node_suffix"]] = self.df["Node ID"].str.rsplit(
                ".", n=1, expand=True
            )

            # Validate 'node_suffix' values
            # this processor does not understand pits
            valid_suffixes = {"1", "2"}
            abnormal_suffix_mask = ~self.df["node_suffix"].isin(valid_suffixes)
            if abnormal_suffix_mask.any():
                abnormal_values = self.df.loc[
                    abnormal_suffix_mask, "node_suffix"
                ].unique()
                logger.warning(
                    f"Abnormal 'node_suffix' values detected in file {self.file_path}: {abnormal_values}"
                )
                # Exclude these rows
                self.df = self.df[~abnormal_suffix_mask]

            # Pivot the DataFrame
            pivot_df = self.df.pivot_table(
                index=["Chan ID", "Time Hmax"],
                columns="node_suffix",
                values="Hmax",
                aggfunc="first",
            ).reset_index()

            pivot_df.rename(
                columns={"1": "US_h", "2": "DS_h", "Time Hmax": "Time"}, inplace=True
            )

            expected_pivot_columns = ["Chan ID", "Time", "US_h", "DS_h"]
            missing_pivot_columns = [
                col for col in expected_pivot_columns if col not in pivot_df.columns
            ]
            if missing_pivot_columns:
                logger.error(
                    f"Missing expected columns after pivoting for file {self.file_path}: {missing_pivot_columns}"
                )
                self.df = pd.DataFrame()
                return

            self.df = pivot_df[expected_pivot_columns]

        except KeyError as e:
            logger.error(
                f"Missing expected columns during extraction for file {self.file_path}: {e}"
            )
            self.df = pd.DataFrame()

        except Exception as e:
            logger.error(
                f"Unexpected error during NMX data extraction for file {self.file_path}: {e}"
            )
            self.df = pd.DataFrame()
