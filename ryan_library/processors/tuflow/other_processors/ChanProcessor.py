# ryan_library/processors/tuflow/ChanProcessor.py

from ..base_processor import ProcessorStatus
from ..max_data_processor import MaxDataProcessor
import pandas as pd
from loguru import logger


class ChanProcessor(MaxDataProcessor):
    """Processor for '_1d_Chan.csv' files."""

    def process(self) -> None:
        """Process the '_1d_Chan.csv' file and return a cleaned DataFrame.
        Returns:
            pd.DataFrame: Processed Chan data."""
        logger.info(f"Starting processing of Chan file: {self.file_path}")

        try:
            status: ProcessorStatus = self.read_maximums_csv()

            if status != ProcessorStatus.SUCCESS:
                logger.error(f"Processing aborted for file: {self.file_path} due to previous errors.")
                self.df = pd.DataFrame()
                return

            # Calculate Height
            if "LBUS Obvert" in self.df.columns and "US Invert" in self.df.columns:
                self.df["Height"] = self.df["LBUS Obvert"] - self.df["US Invert"]
                logger.debug("Calculated 'Height' column.")
            else:
                logger.error(f"Required columns for calculating Height are missing in file {self.file_path}.")
                self.df = pd.DataFrame()
                return

            # Rename 'Channel' to 'Chan ID'
            if "Channel" in self.df.columns:
                self.df.rename(columns={"Channel": "Chan ID"}, inplace=True)
                logger.debug("Renamed 'Channel' to 'Chan ID'.")
            else:
                logger.error(f"'Channel' column is missing in file {self.file_path}.")
                self.df = pd.DataFrame()
                return

            # Rename 'LBUS Obvert' to 'US Obvert'
            if "LBUS Obvert" in self.df.columns:
                self.df.rename(columns={"LBUS Obvert": "US Obvert"}, inplace=True)
                logger.debug("Renamed 'LBUS Obvert' to 'US Obvert'.")
            else:
                logger.error(f"'LBUS Obvert' column is missing in file {self.file_path}.")
                self.df = pd.DataFrame()
                return

            # Proceed with common processing steps from BaseProcessor
            self.add_common_columns()
            self.apply_output_transformations()

            # Validate data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of Chan file: {self.file_path}")

            return

        except Exception as e:
            logger.error(f"Failed to process Chan file {self.file_path}: {e}")
            self.df = pd.DataFrame()
            return
