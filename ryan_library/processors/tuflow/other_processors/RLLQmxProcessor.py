# ryan_library/processors/tuflow/other_processors/RLLQmxProcessor.py

__lazy_modules__: list[str] = ["pandas"]
import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor, ProcessorStatus


class RLLQmxProcessor(BaseProcessor):
    """Processor for '_RLL_Qmx.csv' files produced by the Reporting Location Lines maximum export."""

    # Only true for a valid reshaped source fully excluded by selected locations.
    filtered_empty: bool = False

    def process(self) -> None:
        """Process the '_RLL_Qmx.csv' file and modify self.df in place."""
        logger.info(f"Starting processing of RLL Qmx file: {self.log_path}")
        self.filtered_empty = False
        try:
            status: ProcessorStatus = self.read_maximums_csv()
            if status != ProcessorStatus.SUCCESS:
                logger.error(f"Processing aborted for file: {self.log_path} due to previous errors.")
                self.df = pd.DataFrame()
                return

            self._reshape_rll_qmx_data()
            if self.df.empty:
                # Missing required columns or invalid reshaping is a failure,
                # not an intentional location-filter skip.
                return

            self.apply_entity_filter()
            if self.applied_entity_filter and self.df.empty:
                self.filtered_empty = True
                logger.info("{}: No requested RLL Qmx locations found; skipping file.", self.file_name)
                return
            self.add_common_columns()
            self.apply_output_transformations()

            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of RLL Qmx file: {self.log_path}")
        except Exception as exc:
            logger.error(f"Failed to process RLL Qmx file {self.log_path}: {exc}")
            self.df = pd.DataFrame()
            return

    def _reshape_rll_qmx_data(self) -> None:
        """Rename columns and align schema with other maximum datasets."""
        required_columns: list[str] = ["ID", "Qmax", "Time Qmax", "dQmax", "Time dQmax", "H"]
        missing_columns: list[str] = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            logger.error(f"{self.file_name}: Missing required columns for RLL Qmx processing: {missing_columns}")
            self.df = pd.DataFrame()
            return

        rename_map: dict[str, str] = {
            "ID": "Chan ID",
            "Qmax": "Q",
            "Time Qmax": "Time",
            "dQmax": "dQ",
            "Time dQmax": "Time dQ",
        }
        self.df = self.df.rename(columns=rename_map)

        ordered_columns: list[str] = ["Chan ID", "Time", "Q", "dQ", "Time dQ", "H"]
        self.df = self.df[ordered_columns]
