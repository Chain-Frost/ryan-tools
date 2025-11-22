# ryan_library/processors/tuflow/rll_qmx_processor.py

import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor, ProcessorStatus


class RLLQmxProcessor(BaseProcessor):
    """Processor for '_RLL_Qmx.csv' files produced by the Reporting Location Lines maximum export."""

    def process(self) -> None:
        """Process the '_RLL_Qmx.csv' file and modify self.df in place."""
        logger.info(f"Starting processing of RLL Qmx file: {self.file_path}")
        try:
            status: ProcessorStatus = self.read_maximums_csv()
            if status != ProcessorStatus.SUCCESS:
                logger.error(f"Processing aborted for file: {self.file_path} due to previous errors.")
                self.df = pd.DataFrame()
                return

            self._reshape_rll_qmx_data()

            self.add_common_columns()
            self.apply_output_transformations()

            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of RLL Qmx file: {self.file_path}")
        except Exception as exc:
            logger.error(f"Failed to process RLL Qmx file {self.file_path}: {exc}")
            self.df = pd.DataFrame()
            return

    def _reshape_rll_qmx_data(self) -> None:
        """Rename columns and align schema with other maximum datasets."""
        required_columns: list[str] = ["ID", "Qmax", "Time Qmax", "dQmax", "Time dQmax", "H"]
        missing_columns: list[str] = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            logger.error(
                f"{self.file_name}: Missing required columns for RLL Qmx processing: {missing_columns}"
            )
            self.df = pd.DataFrame()
            return

        rename_map: dict[str, str] = {
            "Qmax": "Q",
            "Time Qmax": "Time",
            "dQmax": "dQ",
            "Time dQmax": "Time dQ",
        }
        self.df.rename(columns=rename_map, inplace=True)

        ordered_columns: list[str] = ["ID", "Time", "Q", "dQ", "Time dQ", "H"]
        self.df = self.df[ordered_columns]
