# ryan_library/processors/tuflow/max_data_processor.py

from typing import Any

import pandas as pd
from loguru import logger

from .base_processor import BaseProcessor


class MaxDataProcessor(BaseProcessor):
    """Intermediate base class for processing maximum data types."""

    def read_maximums_csv(self) -> int:
        """Read a ``Maximums``/``ccA`` CSV into :attr:`df` using configuration metadata.

        Returns:
            int: Status code where ``0`` is success and non-zero values indicate
                data availability or validation issues.
        """

        usecols: list[str] = list(self.columns_to_use.keys())
        dtype_mapping: dict[str, str] = {column: dtype for column, dtype in self.columns_to_use.items() if dtype}

        read_csv_kwargs: dict[str, Any] = {
            "filepath_or_buffer": self.file_path,
            "header": 0,
            "skipinitialspace": True,
        }
        if usecols:
            read_csv_kwargs["usecols"] = usecols
        if dtype_mapping:
            read_csv_kwargs["dtype"] = dtype_mapping

        try:
            df: pd.DataFrame = pd.read_csv(**read_csv_kwargs)
            logger.debug(f"CSV file '{self.file_name}' read successfully with {len(df)} rows.")
        except Exception as exc:
            logger.exception(f"{self.file_name}: Failed to read CSV file '{self.log_path}': {exc}")
            return 3

        if df.empty:
            logger.error(f"{self.file_name}: No data found in file: {self.log_path}")
            return 1

        self.raw_df = df.copy(deep=False)

        if not self.check_headers_match(df.columns.tolist()):
            return 2

        if usecols:
            df = df.loc[:, usecols]

        self.df = df
        return 0
