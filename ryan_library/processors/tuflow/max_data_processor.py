# ryan_library/processors/tuflow/max_data_processor.py

import pandas as pd  # type: ignore[import-untyped]
from loguru import logger
from .base_processor import BaseProcessor, DataValidationError, ProcessorError, ProcessorStatus


class MaxDataProcessor(BaseProcessor):
    """Intermediate base class for processing maximum data types."""

    def read_maximums_csv(self) -> ProcessorStatus:
        """Read ``Maximums`` or ``ccA`` CSV files and return a status flag."""
        # [Implementation as previously defined in BaseProcessor]
        # You might move the read_maximums_csv from BaseProcessor to here.
        # Or if already in BaseProcessor, ensure it's only used by MaxDataProcessor
        return super().read_maximums_csv()

    # Add any other methods specific to maximum data processing
