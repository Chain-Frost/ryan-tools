"""Placeholder processor for TUFLOW V timeseries data.

This stub will be implemented once the Q-processor path is stable so the shared
infrastructure can be reused confidently.
"""

from __future__ import annotations

import pandas as pd

from .base_processor import BaseProcessor


class VProcessor(BaseProcessor):
    """Stub processor for `_1d_V.csv` files until the Q-processor path is stable."""

    def process(self) -> pd.DataFrame:
        """Raise ``NotImplementedError`` until V processing is available.

        TODO: Implement VProcessor once the Q-processor path is stable.
        """
        raise NotImplementedError(
            "VProcessor is not implemented. It will be completed once the Q-processor path is stable."
        )
