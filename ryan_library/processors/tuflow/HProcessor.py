"""Placeholder processor for TUFLOW H timeseries data.

This stub will be implemented once the Q-processor path is stable so the shared
infrastructure can be reused confidently.
"""

from __future__ import annotations

import pandas as pd

from .base_processor import BaseProcessor


class HProcessor(BaseProcessor):
    """Stub processor for `_1d_H.csv` files until the Q-processor path is stable."""

    def process(self) -> pd.DataFrame:
        """Raise ``NotImplementedError`` until H processing is available.

        TODO: Implement HProcessor once the Q-processor path is stable.
        """
        raise NotImplementedError(
            "HProcessor is not implemented. It will be completed once the Q-processor path is stable."
        )
