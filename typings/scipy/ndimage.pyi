"""Binary morphology contracts used by hourly hotspot connected components."""

import numpy as np
from numpy.typing import NDArray


def generate_binary_structure(rank: int, connectivity: int) -> NDArray[np.bool_]: ...

def label(data: NDArray[np.bool_], structure: NDArray[np.bool_] | None = None) -> tuple[NDArray[np.int32], int]: ...

def find_objects(data: NDArray[np.int32], max_label: int = 0) -> list[tuple[slice, slice] | None]: ...
