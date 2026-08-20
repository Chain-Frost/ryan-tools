"""Import configuration for newly promoted script wrappers."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_DIRS = (
    REPO_ROOT / "ryan-scripts" / "TUFLOW-python",
    REPO_ROOT / "ryan-scripts" / "gdal-python",
    REPO_ROOT / "ryan-scripts" / "RORB-python",
    REPO_ROOT / "ryan-scripts" / "hydrology-python",
    REPO_ROOT / "ryan-scripts" / "other",
)
for script_directory in SCRIPT_DIRS:
    sys.path.insert(0, str(script_directory))
