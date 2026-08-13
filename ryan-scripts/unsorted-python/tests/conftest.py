"""Test configuration for experimental scripts kept in the parent directory."""

from __future__ import annotations

import sys
from pathlib import Path

CANDIDATE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(CANDIDATE_DIR))
