from __future__ import annotations

import sys
from pathlib import Path

# Re-export PyHMA to maintain existing imports.
from .PyHMA import *  # noqa: F401,F403

# Make the run-hy8 submodule importable without installing it separately.
RUN_HY8_SRC: Path = Path(__file__).with_name("run_hy8") / "src"
if RUN_HY8_SRC.is_dir():
    run_hy8_src = str(RUN_HY8_SRC.resolve())
    if run_hy8_src not in sys.path:
        sys.path.append(run_hy8_src)
