"""Compatibility wrapper for relocated wrapper utilities."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.functions.wrapper_utils")

from ryan_library.functions.wrapper_utils import *  # noqa: F401,F403

