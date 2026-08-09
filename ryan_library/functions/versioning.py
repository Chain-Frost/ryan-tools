# ryan_library/functions/versioning.py
"""Versioning helpers for ryan_library."""

from importlib import metadata


def get_tools_version(package: str = "ryan_functions") -> str:
    """Return the installed version of ``package`` if available."""
    try:
        return metadata.version(distribution_name=package)
    except metadata.PackageNotFoundError:
        return "unknown"
