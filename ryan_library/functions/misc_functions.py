# ryan_library/functions/misc_functions.py

from __future__ import annotations

from typing import Any


def __getattr__(name: str) -> Any:
    """Lazily load functionalities from their new purpose-built modules to preserve compatibility."""

    if name == "get_tools_version":
        import ryan_library.functions.versioning as versioning

        return getattr(versioning, name)

    if name == "calculate_pool_size":
        import ryan_library.functions.multiprocessing_helpers as multiprocessing_helpers

        return getattr(multiprocessing_helpers, name)

    if name in {"split_strings", "split_strings_in_dict"}:
        import ryan_library.functions.string_helpers as string_helpers

        return getattr(string_helpers, name)

    if name in {
        "ExportContent",
        "build_data_dictionary",
        "ExcelExporter",
        "export_dataframes",
        "save_to_excel",
        "ParquetCompression",
        "DATA_DICTIONARY_SHEET_NAME",
    }:
        import ryan_library.functions.excel_export as excel_export

        return getattr(excel_export, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
