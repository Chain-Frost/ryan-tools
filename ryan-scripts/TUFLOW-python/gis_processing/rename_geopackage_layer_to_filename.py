"""Rename a single GeoPackage layer to match its GeoPackage filename.

Place this script with the GeoPackages or pass a target path by adapting
:func:`main`. Set ``SEARCH_RECURSIVELY`` and ``GPKG_PATTERN`` as required, then
run ``python rename_geopackage_layer_to_filename.py``. Multi-layer GeoPackages are skipped.

Each changed GeoPackage is rebuilt through a temporary file and replaces the
original. Keep a backup and test on copied data first; the script pauses for a
keypress when complete. Console paths are relative to the scan directory;
renames are green, skips yellow and errors red.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

# 250913
import shutil
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import geopandas as gpd
import pyogrio
from colorama import Fore, Style, init

SCRIPT_VERSION = "2026-10-21.1"

# === Configuration ===
# Toggle to search subfolders recursively underneath the Python file.
SEARCH_RECURSIVELY: bool = False  # Set True to scan all nested folders under the script directory.
GPKG_PATTERN: str = "*.gpkg"  # File pattern for GeoPackages


# Function to apply color in terminal output
def print_colored(message: str, color: str) -> None:
    print(color + message + Style.RESET_ALL)


def press_any_key(prompt: str = "Press any key to exit...") -> None:
    """Windows: uses msvcrt (stdlib) to wait for a single keypress.
    Elsewhere: falls back to input().
    """
    if sys.platform == "win32":
        try:
            import msvcrt  # stdlib on Windows

            print(prompt, end="", flush=True)
            msvcrt.getch()
            print()
            return
        except Exception:  # noqa: S110 - fall back to input() if the Windows console API is unavailable
            # Fall through to input() if anything odd happens
            pass
    try:
        input(prompt)
    except EOFError:
        pass


# Initialization function to set up environment and return colors
def initialize(target_dir: str | None = None) -> tuple[Path, str, str, str]:
    """Returns the base directory to process, plus colours.
    - If target_dir is provided, use it (must be a drive-letter path if you plan to chdir).
    - Else use the script's folder, keeping the Q: form by using .absolute() without dereferencing linked paths.
    """
    # Initialize colorama for Windows support
    init()
    base: Path = Path(target_dir).expanduser().absolute() if target_dir else Path(__file__).absolute().parent
    # Separate success, skipped and failure statuses in the console.
    return base, Fore.GREEN, Fore.YELLOW, Fore.RED


# Helper function to check if we should skip the file
def should_skip_file(layers: list[list[str]], gpkg: str, skip_colour: str) -> bool:
    if len(layers) > 1:
        print_colored(message=f"Skipping {gpkg} as it contains multiple layers:", color=skip_colour)
        for layer in layers:
            print_colored(message=f"  - {layer[0]}", color=skip_colour)
        return True
    return False


# Helper function to check if renaming is needed
def is_layer_name_correct(old_layer_name: str, new_layer_name: str, gpkg: str, skip_colour: str) -> bool:
    if old_layer_name == new_layer_name:
        print_colored(message=f"Skipping {gpkg} as the layer name is already correct.", color=skip_colour)
        return True
    return False


# Function to rename the layer in a GeoPackage file
def rename_layer_in_geopackage(
    gpkg: str,
    display_path: str,
    new_layer_name: str,
    old_layer_name: str,
    success_colour: str,
    fail_colour: str,
) -> None:
    try:
        # Read the existing layer
        gdf: gpd.GeoDataFrame = gpd.read_file(filename=gpkg)
        # gdf is a GeoDataFrame

        # Perform the layer renaming operation
        temp_gpkg: str = f"{uuid.uuid4()}.gpkg"  # Create a temporary GeoPackage
        gdf.to_file(filename=temp_gpkg, layer=new_layer_name, driver="GPKG")

        # Replace the original file with the new file
        shutil.move(src=temp_gpkg, dst=gpkg)

        # Success message with original layer name in brackets
        print_colored(
            message=f"Successfully renamed layer in {display_path} to {new_layer_name} (original name: {old_layer_name}).",
            color=success_colour,
        )
    except Exception as e:
        # Error message with failure color and exception details
        print_colored(message=f"Failed to process {display_path}: {e!s}", color=fail_colour)


# Function to process all GeoPackage files in the directory
def process_geopackage_files(base_dir: Path, success_colour: str, skip_colour: str, fail_colour: str) -> None:
    # Keep absolute paths for I/O; shorten console paths relative to the scan root.
    base_dir = base_dir.absolute()
    # Loop over all .gpkg files in the current directory
    # (If SEARCH_RECURSIVELY is True, this will search all subfolders as well.)
    iter_paths: Iterator[Path] = (
        base_dir.rglob(pattern=GPKG_PATTERN) if SEARCH_RECURSIVELY else base_dir.glob(pattern=GPKG_PATTERN)
    )
    for gpkg_path in iter_paths:
        gpkg: str = str(gpkg_path)
        display_path: str = str(gpkg_path.relative_to(base_dir))
        # Use the full path for GIS operations, never the display-only relative path.
        try:
            layers: list[list[str]] = pyogrio.list_layers(gpkg)
        except Exception as e:
            print_colored(message=f"Failed to inspect {display_path}: {e!s}", color=fail_colour)
            continue
        if len(layers) == 0:
            print_colored(message=f"Failed to inspect {display_path}: no layers found.", color=fail_colour)
            continue
        if should_skip_file(layers=layers, gpkg=display_path, skip_colour=skip_colour):
            continue

        # Extract filename without the .gpkg extension
        new_layer_name: str = gpkg_path.stem
        old_layer_name: str = layers[0][0]
        if is_layer_name_correct(
            old_layer_name=old_layer_name, new_layer_name=new_layer_name, gpkg=display_path, skip_colour=skip_colour
        ):
            continue

        # Rename the layer in the GeoPackage
        rename_layer_in_geopackage(
            gpkg=gpkg,
            display_path=display_path,
            new_layer_name=new_layer_name,
            old_layer_name=old_layer_name,
            success_colour=success_colour,
            fail_colour=fail_colour,
        )


# Main function to execute the script
def main() -> None:
    # OPTION A: Use the script's folder (keeps Q: if you launched it that way)
    base_dir, success_colour, skip_colour, fail_colour = initialize()
    print(
        f"{Path(__file__).name} v{SCRIPT_VERSION} "
        f"(Python {sys.version.split()[0]}, GeoPandas {gpd.__version__}, pyogrio {pyogrio.__version__})"
    )

    # OPTION B: Explicitly target a folder (drive-letter path); uncomment and set if desired
    # base_dir, success_colour, skip_colour, fail_colour = initialize(
    #     r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\model"
    # )

    # If you really want to change cwd (not required), only chdir to a drive-letter path:
    # if base_dir.drive:  # e.g., 'Q:'
    #     os.chdir(base_dir)

    process_geopackage_files(
        base_dir=base_dir, success_colour=success_colour, skip_colour=skip_colour, fail_colour=fail_colour
    )

    # Wait for user input before exiting (Windows only)
    if sys.platform == "win32":
        press_any_key()


if __name__ == "__main__":
    main()
