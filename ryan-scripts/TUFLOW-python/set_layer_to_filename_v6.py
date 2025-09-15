# ryan-scripts\TUFLOW-python\set_layer_to_filename_v5.py
# 250913
from collections.abc import Iterator
from pathlib import Path
import geopandas as gpd
import pyogrio
import shutil
import uuid
from colorama import init, Fore, Style
import sys

# === Configuration ===
# Toggle to search subfolders recursively underneath the Python file.
SEARCH_RECURSIVELY: bool = False  # Set True to scan all nested folders under the script directory.
GPKG_PATTERN: str = "*.gpkg"  # File pattern for GeoPackages


# Function to apply color in terminal output
def print_colored(message: str, color: str) -> None:
    print(color + message + Style.RESET_ALL)


def press_any_key(prompt: str = "Press any key to exit...") -> None:
    """
    Windows: uses msvcrt (stdlib) to wait for a single keypress.
    Elsewhere: falls back to input().
    """
    if sys.platform == "win32":
        try:
            import msvcrt  # stdlib on Windows

            print(prompt, end="", flush=True)
            msvcrt.getch()
            print()
            return
        except Exception:
            # Fall through to input() if anything odd happens
            pass
    try:
        input(prompt)
    except EOFError:
        pass


# Initialization function to set up environment and return colors
def initialize(target_dir: str | None = None) -> tuple[Path, str, str]:
    """
    Returns the base directory to process, plus colours.
    - If target_dir is provided, use it (must be a drive-letter path if you plan to chdir).
    - Else use the script's folder, keeping the Q: form by using .absolute(), not .resolve().
    """
    # Initialize colorama for Windows support
    init()
    base: Path = Path(target_dir).expanduser() if target_dir else Path(__file__).absolute().parent
    # Return success and failure colors
    return base, Fore.GREEN, Fore.RED


# Helper function to check if we should skip the file
def should_skip_file(layers: list[list[str]], gpkg: str, fail_colour: str) -> bool:
    if len(layers) > 1:
        print_colored(message=f"Skipping {gpkg} as it contains multiple layers:", color=fail_colour)
        for layer in layers:
            print_colored(message=f"  - {layer[0]}", color=fail_colour)
        return True
    return False


# Helper function to check if renaming is needed
def is_layer_name_correct(old_layer_name: str, new_layer_name: str, gpkg: str, success_colour: str) -> bool:
    if old_layer_name == new_layer_name:
        print_colored(message=f"Skipping {gpkg} as the layer name is already correct.", color=success_colour)
        return True
    return False


# Function to rename the layer in a GeoPackage file
def rename_layer_in_geopackage(
    gpkg: str,
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
            message=f"Successfully renamed layer in {gpkg} to {new_layer_name} (original name: {old_layer_name}).",
            color=success_colour,
        )
    except Exception as e:
        # Error message with failure color and exception details
        print_colored(message=f"Failed to process {gpkg}: {str(e)}", color=fail_colour)


# Function to process all GeoPackage files in the directory
def process_geopackage_files(base_dir: Path, success_colour: str, fail_colour: str) -> None:
    # No chdir needed; operate with absolute paths to avoid UNC/cwd issues entirely
    # Loop over all .gpkg files in the current directory
    # (If SEARCH_RECURSIVELY is True, this will search all subfolders as well.)
    iter_paths: Iterator[Path] = (
        base_dir.rglob(pattern=GPKG_PATTERN) if SEARCH_RECURSIVELY else base_dir.glob(pattern=GPKG_PATTERN)
    )
    for gpkg_path in iter_paths:
        gpkg: str = str(gpkg_path)
        # Use pyogrio to list layers in the GeoPackage
        layers: list[list[str]] = pyogrio.list_layers(gpkg)
        if should_skip_file(layers=layers, gpkg=gpkg, fail_colour=fail_colour):
            continue

        # Extract filename without the .gpkg extension
        new_layer_name: str = gpkg_path.stem
        old_layer_name: str = layers[0][0]
        if is_layer_name_correct(
            old_layer_name=old_layer_name, new_layer_name=new_layer_name, gpkg=gpkg, success_colour=success_colour
        ):
            continue

        # Rename the layer in the GeoPackage
        rename_layer_in_geopackage(
            gpkg=gpkg,
            new_layer_name=new_layer_name,
            old_layer_name=old_layer_name,
            success_colour=success_colour,
            fail_colour=fail_colour,
        )


# Main function to execute the script
def main() -> None:
    # OPTION A: Use the script's folder (keeps Q: if you launched it that way)
    base_dir, success_colour, fail_colour = initialize()

    # OPTION B: Explicitly target a folder (drive-letter path); uncomment and set if desired
    # base_dir, success_colour, fail_colour = initialize(
    #     r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\model"
    # )

    # If you really want to change cwd (not required), only chdir to a drive-letter path:
    # if base_dir.drive:  # e.g., 'Q:'
    #     os.chdir(base_dir)

    process_geopackage_files(base_dir=base_dir, success_colour=success_colour, fail_colour=fail_colour)

    # Wait for user input before exiting (Windows only)
    if sys.platform == "win32":
        press_any_key()


if __name__ == "__main__":
    main()
