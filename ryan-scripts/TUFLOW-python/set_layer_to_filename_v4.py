import geopandas as gpd
import pyogrio
import os
import shutil
import uuid
from colorama import init, Fore, Style
import sys


# Function to apply color in terminal output
def print_colored(message: str, color: str) -> None:
    print(color + message + Style.RESET_ALL)


# Initialization function to set up environment and return colors
def initialize() -> tuple[str, str]:
    # Initialize colorama for Windows support
    init()
    # Set the working directory to the script's location
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Return success and failure colors
    return Fore.GREEN, Fore.RED


# Helper function to check if we should skip the file
def should_skip_file(layers: list[list[str]], gpkg: str) -> bool:
    if len(layers) > 1:
        print(f"Skipping {gpkg} as it contains multiple layers:")
        for layer in layers:
            print(f"  - {layer[0]}")
        return True
    return False


# Helper function to check if renaming is needed
def is_layer_name_correct(old_layer_name: str, new_layer_name: str, gpkg: str) -> bool:
    if old_layer_name == new_layer_name:
        print(f"Skipping {gpkg} as the layer name is already correct.")
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
        gdf = gpd.read_file(gpkg)
        # gdf is a GeoDataFrame

        # Perform the layer renaming operation
        temp_gpkg = f"{uuid.uuid4()}.gpkg"  # Create a temporary GeoPackage
        gdf.to_file(temp_gpkg, layer=new_layer_name, driver="GPKG")

        # Replace the original file with the new file
        shutil.move(temp_gpkg, gpkg)

        # Success message with original layer name in brackets
        print_colored(
            f"Successfully renamed layer in {gpkg} to {new_layer_name} (original name: {old_layer_name}).",
            success_colour,
        )
    except Exception as e:
        # Error message with failure color and exception details
        print_colored(f"Failed to process {gpkg}: {str(e)}", fail_colour)


# Function to process all GeoPackage files in the directory
def process_geopackage_files(success_colour: str, fail_colour: str) -> None:
    # Loop over all .gpkg files in the current directory
    for gpkg in [f for f in os.listdir(".") if f.endswith(".gpkg")]:
        # Use pyogrio to list layers in the GeoPackage
        layers: list[list[str]] = pyogrio.list_layers(gpkg)

        if should_skip_file(layers, gpkg):
            continue

        # Extract filename without the .gpkg extension
        new_layer_name: str = os.path.splitext(gpkg)[0]
        old_layer_name: str = layers[0][0]  # Get the actual layer's name

        if is_layer_name_correct(
            old_layer_name=old_layer_name, new_layer_name=new_layer_name, gpkg=gpkg
        ):
            continue

        # Rename the layer in the GeoPackage
        rename_layer_in_geopackage(
            gpkg, new_layer_name, old_layer_name, success_colour, fail_colour
        )


# Main function to execute the script
def main() -> None:
    success_colour, fail_colour = initialize()
    process_geopackage_files(success_colour, fail_colour)

    # Wait for user input before exiting (Windows only; not tested on Linux)
    if sys.platform == "win32":
        os.system("pause")


if __name__ == "__main__":
    main()
