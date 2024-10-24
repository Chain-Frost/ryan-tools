import os
import processing
from qgis.core import QgsProject

# Define the root directory to search for .lyr files
root_dir = r"C:\Temp"

# Recursively search for .lyr files and convert them to .qml files
print(f"Starting to process .lyr files in: {root_dir}")

for subdir, _, files in os.walk(root_dir):
    print(f"Checking directory: {subdir}")

    for file in files:
        if file.endswith(".lyr"):
            lyr_file = os.path.join(subdir, file)
            qml_file = os.path.splitext(lyr_file)[0] + ".qml"
            print(
                f"Processing {lyr_file}..."
            )  # Notify when a .lyr file is being processed

            try:
                # Run the slyr:lyrtoqml tool and load the result
                result = processing.run(
                    "slyr:lyrtoqml", {"INPUT": lyr_file, "OUTPUT": qml_file}
                )

                print(f"Successfully converted {lyr_file} to {qml_file}")

                # Optional: Load the output .qml file into the QGIS project
                # QgsProject.instance().addMapLayer(result['OUTPUT'])

            except Exception as e:
                print(f"Failed to convert {lyr_file}: {e}")

print("Processing complete.")
