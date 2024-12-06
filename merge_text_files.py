import os
from pathlib import Path

folder_path = r"Q:\BGER\PER\RPRT\ryan-tools\ryan_library\processors\tuflow"


def merge_text_files(
    folder_path: str,
    include_file_name: bool = False,
    output_file_name: str = "output.txt",
) -> None:
    """
    Merges all text files in a folder into one output file.

    Args:
        folder_path (str): Path to the folder containing text files.
        include_file_name (bool): Whether to include file names in the output.
        output_file_name (str): Name of the output file.
    """
    folder = Path(folder_path)
    output_file_path = folder / output_file_name

    # Ensure the folder exists
    if not folder.is_dir():
        print(f"Error: {folder_path} is not a valid directory.")
        return

    with open(output_file_path, "w", encoding="utf-8") as output_file:
        for file in folder.glob("*.py"):
            print(file)
            # Skip the output file if it exists in the same directory
            if file.name == output_file_name:
                continue

            if include_file_name:
                output_file.write(f"File: {file.name}\n")

            # Write file content
            with open(file, "r", encoding="utf-8") as f:
                output_file.write(f.read())

            # Add separator
            output_file.write("\n-------------\n")

    print(f"Merged text files into {output_file_path}")


merge_text_files(folder_path, include_file_name=False)
