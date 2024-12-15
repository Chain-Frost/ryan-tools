# ryan-scripts\fix-chatgpt-type-hints.py
from pathlib import Path
import re


def fix_type_hints(file_path: Path) -> None:
    """Fix type hints in the Python file and log changes."""
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    modified = False
    found_bad_imports = False

    with open(file_path, "w", encoding="utf-8") as file:
        for line_number, line in enumerate(lines, start=1):
            # Check for bad imports
            if "from typing import" in line:
                bad_types = ["List", "Dict", "Tuple", "Set"]
                if any(bad_type in line for bad_type in bad_types):
                    found_bad_imports = True
                    print(
                        f"Bad import found: {file_path} (Line {line_number}): {line.strip()}"
                    )

            original_line = line
            # Replace incorrect type hints using regex
            line = re.sub(r"\bDict\[", "dict[", line)
            line = re.sub(r"\bList\[", "list[", line)
            line = re.sub(r"\bTuple\[", "tuple[", line)
            line = re.sub(r"\bSet\[", "set[", line)

            if line != original_line:
                modified = True
                print(f"Modified: {file_path} (Line {line_number})")

            file.write(line)

    if not modified and not found_bad_imports:
        print(f"    No changes: {file_path}")


def process_folders(folder_paths: list[Path]) -> None:
    """Process all Python files in multiple folders."""
    script_path = Path(__file__).resolve()  # Dynamically get the script's path
    for folder_path in folder_paths:
        folder = Path(folder_path).resolve()  # Resolve to absolute path
        if not folder.exists():
            print(f"Folder not found: {folder}")
            continue

        print(f"Processing folder: {folder}")
        for file in folder.rglob("*.py"):  # Recursively find all .py files
            if file.resolve() == script_path:  # Skip the script itself
                print(f"Skipping: {file}")
                continue
            fix_type_hints(file)


if __name__ == "__main__":
    # List of relative folder paths
    folder_paths: list[Path] = [
        # Path(r".\ryan_library"),
        # Path(r".\ryan-scripts"),
        Path(r"..\ryan_library"),
        Path(r"..\ryan-scripts"),
    ]

    process_folders(folder_paths)
