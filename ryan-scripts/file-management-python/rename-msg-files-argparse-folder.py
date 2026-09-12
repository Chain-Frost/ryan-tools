"""Rename Outlook MSG files from their sent date, sender, and subject.

Run ``python rename-msg-files-argparse-folder.py <folder>`` or omit ``<folder>``
to enter it interactively. Only MSG files directly inside that folder are
processed. Duplicate content and filename collisions are detected, and a
summary table is printed after processing.

This changes filenames in place and has no dry-run mode. Work on a backed-up
folder first and review the configured filename length limits.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false

import hashlib
import re
import sys
from _hashlib import HASH
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import extract_msg  # pyright: ignore[reportMissingImports]
from loguru import logger
from tabulate import tabulate

# Constants for filename length limits
MAX_SENDER_LENGTH = 50
MAX_SUBJECT_LENGTH = 100
MAX_FILENAME_LENGTH = 255  # Typical Windows max path length


def sanitize_filename(s: str) -> str:
    """Remove or replace characters that are invalid in filenames."""
    # Replace invalid characters with underscores
    sanitized: str = re.sub(pattern=r'[\\/*?:"<>|]', repl="_", string=s)
    return sanitized


def compute_file_hash(file_path: Path, hash_algo: str = "sha256") -> str | None:
    """Compute the hash of a file's contents.

    Args:
        file_path: Path to the file.
        hash_algo: Hash algorithm to use (default: 'sha256').

    Returns:
        Hexadecimal hash string if successful, None otherwise.
    """
    try:
        hash_func: HASH = hashlib.new(hash_algo)
        with file_path.open(mode="rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except (OSError, ValueError) as error:
        logger.error("Error computing hash for {}: {}", file_path, error)
        return None


@contextmanager
def open_msg(file_path: Path) -> Generator[Any]:
    """Context manager to open and close a .msg file.

    Args:
        file_path: Path to the .msg file.

    Yields:
        An extract_msg.Message object.
    """
    msg = extract_msg.Message(str(file_path))
    try:
        yield msg
    finally:
        msg.close()


def get_email_properties(file_path: Path) -> tuple[str, str, str]:
    """Extract SentOn, Sender, and Subject from a .msg file.

    Args:
        file_path: Path to the .msg file.

    Returns:
        Tuple containing formatted local date (YYYY-MM-DD_HH-MM-SS),
        sanitized sender, and sanitized subject.
    """
    try:
        with open_msg(file_path) as msg:
            msg_sender = cast("str | None", msg.sender) or "UnknownSender"
            msg_date = cast("datetime | str | None", msg.date) or "1970-01-01 00:00:00"
            msg_subject = cast("str | None", msg.subject) or "NoSubject"

            # Determine if msg_date is a string or datetime object
            if isinstance(msg_date, str):
                try:
                    # Attempt to parse the string to datetime
                    # Common format: "2024-12-20 10:15:00"
                    parsed_date = datetime.strptime(msg_date[:19], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    logger.warning("Unrecognized date format in {}: '{}'", file_path, msg_date)
                    iso_date = "UnknownDate"
                else:
                    iso_date = parsed_date.strftime("%Y-%m-%d_%H-%M-%S")
            elif isinstance(msg_date, datetime):  # pyright: ignore[reportUnnecessaryIsInstance]
                # Convert to local timezone if timezone aware
                if msg_date.tzinfo is not None:
                    local_tz = datetime.now().astimezone().tzinfo
                    msg_date = msg_date.astimezone(local_tz)
                iso_date = msg_date.strftime("%Y-%m-%d_%H-%M-%S")
                logger.debug("msg.date is a datetime object: {}", iso_date)
            else:
                logger.warning("Unsupported date type in {}: {}", file_path, type(msg_date))
                iso_date = "UnknownDate"

            # Sanitize sender and subject to make them filename-safe
            sanitized_sender = sanitize_filename(msg_sender)[:MAX_SENDER_LENGTH]
            sanitized_subject = sanitize_filename(msg_subject)[:MAX_SUBJECT_LENGTH]

            return iso_date, sanitized_sender, sanitized_subject
    except Exception as error:
        logger.error("Error extracting properties from {}: {}", file_path, error)
        return "UnknownDate", "UnknownSender", "NoSubject"


def limit_filename_length(new_filename: str) -> str:
    """Ensure the new filename does not exceed the maximum allowed length.

    Args:
        new_filename: The proposed new filename.

    Returns:
        A filename trimmed to the maximum allowed length.
    """
    if len(new_filename) <= MAX_FILENAME_LENGTH:
        return new_filename
    # Calculate how much to trim
    excess_length = len(new_filename) - MAX_FILENAME_LENGTH
    # Trim the subject part to reduce the length
    parts = new_filename.split("_", 2)  # Split into date, sender, subject
    if len(parts) < 3:
        return new_filename[:MAX_FILENAME_LENGTH]  # Fallback: trim entire string

    date_part, sender_part, subject_part = parts
    # Further split the subject to separate '.msg'
    if subject_part.lower().endswith(".msg"):
        subject_part = subject_part[:-4]  # Remove '.msg'

    # Trim the subject
    subject_trimmed = subject_part[:-excess_length] if excess_length < len(subject_part) else "TrimmedSubject"

    # Reconstruct the filename
    new_filename = f"{date_part}_{sender_part}_{subject_trimmed}.msg"
    return new_filename[:MAX_FILENAME_LENGTH]


def rename_msg_files(directory: str | Path) -> None:
    """Rename all .msg files in the specified directory based on email properties.

    Args:
        directory: Path to the directory containing .msg files.
    """
    target_directory = Path(directory)
    if not target_directory.is_dir():
        logger.error("The specified path does not exist: {}", target_directory)
        sys.exit(1)

    # Dictionary to track hashes and detect identical files
    hash_dict: dict[str, str] = {}

    # Dictionary to track filename counts for handling duplicates
    filename_counts: dict[str, int] = {}

    # List to hold summary data
    summary: list[dict[str, str]] = []

    # Iterate over all .msg files in the directory
    for original_path in target_directory.iterdir():
        if original_path.is_file() and original_path.suffix.lower() == ".msg":
            filename = original_path.name

            # Compute file hash
            file_hash = compute_file_hash(file_path=original_path)
            if file_hash is None:
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": "",
                        "Status": "Hash Error",
                    }
                )
                continue  # Skip files that couldn't be hashed

            # Check for identical files
            if file_hash in hash_dict:
                logger.warning("'{}' is identical to '{}'. Skipping renaming.", filename, hash_dict[file_hash])
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": "",
                        "Status": f"Duplicate of {hash_dict[file_hash]}",
                    }
                )
                continue  # Skip renaming identical files
            hash_dict[file_hash] = filename  # Add hash to dictionary

            # Extract email properties
            sent_on, sender, subject = get_email_properties(original_path)
            if sent_on == "UnknownDate" and sender == "UnknownSender" and subject == "NoSubject":
                logger.warning("Skipping file due to missing properties: {}", filename)
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": "",
                        "Status": "Missing Properties",
                    }
                )
                continue  # Skip files with missing properties

            # Construct the new filename
            base_new_name: str = f"{sent_on}_{sender}_{subject}"
            new_name: str = f"{base_new_name}.msg"
            new_path = target_directory / new_name

            # Handle duplicate filenames by appending a numerical suffix
            if new_name in filename_counts:
                filename_counts[new_name] += 1
                new_name = f"{base_new_name}({filename_counts[new_name]}).msg"
                new_path = target_directory / new_name
            else:
                filename_counts[new_name] = 1

            # If the new filename already exists, append a number to make it unique
            counter = 1
            while new_path.exists():
                new_name = f"{base_new_name}({counter}).msg"
                new_path = target_directory / new_name
                counter += 1

            # Limit the filename length to prevent exceeding Windows' max path length
            new_name = limit_filename_length(new_name)
            new_path = target_directory / new_name

            # Perform the renaming
            try:
                original_path.rename(new_path)
                logger.success("Renamed: '{}' --> '{}'", filename, new_name)
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": new_name,
                        "Status": "Renamed",
                    }
                )
            except OSError as error:
                logger.error("Error renaming '{}' to '{}': {}", filename, new_name, error)
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": new_name,
                        "Status": f"Renaming Error: {error}",
                    }
                )

    # Display summary table
    if summary:
        print("\nSummary of Renaming Process:")
        print(
            tabulate(
                tabular_data=summary,
                headers="keys",
                tablefmt="fancy_grid",
                stralign="left",
            )
        )


def main() -> None:
    """Main function to execute the script."""
    if len(sys.argv) > 1:
        directory: str = sys.argv[1]
    else:
        # Prompt the user to enter the directory path
        directory = input("Enter the path to the directory containing .msg files: ").strip()

    rename_msg_files(directory=directory)


if __name__ == "__main__":
    # Configure loguru to display colored messages
    logger.remove()  # Remove the default logger
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
        colorize=True,
        level="DEBUG",  # Set to DEBUG to capture all levels of logs
    )

    main()
