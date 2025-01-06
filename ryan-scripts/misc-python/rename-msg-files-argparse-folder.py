import os
import sys
import extract_msg
import hashlib
import re
from datetime import datetime
from loguru import logger
from tabulate import tabulate
from contextlib import contextmanager

# Constants for filename length limits
MAX_SENDER_LENGTH = 50
MAX_SUBJECT_LENGTH = 100
MAX_FILENAME_LENGTH = 255  # Typical Windows max path length


def sanitize_filename(s: str) -> str:
    """Remove or replace characters that are invalid in filenames."""
    # Replace invalid characters with underscores
    sanitized: str = re.sub(r'[\\/*?:"<>|]', "_", s)
    return sanitized


def compute_file_hash(file_path: str, hash_algo: str = "sha256") -> str | None:
    """Compute the hash of a file's contents.

    Args:
        file_path: Path to the file.
        hash_algo: Hash algorithm to use (default: 'sha256').

    Returns:
        Hexadecimal hash string if successful, None otherwise.
    """
    try:
        hash_func = hashlib.new(hash_algo)
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        logger.error(f"Error computing hash for {file_path}: {e}")
        return None


@contextmanager
def open_msg(file_path: str):
    """Context manager to open and close a .msg file.

    Args:
        file_path: Path to the .msg file.

    Yields:
        An extract_msg.Message object.
    """
    msg = extract_msg.Message(file_path)
    try:
        yield msg
    finally:
        msg.close()


def get_email_properties(file_path: str) -> tuple[str, str, str]:
    """Extract SentOn, Sender, and Subject from a .msg file.

    Args:
        file_path: Path to the .msg file.

    Returns:
        Tuple containing formatted local date (YYYY-MM-DD_HH-MM-SS),
        sanitized sender, and sanitized subject.
    """
    try:
        with open_msg(file_path) as msg:
            msg_sender: str = msg.sender if msg.sender else "UnknownSender"
            msg_date = msg.date if msg.date else "1970-01-01 00:00:00"
            msg_subject: str = msg.subject if msg.subject else "NoSubject"

            # Determine if msg_date is a string or datetime object
            if isinstance(msg_date, str):
                try:
                    # Attempt to parse the string to datetime
                    # Common format: "2024-12-20 10:15:00"
                    parsed_date = datetime.strptime(msg_date[:19], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    logger.warning(
                        f"Unrecognized date format in {file_path}: '{msg_date}'"
                    )
                    iso_date = "UnknownDate"
                else:
                    iso_date = parsed_date.strftime("%Y-%m-%d_%H-%M-%S")
            elif isinstance(msg_date, datetime):
                # Convert to local timezone if timezone aware
                if msg_date.tzinfo is not None:
                    local_tz = datetime.now().astimezone().tzinfo
                    msg_date = msg_date.astimezone(local_tz)
                iso_date = msg_date.strftime("%Y-%m-%d_%H-%M-%S")
                logger.debug(f"msg.date is a datetime object: {iso_date}")
            else:
                logger.warning(
                    f"Unsupported date type in {file_path}: {type(msg_date)}"
                )
                iso_date = "UnknownDate"

            # Sanitize sender and subject to make them filename-safe
            sanitized_sender = sanitize_filename(msg_sender)[:MAX_SENDER_LENGTH]
            sanitized_subject = sanitize_filename(msg_subject)[:MAX_SUBJECT_LENGTH]

            return iso_date, sanitized_sender, sanitized_subject
    except Exception as e:
        logger.error(f"Error extracting properties from {file_path}: {e}")
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
    else:
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
        subject_trimmed = (
            subject_part[:-excess_length]
            if excess_length < len(subject_part)
            else "TrimmedSubject"
        )

        # Reconstruct the filename
        new_filename = f"{date_part}_{sender_part}_{subject_trimmed}.msg"
        return new_filename[:MAX_FILENAME_LENGTH]


def rename_msg_files(directory: str) -> None:
    """Rename all .msg files in the specified directory based on email properties.

    Args:
        directory: Path to the directory containing .msg files.
    """
    if not os.path.isdir(directory):
        logger.error(f"The specified path does not exist: {directory}")
        sys.exit(1)

    # Dictionary to track hashes and detect identical files
    hash_dict: dict[str, str] = {}

    # Dictionary to track filename counts for handling duplicates
    filename_counts: dict[str, int] = {}

    # List to hold summary data
    summary: list[dict[str, str]] = []

    # Iterate over all .msg files in the directory
    for filename in os.listdir(directory):
        if filename.lower().endswith(".msg"):
            original_path = os.path.join(directory, filename)

            # Compute file hash
            file_hash = compute_file_hash(original_path)
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
                logger.warning(
                    f"'{filename}' is identical to '{hash_dict[file_hash]}'. Skipping renaming."
                )
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": "",
                        "Status": f"Duplicate of {hash_dict[file_hash]}",
                    }
                )
                continue  # Skip renaming identical files
            else:
                hash_dict[file_hash] = filename  # Add hash to dictionary

            # Extract email properties
            sent_on, sender, subject = get_email_properties(original_path)
            if (
                sent_on == "UnknownDate"
                and sender == "UnknownSender"
                and subject == "NoSubject"
            ):
                logger.warning(f"Skipping file due to missing properties: {filename}")
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": "",
                        "Status": "Missing Properties",
                    }
                )
                continue  # Skip files with missing properties

            # Construct the new filename
            base_new_name = f"{sent_on}_{sender}_{subject}"
            new_name = f"{base_new_name}.msg"
            new_path = os.path.join(directory, new_name)

            # Handle duplicate filenames by appending a numerical suffix
            if new_name in filename_counts:
                filename_counts[new_name] += 1
                new_name = f"{base_new_name}({filename_counts[new_name]}).msg"
                new_path = os.path.join(directory, new_name)
            else:
                filename_counts[new_name] = 1

            # If the new filename already exists, append a number to make it unique
            counter = 1
            while os.path.exists(new_path):
                new_name = f"{base_new_name}({counter}).msg"
                new_path = os.path.join(directory, new_name)
                counter += 1

            # Limit the filename length to prevent exceeding Windows' max path length
            new_name = limit_filename_length(new_name)
            new_path = os.path.join(directory, new_name)

            # Perform the renaming
            try:
                os.rename(original_path, new_path)
                logger.success(f"Renamed: '{filename}' --> '{new_name}'")
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": new_name,
                        "Status": "Renamed",
                    }
                )
            except Exception as e:
                logger.error(f"Error renaming '{filename}' to '{new_name}': {e}")
                summary.append(
                    {
                        "Original Filename": filename,
                        "New Filename": new_name,
                        "Status": f"Renaming Error: {e}",
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
        directory = input(
            prompt="Enter the path to the directory containing .msg files: "
        ).strip()

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
