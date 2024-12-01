# ryan_functions.file_utils.py
from concurrent.futures._base import Future
from glob import iglob
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging


def get_all_files(directory: str, file_extension: str = "tif") -> list[str]:
    """
    Gather a list of all files with a specific extension within a directory, recursively.

    Args:
        directory (str): The directory to search in.
        file_extension (str): The file extension to filter by.

    Returns:
        List[str]: A list of file paths.
    """
    search_pattern: str = f"{directory}/**/*.{file_extension}"
    return [
        f
        for f in iglob(pathname=search_pattern, recursive=True)
        if os.path.isfile(path=f)
    ]


def find_files_parallel(
    root_dir: str, pattern: str = ".tlf", exclude: tuple = (".hpc.tlf", ".gpu.tlf")
) -> list[str]:
    """
    Find files in parallel within a directory.

    Args:
        root_dir (str): The root directory to search in.
        pattern (str): The file extension pattern to look for.
        exclude (tuple): A tuple of file extensions to exclude.

    Returns:
        list[str]: A list of file paths matching the pattern and not excluded.
    """

    def search_dir(path: str) -> list[str]:
        matched_files: list[str] = []
        for root, _, files in os.walk(path):
            for file in files:
                if file.endswith(pattern) and not file.endswith(exclude):
                    matched_files.append(os.path.join(root, file))
        return matched_files

    with ThreadPoolExecutor() as executor:
        futures: list[Future[list[str]]] = [
            executor.submit(search_dir, os.path.join(root_dir, d))
            for d in os.listdir(root_dir)
            if os.path.isdir(os.path.join(root_dir, d))
        ]
        results: list[list[str]] = [f.result() for f in as_completed(futures)]

    # Flatten the list of lists
    return [item for sublist in results for item in sublist]


def checkEmptyFile(file):
    # Check if file is empty by checking its size
    if os.path.getsize(file) == 0:
        print(f"Skipping empty file: {file}")
        return True
    else:
        return False


def is_non_zero_file(fpath: str) -> bool:
    """
    Check if a file exists and is not zero size, logging specific errors if conditions are not met.

    Args:
        fpath (str): Path to the file.

    Returns:
        bool: True if the file exists and is not empty, False otherwise.
    """
    logger = logging.getLogger(__name__)

    try:
        # Check if the path exists
        if not os.path.exists(fpath):
            logger.error(f"File does not exist: {fpath}")
            return False

        # Check if the path is a file
        if not os.path.isfile(fpath):
            logger.error(f"Path is not a file: {fpath}")
            return False

        # Check if the file is not empty
        size = os.path.getsize(fpath)
        if size == 0:
            logger.error(f"File is empty: {fpath}")
            return False

        # File exists and is not empty
        return True

    except PermissionError:
        logger.error(f"Permission denied when accessing file: {fpath}")
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while accessing file '{fpath}': {e}"
        )
        return False


def ensure_output_directory(output_dir):
    """
    Ensures that the output directory exists; creates it if it does not.

    Parameters:
    - output_dir: str, path to the output directory.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory: {output_dir}")
    else:
        logging.info(f"Output directory already exists: {output_dir}")
