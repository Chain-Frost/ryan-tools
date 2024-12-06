# ryan_library\functions\file_utils.py
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging


def find_files_parallel(
    root_dirs: list[Path],
    patterns: str | list[str],
    excludes: str | list[str] | None = None,
) -> list[Path]:
    """
    Find files in parallel within multiple directories.

    Args:
        root_dirs (List[Path]): The root directories to search in.
        patterns (str | List[str]): A file extension pattern or a list of patterns to look for.
        excludes (str | List[str] | None): A file extension pattern or a list of patterns to exclude. Can be None.

    Returns:
        List[Path]: A list of file paths matching the patterns and not excluded.
    """
    logger = logging.getLogger(__name__)

    # Ensure patterns and excludes are lists
    if isinstance(patterns, str):
        patterns = [patterns]
    if excludes is None:
        excludes = []
    elif isinstance(excludes, str):
        excludes = [excludes]

    patterns = [p.lower() for p in patterns]
    excludes = [e.lower() for e in excludes]

    def search_dir(path: Path, root_dir: Path) -> tuple[list[Path], int, int]:
        """
        Search for files matching the patterns in a single directory.

        Args:
            path (Path): The directory path to search.
            root_dir (Path): The root directory to calculate relative depth.

        Returns:
            Tuple containing:
                - List of matched file paths.
                - Number of files searched.
                - Number of folders searched.
        """
        matched_files: list[Path] = []
        files_searched = 0
        folders_searched = 0

        for subpath in path.rglob("*"):
            if subpath.is_dir():
                # Calculate depth relative to root_dir
                try:
                    relative_path = subpath.relative_to(root_dir)
                    depth = len(relative_path.parts)
                except ValueError:
                    # If subpath is not relative to root_dir, skip depth calculation
                    depth = 0

                if depth % 2 == 0:
                    print(f"Searching in folder (level {depth}): {subpath}")

                folders_searched += 1
                continue  # Skip directories for file matching

            files_searched += 1
            if any(subpath.suffix.lower() == pattern for pattern in patterns):
                if not any(subpath.suffix.lower() == exclude for exclude in excludes):
                    logger.debug(f"Matched file: {subpath}")
                    matched_files.append(subpath)

        logger.info(f"Found {len(matched_files)} files in {path}")
        return matched_files, files_searched, folders_searched

    logger.info(f"Starting search in {len(root_dirs)} root directories.")

    total_files_searched = 0
    total_folders_searched = 0

    with ThreadPoolExecutor() as executor:
        # Pass both path and root_dir to search_dir
        futures = [
            executor.submit(search_dir, root_dir, root_dir) for root_dir in root_dirs
        ]
        results = []
        for future in as_completed(futures):
            try:
                matched, files, folders = future.result()
                results.append(matched)
                total_files_searched += files
                total_folders_searched += folders
            except Exception as e:
                logger.error(f"Error occurred during file search: {e}")

    # Flatten the list of lists
    all_files = [file for sublist in results for file in sublist]
    logger.info(f"Total files matched: {len(all_files)}")
    logger.info(f"Total files searched: {total_files_searched}")
    logger.info(f"Total folders searched: {total_folders_searched}")

    return all_files


def is_non_zero_file(fpath: Path) -> bool:
    """
    Check if a file exists and is not zero size, logging specific errors if conditions are not met.

    Args:
        fpath (Path): Path to the file.

    Returns:
        bool: True if the file exists and is not empty, False otherwise.
    """
    logger = logging.getLogger(__name__)

    try:
        if not fpath.exists():
            logger.error(f"File does not exist: {fpath}")
            return False

        if not fpath.is_file():
            logger.error(f"Path is not a file: {fpath}")
            return False

        if fpath.stat().st_size == 0:
            logger.error(f"File is empty: {fpath}")
            return False

        return True

    except PermissionError:
        logger.error(f"Permission denied when accessing file: {fpath}")
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while accessing file '{fpath}': {e}"
        )
        return False


def ensure_output_directory(output_dir: Path) -> None:
    """
    Ensures that the output directory exists; creates it if it does not.

    Args:
        output_dir (Path): Path to the output directory.
    """
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created output directory: {output_dir}")
    else:
        logging.info(f"Output directory already exists: {output_dir}")
