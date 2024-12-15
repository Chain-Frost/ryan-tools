# ryan_library\functions\file_utils.py
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import fnmatch
from loguru import logger


def find_files_parallel(
    root_dirs: list[Path],
    patterns: str | list[str],
    excludes: str | list[str] | None = None,
    report_level: int | None = 2,
    print_found_folder: bool = True,
    recursive_search: bool = True,
) -> list[Path]:
    """
    Search for files matching specific patterns across multiple directories in parallel.

    This function traverses through the provided root directories, searching for files
    that match the given patterns while excluding any files that match the exclusion patterns.
    The search can be performed recursively or non-recursively based on the `recursive_search` flag.
    The search is performed in parallel using multiple threads to improve performance.

    Args:
        root_dirs (list[Path]): The root directories where the search will begin.
        patterns (Union[str, list[str]]): Glob pattern(s) to include in the search.
            Can be a single pattern string or a list of patterns. Patterns can match
            any part of the filename, e.g., "*.hpc.dt.csv", "M01_*", etc.
        excludes (Union[str, list[str], None], optional): Glob pattern(s) to exclude
            from the search. Can be a single pattern string, a list of patterns, or None.
            Patterns can match any part of the filename, e.g., "*.hpc.tlf", "temp_*", etc.
            Defaults to None.
        report_level (Optional[int], optional): Determines the frequency of logging
            folder search progress based on directory depth. If set to an integer,
            it logs at every 'report_level' depth. If None, depth-based reporting
            is disabled. Defaults to 2.
        print_found_folder (bool, optional): If True, logs the folders that contain
            matched files. Defaults to True.
        recursive_search (bool, optional): If True, searches directories recursively.
            If False, only searches the top-level directories without traversing subdirectories.
            Defaults to True.

    Returns:
        list[Path]: A list of file paths that match the specified patterns and do not
            match any of the exclusion patterns.
    """

    # Validate inputs
    if (
        not root_dirs
        or not isinstance(root_dirs, list)
        or all(not isinstance(d, Path) for d in root_dirs)
    ):
        logger.error("No root directories provided or invalid root_dirs argument.")
        raise ValueError("You must provide at least one valid root directory.")

    if not patterns or (isinstance(patterns, list) and not patterns):
        logger.error("No patterns provided for searching.")
        raise ValueError("You must provide at least one search pattern.")

    # Normalize 'patterns' and 'excludes' to lists for consistent processing
    if isinstance(patterns, str):
        patterns = [patterns]
    if excludes is None:
        excludes = []
    elif isinstance(excludes, str):
        excludes = [excludes]

    # Convert all patterns to lowercase for case-insensitive matching
    patterns = [p.lower() for p in patterns]
    excludes = [e.lower() for e in excludes]
    logger.info(f"Search patterns:  {patterns}")
    if excludes:
        logger.info(f"Exclude patterns: {excludes}")
    # Obtain the current working directory to calculate relative paths later
    current_dir = Path.cwd()

    def search_dir(path: Path, root_dir: Path) -> tuple[list[Path], int, int]:
        """
        Search for files matching the patterns within a single directory.

        This helper function is executed in parallel across multiple directories.
        It traverses the directory tree (recursively or non-recursively based on `recursive_search`),
        matches files against the inclusion and exclusion patterns, and logs progress
        based on the report_level.

        Args:
            path (Path): The directory path to search.
            root_dir (Path): The root directory to calculate relative depth.

        Returns:
            tuple[list[Path], int, int]: A tuple containing:
                - List of matched file paths.
                - Number of files searched.
                - Number of folders searched.
        """
        matched_files: list[Path] = []  # Stores paths of files that match the patterns
        files_searched = 0  # Counter for the number of files processed
        folders_searched = 0  # Counter for the number of folders processed
        folders_with_matches = set()  # Tracks folders containing matched files

        # Choose the appropriate globbing method based on `recursive_search`
        if recursive_search:
            iterator = path.rglob("*")
        else:
            iterator = path.glob("*")

        # Traverse the directory (recursively or not)
        for subpath in iterator:
            if subpath.is_dir():
                # Only perform depth-related logging if recursive_search is True
                if recursive_search and report_level is not None:
                    # Calculate the directory depth relative to the root_dir
                    try:
                        relative_path = subpath.relative_to(root_dir)
                        depth = len(relative_path.parts)
                    except ValueError:
                        # If subpath is not relative to root_dir, default depth to 0
                        depth = 0

                    # Log the folder being searched based on the report_level
                    if depth % report_level == 0:
                        try:
                            display_path = subpath.relative_to(current_dir)
                        except ValueError:
                            # Use absolute path if relative path cannot be determined
                            display_path = subpath.resolve()
                        logger.info(f"Searching ({depth}): {display_path}")

                folders_searched += 1  # Increment folder counter
                continue  # Skip directories for file matching

            files_searched += 1  # Increment file counter

            # Get the filename in lowercase for case-insensitive matching
            filename = subpath.name.lower()

            # Inclusion Check: Check if the filename matches any inclusion pattern
            if any(fnmatch.fnmatch(filename, pattern) for pattern in patterns):
                # Exclusion Check: Ensure the filename does not match any exclusion pattern
                if not any(fnmatch.fnmatch(filename, exclude) for exclude in excludes):
                    logger.debug(f"Matched file:  {subpath.relative_to(current_dir)}")
                    matched_files.append(subpath)  # Add to matched files list
                    folders_with_matches.add(subpath.parent)  # Track parent folder

        # Optionally log folders that contain matched files
        if print_found_folder:
            for folder in folders_with_matches:
                try:
                    display_path = folder.relative_to(current_dir)
                except ValueError:
                    display_path = folder.resolve()
                logger.info(f"Folder with matched files: {display_path}")

        logger.info(f"Found {len(matched_files)} files in {path}")
        return matched_files, files_searched, folders_searched

    logger.info(f"Starting search in {len(root_dirs)} root directories.")

    total_files_searched = 0  # Total number of files processed across all directories
    total_folders_searched = (
        0  # Total number of folders processed across all directories
    )

    # Utilize a thread pool to perform directory searches in parallel
    with ThreadPoolExecutor() as executor:
        # Submit a search task for each root directory
        futures = [
            executor.submit(search_dir, root_dir, root_dir) for root_dir in root_dirs
        ]
        results = []  # To store the results from each thread

        # Process the results as they complete
        for future in as_completed(futures):
            try:
                matched, files, folders = future.result()
                results.append(matched)
                total_files_searched += files
                total_folders_searched += folders
            except Exception as e:
                # Log any errors that occur during the search
                logger.error(f"Error occurred during file search: {e}")

    # Combine all matched files from different directories into a single list
    all_files = [file for sublist in results for file in sublist]
    logger.info(f"Total files matched: {len(all_files)}")
    logger.info(f"Total files searched: {total_files_searched}")
    logger.info(f"Total folders searched: {total_folders_searched}")

    return all_files


def is_non_zero_file(fpath: Path) -> bool:
    """
    Verify that a given file exists, is indeed a file, and is not empty.

    This function performs a series of checks on the provided file path to ensure
    that the file is present, accessible, and contains data. It logs specific
    error messages if any of these conditions are not met.

    Args:
        fpath (Path): The path to the file to be checked.

    Returns:
        bool: True if the file exists, is a regular file, and is non-empty.
              False otherwise.
    """

    try:
        # Check if the file exists
        if not fpath.exists():
            logger.error(f"File does not exist: {fpath}")
            return False

        # Check if the path points to a file (not a directory or other type)
        if not fpath.is_file():
            logger.error(f"Path is not a file: {fpath}")
            return False

        # Check if the file size is greater than zero
        if fpath.stat().st_size == 0:
            logger.error(f"File is empty: {fpath}")
            return False

        return True  # All checks passed

    except PermissionError:
        # Handle cases where the file cannot be accessed due to permission issues
        logger.error(f"Permission denied when accessing file: {fpath}")
        return False
    except Exception as e:
        # Catch-all for any other unexpected errors
        logger.error(
            f"An unexpected error occurred while accessing file '{fpath}': {e}"
        )
        return False


def ensure_output_directory(output_dir: Path) -> None:
    """
    Ensure that the specified output directory exists; create it if it does not.

    This function checks whether the given output directory exists. If it does not,
    it creates the directory along with any necessary parent directories. It logs
    the action taken, whether the directory was created or already existed.

    Args:
        output_dir (Path): The path to the output directory to be ensured.
    """

    if not output_dir.exists():
        # Create the directory and any necessary parent directories
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
    else:
        logger.info(f"Output directory already exists: {output_dir}")
