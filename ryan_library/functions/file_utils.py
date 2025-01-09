# ryan_library/functions/file_utils.py

import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, Optional
import fnmatch
from loguru import logger


def find_files_parallel(
    root_dirs: Optional[list[Path]] = None,
    patterns: Optional[Union[str, list[str]]] = None,
    excludes: Optional[Union[str, list[str]]] = None,
    report_level: Optional[int] = 2,
    print_found_folder: bool = True,
    recursive_search: bool = True,
    # Deprecated parameters for backward compatibility
    root_dir: Optional[Union[str, Path, list[Union[str, Path]]]] = None,
    pattern: Optional[Union[str, list[str]]] = None,
    exclude: Optional[Union[str, list[str]]] = None,
) -> list[Path]:
    """
    Search for files matching specific patterns across multiple directories in parallel.

    This function traverses through the provided root directories, searching for files
    that match the given patterns while excluding any files that match the exclusion patterns.
    The search can be performed recursively or non-recursively based on the `recursive_search` flag.
    The search is performed in parallel using multiple threads to improve performance.

    Args:
        root_dirs (Optional[list[Path]]): The root directories where the search will begin.
            If not provided, `root_dir` can be used for backward compatibility.
        patterns (Optional[Union[str, list[str]]]): Glob pattern(s) to include in the search.
            Can be a single pattern string or a list of patterns.
        excludes (Optional[Union[str, list[str]]]): Glob pattern(s) to exclude from the search.
            Can be a single pattern string, a list of patterns, or None.
        report_level (Optional[int], optional): Determines the frequency of logging
            folder search progress based on directory depth. Defaults to 2.
        print_found_folder (bool, optional): If True, logs the folders that contain
            matched files. Defaults to True.
        recursive_search (bool, optional): If True, searches directories recursively.
            Defaults to True.
        root_dir (Optional[Union[str, Path, list[Union[str, Path]]]]): **Deprecated.**
            Use `root_dirs` instead.
        pattern (Optional[Union[str, list[str]]]): **Deprecated.** Use `patterns` instead.
        exclude (Optional[Union[str, list[str]]]): **Deprecated.** Use `excludes` instead.

    Returns:
        list[Path]: A list of file paths that match the specified patterns and do not
            match any of the exclusion patterns.
    """
    # Handle deprecated parameters with warnings
    if root_dir is not None:
        warnings.warn(
            "The 'root_dir' parameter is deprecated and will be removed in a future version. "
            "Use 'root_dirs' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if root_dirs is None:
            if isinstance(root_dir, (str, Path)):
                root_dirs = [Path(root_dir)]
            elif isinstance(root_dir, list):
                root_dirs = [Path(r) for r in root_dir]
            else:
                raise TypeError(
                    "The 'root_dir' must be a string, Path, or list of these."
                )
        else:
            logger.warning(
                "Both 'root_dir' and 'root_dirs' were provided. "
                "'root_dirs' will take precedence."
            )

    if pattern is not None:
        warnings.warn(
            "The 'pattern' parameter is deprecated and will be removed in a future version. "
            "Use 'patterns' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if patterns is None:
            patterns = pattern
        else:
            logger.warning(
                "Both 'pattern' and 'patterns' were provided. "
                "'patterns' will take precedence."
            )

    if exclude is not None:
        warnings.warn(
            "The 'exclude' parameter is deprecated and will be removed in a future version. "
            "Use 'excludes' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if excludes is None:
            excludes = exclude
        else:
            logger.warning(
                "Both 'exclude' and 'excludes' were provided. "
                "'excludes' will take precedence."
            )

    # Validate required parameters
    if not root_dirs:
        raise TypeError(
            "find_files_parallel() missing required argument: 'root_dirs' or 'root_dir'"
        )
    if not patterns:
        raise TypeError(
            "find_files_parallel() missing required argument: 'patterns' or 'pattern'"
        )

    # Normalize 'patterns' and 'excludes' to lists for consistent processing
    patterns = [patterns] if isinstance(patterns, str) else patterns
    excludes = [excludes] if isinstance(excludes, str) else excludes or []

    # Convert all patterns to lowercase for case-insensitive matching
    patterns = [p.lower() for p in patterns]
    excludes = [e.lower() for e in excludes]

    logger.info(f"Search patterns: {patterns}")
    if excludes:
        logger.info(f"Exclude patterns: {excludes}")

    # Obtain the current working directory to calculate relative paths later
    current_dir = Path.cwd()

    def search_dir(path: Path, root_dir: Path) -> tuple[list[Path], int, int]:
        """
        Search for files matching the patterns within a single directory.

        Args:
            path (Path): The directory path to search.
            root_dir (Path): The root directory to calculate relative depth.

        Returns:
            tuple[list[Path], int, int]: A tuple containing:
                - List of matched file paths.
                - Number of files searched.
                - Number of folders searched.
        """
        matched_files: list[Path] = []
        files_searched = 0
        folders_searched = 0
        folders_with_matches = set()

        iterator = path.rglob("*") if recursive_search else path.glob("*")

        for subpath in iterator:
            if subpath.is_dir():
                folders_searched += 1

                if recursive_search and report_level:
                    try:
                        relative_path = subpath.relative_to(root_dir)
                        depth = len(relative_path.parts)
                    except ValueError:
                        depth = 0

                    if depth % report_level == 0:
                        display_path = (
                            subpath.relative_to(current_dir)
                            if subpath.is_relative_to(current_dir)
                            else subpath.resolve()
                        )
                        logger.info(f"Searching (depth {depth}): {display_path}")
                continue

            files_searched += 1
            filename = subpath.name.lower()

            # Inclusion Check: Check if the filename matches any inclusion pattern
            if any(fnmatch.fnmatch(filename, pattern) for pattern in patterns):
                # Exclusion Check: Ensure the filename does not match any exclusion pattern
                if not any(fnmatch.fnmatch(filename, exclude) for exclude in excludes):
                    logger.debug(f"Matched file: {subpath.relative_to(current_dir)}")
                    matched_files.append(subpath)
                    folders_with_matches.add(subpath.parent)

        if print_found_folder:
            for folder in folders_with_matches:
                display_path = (
                    folder.relative_to(current_dir)
                    if folder.is_relative_to(current_dir)
                    else folder.resolve()
                )
                logger.info(f"Folder with matched files: {display_path}")

        logger.info(f"Found {len(matched_files)} files in {path}")
        return matched_files, files_searched, folders_searched

    logger.info(f"Starting search in {len(root_dirs)} root directory(ies).")

    total_files_searched = 0
    total_folders_searched = 0
    all_matched_files: list[Path] = []

    # Utilize a thread pool to perform directory searches in parallel
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(search_dir, root_dir, root_dir): root_dir
            for root_dir in root_dirs
        }

        for future in as_completed(futures):
            root_dir = futures[future]
            try:
                matched, files, folders = future.result()
                all_matched_files.extend(matched)
                total_files_searched += files
                total_folders_searched += folders
            except Exception as e:
                logger.error(f"Error searching in {root_dir}: {e}")

    logger.info(f"Total files matched: {len(all_matched_files)}")
    logger.info(f"Total files searched: {total_files_searched}")
    logger.info(f"Total folders searched: {total_folders_searched}")

    return all_matched_files


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
