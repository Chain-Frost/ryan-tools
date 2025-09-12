# ryan_library/functions/file_utils.py

import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import fnmatch
from loguru import logger
import threading
from queue import Queue


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
        patterns (str | list[str]): Glob pattern(s) to include in the search.
            Can be a single pattern string or a list of patterns.
        excludes (str | list[str] | None): Glob pattern(s) to exclude from the search.
            Can be a single pattern string, a list of patterns, or None.
        report_level (int | None, optional): Determines the frequency of logging
            folder search progress based on directory depth. Defaults to 2.
        print_found_folder (bool, optional): If True, logs the folders that contain
            matched files. Defaults to True.
        recursive_search (bool, optional): If True, searches directories recursively.
            Defaults to True.

    Returns:
        list[Path]: A list of file paths that match the specified patterns and do not
            match any of the exclusion patterns.
    """

    # Normalize 'patterns' and 'excludes' to lists for consistent processing
    patterns = [patterns] if isinstance(patterns, str) else patterns
    excludes = [excludes] if isinstance(excludes, str) else excludes or []

    # Convert all patterns to lowercase for case-insensitive matching
    patterns = [p.lower() for p in patterns]
    excludes = [e.lower() for e in excludes]

    logger.info(f"Root directories: {root_dirs}")
    logger.info(f"Search patterns: {patterns}")
    if excludes:
        logger.info(f"Exclude patterns: {excludes}")

    # Obtain the current working directory to calculate relative paths later
    current_dir: Path = Path.cwd()

    # Thread-safe structures
    matched_files_lock = threading.Lock()
    matched_files: list[Path] = []
    folders_with_matches_lock = threading.Lock()
    folders_with_matches: set[Path] = set()

    # Visited directories to prevent processing the same directory multiple times
    visited_lock = threading.Lock()
    visited_dirs: set[Path] = set()

    # Queue for directories to process
    dir_queue = Queue()

    # Initialize the queue with resolved root directories
    for root_dir in root_dirs:
        try:
            resolved_root: Path = root_dir.resolve(strict=True)
            with visited_lock:
                if resolved_root not in visited_dirs:
                    visited_dirs.add(resolved_root)
                    dir_queue.put((resolved_root, resolved_root))  # (current_path, root_dir)
        except FileNotFoundError:
            logger.error(f"Root directory does not exist: {root_dir}")
        except Exception as e:
            logger.error(f"Error resolving root directory {root_dir}: {e}")

    def worker() -> None:
        while True:
            try:
                current_path, root_dir = dir_queue.get(timeout=1)
                # Timeout to allow graceful exit
            except:
                # Queue is empty or timeout reached
                return
            try:
                local_matched = []
                local_folders_with_matches = set()
                files_searched = 0
                folders_searched = 0

                # Use non-recursive glob to avoid overlapping traversals
                try:
                    iterator = current_path.iterdir()
                except PermissionError:
                    logger.error(f"Permission denied accessing directory: {current_path}")
                    dir_queue.task_done()
                    continue
                except Exception as e:
                    logger.error(f"Error accessing directory {current_path}: {e}")
                    dir_queue.task_done()
                    continue

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
                                try:
                                    display_path = subpath.relative_to(current_dir)
                                except ValueError:
                                    display_path = subpath.resolve()
                                logger.info(f"Searching (depth {depth}): {display_path}")

                        if recursive_search:
                            try:
                                resolved_subpath = subpath.resolve(strict=True)
                            except FileNotFoundError:
                                logger.warning(f"Subdirectory does not exist (might be a broken symlink): {subpath}")
                                continue
                            except PermissionError:
                                logger.error(f"Permission denied accessing subdirectory: {subpath}")
                                continue
                            except Exception as e:
                                logger.error(f"Error resolving subdirectory {subpath}: {e}")
                                continue

                            with visited_lock:
                                if resolved_subpath in visited_dirs:
                                    continue
                                visited_dirs.add(resolved_subpath)
                            # Enqueue the resolved subdirectory for processing
                            dir_queue.put((resolved_subpath, root_dir))
                        continue

                    files_searched += 1
                    filename = subpath.name.lower()

                    # Inclusion Check
                    if any(fnmatch.fnmatch(filename, pattern) for pattern in patterns):
                        # Exclusion Check
                        if not any(fnmatch.fnmatch(filename, exclude) for exclude in excludes):
                            try:
                                matched_file = subpath.resolve(strict=True)
                                logger.debug(f"Matched file: {matched_file.relative_to(current_dir)}")
                                local_matched.append(matched_file)
                                local_folders_with_matches.add(matched_file.parent)
                            except FileNotFoundError:
                                logger.warning(f"File does not exist (might have been moved): {subpath}")
                            except PermissionError:
                                logger.error(f"Permission denied accessing file: {subpath}")
                            except Exception as e:
                                logger.error(f"Error resolving file {subpath}: {e}")

                # Safely update the global matched_files list
                if local_matched:
                    with matched_files_lock:
                        matched_files.extend(local_matched)
                # Safely update the global folders_with_matches set
                if local_folders_with_matches and print_found_folder:
                    with folders_with_matches_lock:
                        folders_with_matches.update(local_folders_with_matches)

                if len(local_matched) > 0:
                    logger.info(f"Found {len(local_matched)} files in {current_path}")
            except Exception as e:
                logger.error(f"Unexpected error processing {current_path}: {e}")
            finally:
                dir_queue.task_done()

    logger.info(f"Starting search in {len(root_dirs)} root directory(ies).")

    # Determine the number of worker threads; adjust as needed
    num_workers: int = min(32, (len(root_dirs) * 4) or 4)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Launch worker threads
        futures = [executor.submit(worker) for _ in range(num_workers)]

        # Wait for all tasks in the queue to be processed
        dir_queue.join()

        # Optionally, wait for all worker threads to complete
        for future in futures:
            future.result()

    # Log folders with matched files
    if print_found_folder:
        for folder in folders_with_matches:
            try:
                display_path: Path = folder.relative_to(current_dir)
            except ValueError:
                display_path = folder.resolve()
            logger.info(f"Folder with matched files: {display_path}")

    logger.info(f"Total files matched: {len(matched_files)}")

    return matched_files


def is_non_zero_file(fpath: Path | str) -> bool:
    """Verify that a given file exists, is indeed a file, and is not empty.

    This function performs a series of checks on the provided file path to ensure
    that the file is present, accessible, and contains data. It logs specific
    error messages if any of these conditions are not met.

    Args:
        fpath (Path): The path to the file to be checked.

    Returns:
        bool: True if the file exists, is a regular file, and is non-empty.
              False otherwise.
    """

    # force fpath to a Path. Some legacy scripts passed them as strings.
    fpath = Path(fpath)

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
        logger.error(f"An unexpected error occurred while accessing file '{fpath}': {e}")
        return False


def ensure_output_directory(output_dir: Path) -> None:
    """Ensure that the specified output directory exists; create it if it does not.

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
