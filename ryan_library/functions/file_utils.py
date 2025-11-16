# ryan_library/functions/file_utils.py

from collections.abc import Generator
from pathlib import Path
import fnmatch
import re
from loguru import logger
import threading
from queue import Empty, Queue


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

    def _normalize_globs(globs: str | list[str] | None) -> list[str]:
        """Ensure downstream logic always receives an iterable of glob strings."""
        if globs is None:
            return []
        if isinstance(globs, str):
            return [globs]
        return list(globs)

    def _compile_patterns(globs: list[str]) -> list[re.Pattern[str]]:
        """Translate glob syntax into compiled regex objects once up front."""
        compiled: list[re.Pattern[str]] = []
        for raw_pattern in globs:
            compiled.append(re.compile(fnmatch.translate(raw_pattern), re.IGNORECASE))
        return compiled

    def _matches_any(name: str, compiled: list[re.Pattern[str]]) -> bool:
        """Return True when the provided filename matches any compiled glob."""
        return any(pattern.match(name) for pattern in compiled)

    include_patterns: list[str] = _normalize_globs(patterns)
    exclude_patterns: list[str] = _normalize_globs(excludes)

    compiled_includes: list[re.Pattern[str]] = _compile_patterns(include_patterns)
    compiled_excludes: list[re.Pattern[str]] = _compile_patterns(exclude_patterns)

    logger.info(f"Root directories: {root_dirs}")
    logger.info(f"Search patterns: {include_patterns}")
    if exclude_patterns:
        logger.info(f"Exclude patterns: {exclude_patterns}")

    # Obtain the current working directory to calculate relative paths later
    # ``absolute`` preserves drive-letter vs UNC style while ensuring an
    # absolute path.
    current_dir: Path = Path.cwd().absolute()

    # Thread-safe structures
    matched_files_lock = threading.Lock()
    matched_files: list[Path] = []
    folders_with_matches_lock = threading.Lock()
    folders_with_matches: set[Path] = set()

    # Visited directories to prevent processing the same directory multiple times
    visited_lock = threading.Lock()
    visited_dirs: set[Path] = set()

    # Queue for directories to process; the stop event lets workers exit once traversal finishes.
    dir_queue: Queue[tuple[Path, Path]] = Queue()
    stop_event = threading.Event()

    # Initialize the queue with absolute root directories without converting
    # between drive letters and UNC paths.
    for root_dir in root_dirs:
        try:
            abs_root: Path = root_dir.expanduser().absolute()
            if not abs_root.exists():
                raise FileNotFoundError
            with visited_lock:
                if abs_root not in visited_dirs:
                    visited_dirs.add(abs_root)
                    dir_queue.put((abs_root, abs_root))  # (current_path, root_dir)
        except FileNotFoundError:
            logger.error(f"Root directory does not exist: {root_dir}")
        except Exception as exc:
            logger.error(f"Error resolving root directory {root_dir}: {exc}")

    def worker() -> None:
        """Continuously pull directories off the queue, scanning files and enqueueing child folders."""
        while not stop_event.is_set():
            try:
                current_path, root_dir = dir_queue.get(timeout=0.2)
            except Empty:
                continue
            try:
                # Keep per-thread results local so we only touch global locks when we have matches.
                local_matched: list[Path] = []
                local_folders_with_matches: set[Path] = set()

                try:
                    iterator: Generator[Path, None, None] = current_path.iterdir()
                except PermissionError:
                    logger.error(f"Permission denied accessing directory: {current_path}")
                    continue
                except Exception as exc:
                    logger.error(f"Error accessing directory {current_path}: {exc}")
                    continue

                for subpath in iterator:
                    if subpath.is_dir():
                        if recursive_search and report_level:
                            try:
                                relative_path = subpath.relative_to(root_dir)
                                depth = len(relative_path.parts)
                            except ValueError:
                                depth = 0

                            if depth % report_level == 0:
                                try:
                                    display_path: Path = subpath.relative_to(current_dir)
                                except ValueError:
                                    display_path = subpath.absolute()
                                logger.debug(f"Searching (depth {depth}): {display_path}")

                        if recursive_search:
                            try:
                                resolved_subpath: Path = subpath.absolute()
                                if not resolved_subpath.exists():
                                    logger.warning(
                                        f"Subdirectory does not exist (might be a broken symlink): {subpath}"
                                    )
                                    continue
                            except PermissionError:
                                logger.error(f"Permission denied accessing subdirectory: {subpath}")
                                continue
                            except Exception as exc:
                                logger.error(f"Error resolving subdirectory {subpath}: {exc}")
                                continue

                            # Record directories we've seen so we do not process the same path twice
                            # when multiple roots overlap or symlinks point back to an ancestor.
                            with visited_lock:
                                if resolved_subpath in visited_dirs:
                                    continue
                                visited_dirs.add(resolved_subpath)
                            # Enqueue the resolved subdirectory for processing
                            dir_queue.put((resolved_subpath, root_dir))
                        continue

                    filename: str = subpath.name

                    if not _matches_any(name=filename, compiled=compiled_includes):
                        continue

                    if compiled_excludes and _matches_any(name=filename, compiled=compiled_excludes):
                        continue

                    try:
                        matched_file: Path = subpath.absolute()
                        display_path = matched_file
                        try:
                            display_path = matched_file.relative_to(current_dir)
                        except ValueError:
                            pass
                        logger.debug(f"Matched file: {display_path}")
                        if not matched_file.exists():
                            raise FileNotFoundError
                        local_matched.append(matched_file)
                        local_folders_with_matches.add(matched_file.parent)
                    except FileNotFoundError:
                        logger.warning(f"File does not exist (might have been moved): {subpath}")
                    except PermissionError:
                        logger.error(f"Permission denied accessing file: {subpath}")
                    except Exception as exc:
                        logger.error(f"Error resolving file {subpath}: {exc}")

                # Safely update the global matched_files list
                if local_matched:
                    with matched_files_lock:
                        matched_files.extend(local_matched)
                # Safely update the global folders_with_matches set
                if local_folders_with_matches and print_found_folder:
                    with folders_with_matches_lock:
                        folders_with_matches.update(local_folders_with_matches)

                if local_matched:
                    logger.debug(f"Found {len(local_matched)} files in {current_path}")
            except Exception as exc:
                logger.error(f"Unexpected error processing {current_path}: {exc}")
            finally:
                dir_queue.task_done()

    logger.info(f"Starting search in {len(root_dirs)} root directory(ies).")
    num_workers: int = min(32, max(len(root_dirs) * 4, 4))
    threads: list[threading.Thread] = [
        threading.Thread(target=worker, name=f"find-files-worker-{i}", daemon=True) for i in range(num_workers)
    ]
    for thread in threads:
        thread.start()

    # Wait until every directory queued for processing has been handled.
    dir_queue.join()
    stop_event.set()

    for thread in threads:
        thread.join()

    # Log folders with matched files
    if print_found_folder:
        for folder in folders_with_matches:
            try:
                display_path: Path = folder.relative_to(current_dir)
            except ValueError:
                display_path = folder.absolute()
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
