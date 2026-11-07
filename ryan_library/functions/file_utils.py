# ryan_library/functions/file_utils.py

import fnmatch
import re
import stat
import threading
from collections.abc import Generator
from pathlib import Path
from queue import Empty, Queue

from loguru import logger


def _physical_directory_identity(path: Path) -> tuple[int, int] | Path:
    """Identify a directory across spelling aliases without rewriting its visible path."""
    metadata = path.stat()  # Follow symlinks and Windows junctions for cycle detection.
    if metadata.st_ino:
        return metadata.st_dev, metadata.st_ino
    # Some network filesystems supply no stable inode; canonicalise only the key,
    # never the path shown to callers (mapped drives and UNC are preserved).
    return path.resolve(strict=True)


def find_files_parallel(
    root_dirs: list[Path],
    patterns: str | list[str],
    excludes: str | list[str] | None = None,
    report_level: int | None = 2,
    print_found_folder: bool = True,
    recursive_search: bool = True,
) -> list[Path]:
    """Search for files matching specific patterns across multiple directories in parallel.

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
        list[Path]: Sorted matched paths, preserving the spelling of the first
            visited physical directory. Distinct file hardlinks are not collapsed.

    Raises:
        OSError: If any root, directory or matched file could not be scanned;
            a partial match list is never reported as a successful scan.
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

    logger.info("Root directories: {}", root_dirs)
    logger.info("Search patterns: {}", include_patterns)
    if exclude_patterns:
        logger.info("Exclude patterns: {}", exclude_patterns)

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
    visited_dirs: set[tuple[int, int] | Path] = set()

    # Files cannot be reported as a complete scan if a root, directory, or
    # matched file was inaccessible. Workers accumulate failures for the caller.
    discovery_errors_lock = threading.Lock()
    discovery_errors: list[str] = []

    def record_error(reason: str, path: Path, exc: Exception | None = None) -> None:
        detail = f"{reason}: {path}" if exc is None else f"{reason}: {path}: {exc}"
        with discovery_errors_lock:
            discovery_errors.append(detail)

    # Queue for directories to process; the stop event lets workers exit once traversal finishes.
    dir_queue: Queue[tuple[Path, Path]] = Queue()
    stop_event = threading.Event()

    # Initialize the queue with absolute root directories without converting
    # between drive letters and UNC paths.
    for root_dir in root_dirs:
        try:
            abs_root: Path = root_dir.expanduser().absolute()
            if not abs_root.exists():
                raise FileNotFoundError(abs_root)
            if not abs_root.is_dir():
                raise NotADirectoryError(abs_root)
            directory_key = _physical_directory_identity(abs_root)
            with visited_lock:
                if directory_key not in visited_dirs:
                    visited_dirs.add(directory_key)
                    dir_queue.put((abs_root, abs_root))  # (current_path, root_dir)
        except FileNotFoundError as exc:
            logger.error("Root directory does not exist: {}", root_dir)
            record_error("Root directory does not exist", root_dir, exc)
        except NotADirectoryError as exc:
            logger.error("Root path is not a directory: {}", root_dir)
            record_error("Root path is not a directory", root_dir, exc)
        except Exception as exc:
            logger.error("Error resolving root directory {}: {}", root_dir, exc)
            record_error("Error resolving root directory", root_dir, exc)

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
                    iterator: Generator[Path] = current_path.iterdir()
                except PermissionError as exc:
                    logger.error("Permission denied accessing directory: {}", current_path)
                    record_error("Permission denied accessing directory", current_path, exc)
                    continue
                except Exception as exc:
                    logger.error("Error accessing directory {}: {}", current_path, exc)
                    record_error("Error accessing directory", current_path, exc)
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
                                logger.debug("Searching (depth {}): {}", depth, display_path)

                        if recursive_search:
                            try:
                                resolved_subpath: Path = subpath.absolute()
                                if not resolved_subpath.exists():
                                    logger.warning(
                                        "Subdirectory does not exist (might be a broken symlink): {}", subpath
                                    )
                                    record_error("Subdirectory disappeared during discovery", subpath)
                                    continue
                                directory_key = _physical_directory_identity(resolved_subpath)
                            except PermissionError as exc:
                                logger.error("Permission denied accessing subdirectory: {}", subpath)
                                record_error("Permission denied accessing subdirectory", subpath, exc)
                                continue
                            except Exception as exc:
                                logger.error("Error resolving subdirectory {}: {}", subpath, exc)
                                record_error("Error resolving subdirectory", subpath, exc)
                                continue

                            # Stat follows links to prevent re-entering an ancestor through a
                            # symlink/junction; keep the original lexical path in the queue.
                            with visited_lock:
                                if directory_key in visited_dirs:
                                    continue
                                visited_dirs.add(directory_key)
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
                        logger.debug("Matched file: {}", display_path)
                        if not matched_file.exists():
                            raise FileNotFoundError
                        local_matched.append(matched_file)
                        local_folders_with_matches.add(matched_file.parent)
                    except FileNotFoundError as exc:
                        logger.warning("File does not exist (might have been moved): {}", subpath)
                        record_error("Matched file disappeared during discovery", subpath, exc)
                    except PermissionError as exc:
                        logger.error("Permission denied accessing file: {}", subpath)
                        record_error("Permission denied accessing file", subpath, exc)
                    except Exception as exc:
                        logger.error("Error resolving file {}: {}", subpath, exc)
                        record_error("Error resolving file", subpath, exc)

                # Safely update the global matched_files list
                if local_matched:
                    with matched_files_lock:
                        matched_files.extend(local_matched)
                # Safely update the global folders_with_matches set
                if local_folders_with_matches and print_found_folder:
                    with folders_with_matches_lock:
                        folders_with_matches.update(local_folders_with_matches)

                if local_matched:
                    logger.debug("Found {} files in {}", len(local_matched), current_path)
            except Exception as exc:
                logger.error("Unexpected error processing {}: {}", current_path, exc)
                record_error("Unexpected error processing directory", current_path, exc)
            finally:
                dir_queue.task_done()

    logger.info("Starting search in {} root directory(ies).", len(root_dirs))
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
        for folder in sorted(folders_with_matches, key=lambda p: (str(p).casefold(), str(p))):
            try:
                display_path: Path = folder.relative_to(current_dir)
            except ValueError:
                display_path = folder.absolute()
            logger.info("Folder with matched files: {}", display_path)

    if discovery_errors:
        raise OSError(
            f"File discovery incomplete ({len(discovery_errors)} error(s)); first: {discovery_errors[0]}"
        )

    # Match paths are deduplicated by spelling, not by file identity: distinct
    # hardlinks / file symlinks remain separate; physical *directories* are unique.
    results = sorted(set(matched_files), key=lambda p: (str(p).casefold(), str(p)))
    logger.info("Total files matched: {}", len(results))
    return results


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
        stat_result = fpath.stat()
    except FileNotFoundError:
        logger.error("File does not exist: {}", fpath)
        return False
    except PermissionError:
        # Handle cases where the file cannot be accessed due to permission issues
        logger.error("Permission denied when accessing file: {}", fpath)
        return False
    except Exception as e:
        # Catch-all for any other unexpected errors
        logger.error("An unexpected error occurred while accessing file '{}': {}", fpath, e)
        return False

    if not stat.S_ISREG(stat_result.st_mode):
        logger.error("Path is not a file: {}", fpath)
        return False

    if stat_result.st_size == 0:
        logger.warning("File is empty: {}", fpath)
        return False

    return True  # All checks passed


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
        logger.info("Created output directory: {}", output_dir)
    else:
        logger.info("Output directory already exists: {}", output_dir)
