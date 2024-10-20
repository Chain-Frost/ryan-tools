from concurrent.futures._base import Future
from glob import iglob
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


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
    return [f for f in iglob(pathname=search_pattern, recursive=True) if os.path.isfile(path=f)]


def find_files_parallel(root_dir: str, pattern: str = ".tlf", exclude: tuple = (".hpc.tlf", ".gpu.tlf")) -> list[str]:
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
