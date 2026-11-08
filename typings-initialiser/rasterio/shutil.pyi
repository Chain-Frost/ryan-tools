from os import PathLike

from .io import DatasetReader

def copy(src: DatasetReader, dst: str | PathLike[str], *, driver: str, **creation_options: str) -> None: ...
