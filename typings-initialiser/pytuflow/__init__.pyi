from collections.abc import Sequence
from pathlib import Path

class GlobalScope: ...

class ScenarioScope:
    names: Sequence[str]
    def is_neg(self) -> bool: ...

class TGCInput:
    lhs: str
    scope: Sequence[GlobalScope | ScenarioScope]
    files: Sequence[str | Path]
    def __str__(self) -> str: ...

class TGC:
    inputs: Sequence[TGCInput]
    def __init__(self, path: str | Path) -> None: ...
