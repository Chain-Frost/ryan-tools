# ryan_functions/__init__.py

import importlib
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import find_spec, spec_from_loader
import pkgutil
from collections.abc import Sequence
import sys
import warnings
from types import ModuleType

import ryan_library.functions

# Issue a warning when the module is imported
warnings.warn(
    message="The 'ryan_functions' package is deprecated and has been moved to 'ryan_library.functions'. "
    "Please update your import statements accordingly.",
    category=UserWarning,
    stacklevel=2,
)

__all__: list[str] = [  # pyright: ignore[reportUnsupportedDunderAll]
    name for _, name, _ in pkgutil.iter_modules(ryan_library.functions.__path__)
]

_COMPATIBILITY_PREFIX = f"{__name__}."
_TARGET_PREFIX = "ryan_library.functions."


class _CompatibilityModuleLoader(Loader):
    """Load one modern module and expose it through its deprecated name."""

    def __init__(self, target_name: str) -> None:
        self.target_name = target_name

    def create_module(self, spec: ModuleSpec) -> ModuleType:
        module: ModuleType = importlib.import_module(self.target_name)
        sys.modules[spec.name] = module
        return module

    def exec_module(self, module: ModuleType) -> None:
        """The target module was executed by :meth:`create_module`."""


class _CompatibilityModuleFinder(MetaPathFinder):
    """Resolve deprecated dotted imports without importing unrelated modules."""

    compatibility_package = __name__

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        del path, target
        if not fullname.startswith(_COMPATIBILITY_PREFIX):
            return None

        target_name: str = _TARGET_PREFIX + fullname.removeprefix(_COMPATIBILITY_PREFIX)
        target_spec: ModuleSpec | None = find_spec(target_name)
        if target_spec is None:
            return None

        return spec_from_loader(
            fullname,
            _CompatibilityModuleLoader(target_name),
            origin=target_spec.origin,
            is_package=target_spec.submodule_search_locations is not None,
        )


if not any(getattr(finder, "compatibility_package", None) == __name__ for finder in sys.meta_path):
    sys.meta_path.insert(0, _CompatibilityModuleFinder())


def __getattr__(name: str) -> ModuleType:
    """Load a requested compatibility submodule on first attribute access."""
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module: ModuleType = importlib.import_module(name=f"{_TARGET_PREFIX}{name}")
    globals()[name] = module
    sys.modules[f"{_COMPATIBILITY_PREFIX}{name}"] = module
    return module
