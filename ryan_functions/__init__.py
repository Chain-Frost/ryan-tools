# ryan_functions/__init__.py

import warnings
import importlib
import pkgutil
import sys  # Import sys to manipulate sys.modules
import ryan_library.functions

# Issue a warning when the module is imported
warnings.warn(
    "The 'ryan_functions' package is deprecated and has been moved to 'ryan_library.functions'. "
    "Please update your import statements accordingly.",
    UserWarning,
    stacklevel=2,
)

# Dynamically import all modules from ryan_library.functions
for loader, module_name, is_pkg in pkgutil.iter_modules(ryan_library.functions.__path__):
    module = importlib.import_module(f"ryan_library.functions.{module_name}")
    globals()[module_name] = module

    # Register each submodule in sys.modules
    sys.modules[f"ryan_functions.{module_name}"] = module

__all__ = [name for _, name, _ in pkgutil.iter_modules(ryan_library.functions.__path__)]


def __getattr__(name):
    """
    Handle attribute access for submodules dynamically.
    """
    if name in globals():
        return globals()[name]
    else:
        # Attempt to import the submodule from the new location
        try:
            module = importlib.import_module(f"ryan_library.functions.{name}")
            globals()[name] = module

            # Register the submodule in sys.modules
            sys.modules[f"ryan_functions.{name}"] = module

            warnings.warn(
                f"The '{name}' module is deprecated and has been moved to 'ryan_library.functions.{name}'. "
                "Please update your import statements accordingly.",
                UserWarning,
                stacklevel=2,
            )
            return module
        except ImportError as e:
            raise ImportError(f"Module '{name}' not found in 'ryan_functions' or 'ryan_library.functions'.") from e
