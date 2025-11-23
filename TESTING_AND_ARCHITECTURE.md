# Testing Strategy and Architecture

## Architecture Overview

This repository follows a strict separation of concerns pattern:

1.  **Scripts (`ryan_library/scripts`)**:
    *   **Role**: Orchestrators.
    *   **Responsibility**: Handle CLI arguments, multiprocessing setup, logging configuration, and high-level flow control.
    *   **Constraint**: Should contain minimal business logic. They delegate to `functions` and `processors`.

2.  **Functions (`ryan_library/functions`)**:
    *   **Role**: Workers / Utilities.
    *   **Responsibility**: Pure functions, data processing logic, file I/O helpers, and specific algorithms.
    *   **Constraint**: Should be testable in isolation.

3.  **Processors (`ryan_library/processors`)**:
    *   **Role**: Object-Oriented Data Handlers.
    *   **Responsibility**: Encapsulate state and logic for specific data types (e.g., TUFLOW results). Inherit from `BaseProcessor`.

## Testing Strategy

Tests are located in the `tests/` directory and mirror the structure of `ryan_library/`.

### Goals
*   **Thoroughness**: Cover basic functionality (loading, writing, error handling) and deep logic verification.
*   **Regression Prevention**: Ensure future changes do not break existing functionality.
*   **Data**: Use `ryan-tools\tests\test_data\tuflow\` for TUFLOW-related tests.

### Naming Conventions
*   Test files: `test_<module_name>.py`
*   Test classes: `Test<ClassName>`
*   Test functions: `test_<function_name>_<condition>`

## Deprecation Policy
*   Legacy code should be identified and listed for deprecation.
*   Prefer delegating legacy endpoints to maintained functions rather than duplicating logic.
