# Testing Strategy and Architecture

## Architecture Overview

This repository follows a separation-of-concerns pattern:

1. **Human-facing wrappers (`ryan-scripts`)**:
    * **Role**: Editable and directly runnable entry points.
    * **Responsibility**: Set user-facing defaults, working directories, and command-line options before calling an orchestrator.
    * **Constraint**: Keep wrappers thin so workflow behaviour is not duplicated.

2. **Orchestrators (`ryan_library/orchestrators`)**:
    * **Role**: Workflow controllers.
    * **Responsibility**: Coordinate multiprocessing, logging, file collection, processors, reusable functions, and exports.
    * **Constraint**: Contain high-level flow control but delegate reusable business logic to `functions` and `processors`.

3. **Functions (`ryan_library/functions`)**:
    * **Role**: Workers / Utilities.
    * **Responsibility**: Pure functions, data processing logic, file I/O helpers, and specific algorithms.
    * **Constraint**: Should be testable in isolation.

4. **Processors (`ryan_library/processors`)**:
    * **Role**: Object-Oriented Data Handlers.
    * **Responsibility**: Encapsulate state and logic for specific data types (e.g., TUFLOW results). Inherit from `BaseProcessor`.

`ryan_library/scripts` is a deprecated compatibility namespace that forwards legacy imports to
`ryan_library/orchestrators` or `ryan_library/functions`. New code must not use it. Its compatibility deadline is
31 December 2026; tests and callers must migrate before the namespace is removed.

## Testing Strategy

Tests are located in the `tests/` directory and mirror the structure of `ryan_library/`.

### Goals

* **Thoroughness**: Cover basic functionality (loading, writing, error handling) and deep logic verification.
* **Regression Prevention**: Ensure future changes do not break existing functionality.
* **Data**: Use `ryan-tools\tests\test_data\tuflow\` for TUFLOW-related tests.

### Naming Conventions

* Test files: `test_<module_name>.py`
* Test classes: `Test<ClassName>`
* Test functions: `test_<function_name>_<condition>`

## Deprecation Policy

* Legacy code should be identified and listed for deprecation.
* Prefer delegating legacy endpoints to maintained functions rather than duplicating logic.
* New imports must use `ryan_library.orchestrators` rather than the deprecated `ryan_library.scripts` namespace.
