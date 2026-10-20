# TUFLOW proposed-run input preflight

## Scope

Implement a read-only Python checker in ryan-tools for every event/scenario combination proposed by the maintained
TUFLOW simulation launcher. It must verify scenario- and event-dependent control-file inputs and event-specific
BC database Source files without starting TUFLOW. Provide a standalone wrapper and an opt-in integrated check.
Preserve existing GPU scheduling, run ordering and batch execution defaults.

## State

In validation. Owner: ChatGPT. Updated: 2026-10-21. Next review: exact-head CI and Windows engineering smoke checks.

## Decisions

- PyTUFLOW is optional, not a required runtime dependency for the whole ryan-tools distribution.
- Reuse the launcher's exact deduplicated Simulation objects for integrated checks.
- Extract reusable run-flag parsing into ryan_library, while retaining the launcher scheduling/dashboard logic.
- Keep checks read-only; write only a finding CSV; fail closed on unresolved references.
- Enable integrated checks only when explicitly requested until real-model validation passes.

## Acceptance criteria

- No TUFLOW process started from preflight-only or standalone checker.
- Missing BC Source CSV for one event is reported only for its affected proposed runs.
- Recursively inspect model input references; check GeoPackage layer names when enabled.
- Return non-zero and prevent batch launch for missing/unresolved dependencies.
- Provide --no-pause and --help on both wrappers.
- Document limitations, install and regression testing; verify current Python 3.14 wheel.

## Validation and delivery

New focused tests accompany the implementation. GitHub hosted CI and native Windows/PyTUFLOW installation,
large model performance, optional package extra and actual HPC control file checks remain to be verified.
Do not merge until validation and review are complete.

## Next action

Run exact-head Ruff, formatting, Pyright, focused pytest, wrapper --help, and Windows-based representative
model checks; address any review comments before requesting merge.
