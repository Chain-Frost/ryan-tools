# Temporary accepted PyTuflow dependency wheel

This distribution is a **dependency artifact**, not package data in the
published ryan-tools wheel. Its immutable source is
[Chain-Frost/PyTuflow at 887534ea](https://github.com/Chain-Frost/PyTuflow/commit/887534ea8c15b80d185a38a7ead1d42ab42a7167),
associated with [upstream PR #17](https://github.com/TUFLOW-Support/PyTuflow/pull/17).
It is not the separate #65 integration wheel.

The accepted `pytuflow-1.3.0-py3-none-any.whl` is 976,447 bytes and has
SHA-256 `4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3`.
Three independent Windows/Python 3.14 jobs verified these same bytes.
The sibling `provenance.json` records source, build tools and evidence.
See [the CI dependency work record](../../../../docs/work/2026-11-08-ci-dependency-groups.md).

From the repository root, verify before any installation:

```powershell
python repo-scripts/vendor_pytuflow_wheel.py
```

The verifier checks the hash, ZIP contents, license, CLI entry points,
typed marker and runtime resources. Missing or mismatched bytes are a
hard error; do not substitute a newer upstream or unrelated fork wheel.
Keep it outside `dist/`, LFS and package-data discovery.

Remove the temporary pin and binary only when a **released** upstream build
contains the needed fixes and passes consumer/public capability acceptance.
