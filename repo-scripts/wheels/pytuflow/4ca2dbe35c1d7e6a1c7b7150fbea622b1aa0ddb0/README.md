# PyTuflow 1.3.1 contribution dependency

Source: [PR #17 single commit 4ca2dbe35c1d](https://github.com/TUFLOW-Support/PyTuflow/pull/17), based on official 1.3.1. This is not the released PyPI 1.3.1 source.

Wheel: `pytuflow-1.3.1-py3-none-any.whl`, 984,867 bytes.
SHA-256: `41facdb3b220a9d032e7fb7e3a5ea94dee76a0e910fbeb0a5caf44289a6f4870`.

Built on Windows/Python 3.14.6 from a clean detached checkout verified against the upstream PR ref and its single parent. The sibling provenance manifest records the immutable source and actual build tools. Reproduce with `python -m pip wheel --no-deps --no-cache-dir --no-build-isolation --wheel-dir <temp> <verified_git_checkout>`.

Run `python repo-scripts/vendor_pytuflow_wheel.py` before installation and add `--installed` afterwards. The verifier checks bytes, metadata, license, CLI entries, runtime resources, typed marker and installed archive URL/digest.

Fresh acceptance and limitations are recorded in [the update record](../../../../docs/work/2026-11-15-pytuflow-pr17-131.md) and its consumer PR. Historical native/private-model evidence is not fresh acceptance of this SHA. Hosted job links are recorded on the PR after execution; an empty manifest job list does not claim hosted acceptance.

This repository installation artifact is excluded from the published ryan-tools package. Replace the temporary pin only when a released upstream build contains the required functionality and passes consumer acceptance.
