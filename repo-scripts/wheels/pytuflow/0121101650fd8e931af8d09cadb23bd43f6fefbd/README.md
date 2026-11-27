# PyTuflow 1.3.1 materials CSV correction wheel

Source: [PR #17 at 0121101650fd8e931af8d09cadb23bd43f6fefbd](https://github.com/TUFLOW-Support/PyTuflow/pull/17). Supersedes 4ca2dbe35c1d7e6a1c7b7150fbea622b1aa0ddb0; this is not a released PyPI build.

Wheel: pytuflow-1.3.1-py3-none-any.whl, 978,076 bytes. SHA-256: b223a803d5c900b85c50855fc18128855dde016ac60e191de63043b440538fc3.

Built with Python 3.14.10 and hatchling 1.32.0. Synthetic materials CSV regressions passed on the installed build. Actual owner-file acceptance (11 materials and 12 run selections) is documented in fork PR #74, not rerun here. No hydraulic or native solver run.

Verify using python repo-scripts/vendor_pytuflow_wheel.py --installed after installing from repo-scripts/pytuflow-requirement.txt.
