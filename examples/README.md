# Examples

These examples show how to use maintained `ryan_library` APIs directly. For standard project workflows, start with a
maintained wrapper under [`ryan-scripts`](../ryan-scripts/README.md); wrappers retain editable paths and settings while
the library supplies reusable processing.

- [TUFLOW API examples](tuflow/README.md): filename parsing, processors, batch loading, PO checks and POMM summaries.
- [DataFrame and export examples](dataframes/README.md): shared table helpers and Excel/Parquet export.
- [`tuflow_workflow_demo.ipynb`](tuflow_workflow_demo.ipynb): notebook-oriented TUFLOW exploration.
- [`logging_usage.py`](logging_usage.py): serial, multiprocessing and notebook-safe Loguru setup.

Copy an example into project code and replace its paths, filters and output names. Do not edit installed package files.
