# Maintenance benchmarks

These scripts measure candidate implementation choices and regressions; they are not production interfaces or
repeatable performance claims. Record the command, Python and package versions, data source, storage location and result
when using their output to support a decision.

## File collection

`benchmark_file_collection.py` compares `find_files_parallel()` and TUFLOW `collect_files()` from the installed package
and the current working tree. Missing roots are skipped, which allows the local and network paths to be measured in one
run:

```powershell
py -3.14 repo-scripts\benchmarks\benchmark_file_collection.py --runs 5
py -3.14 repo-scripts\benchmarks\benchmark_file_collection.py --local-root D:\results --network-root \\server\share\results
```

The installed comparison is meaningful only when the installed distribution is distinct from the editable checkout.

## DataFrame backends

`benchmark_dataframe_backends.py` measures concatenation and grouping using synthetic pandas frames. It can also try
Polars and PyArrow when installed, or load existing CSV/Parquet files with repeated `--input-glob` options:

```powershell
py -3.14 repo-scripts\benchmarks\benchmark_dataframe_backends.py --num-frames 200 --rows-per-frame 1000
py -3.14 repo-scripts\benchmarks\benchmark_dataframe_backends.py --input-glob "results\*.csv" --limit-frames 100
```

Backend timings include different conversion and feature scopes, so compare like-for-like actions and validate output
semantics before choosing an implementation.
