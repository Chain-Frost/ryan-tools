# TUFLOW API examples

Use these snippets for small custom workflows and exploratory work. The maintained scripts in
[`ryan-scripts/TUFLOW-python`](../../ryan-scripts/TUFLOW-python/README.md) are the preferred entry points for established
reports because they already provide editable defaults, command-line options, logging, exports and exit handling.

## Process one result

`BaseProcessor.from_file()` selects the concrete processor from the configured filename suffix:

```python
from pathlib import Path

from ryan_library.processors.tuflow.base_processor import BaseProcessor

processor = BaseProcessor.from_file(Path("M11_01p_00120m_TP01_1d_Q.csv"))
processor.process()

if processor.processed and not processor.df.empty:
    print(processor.data_type)
    print(processor.df.head())
    processor.discard_raw_dataframe()
```

## Discover and process a batch

`collect_files()` uses the shared suffix registry, while `process_files_in_parallel()` returns a
`ProcessorCollection` that can combine compatible results:

```python
from pathlib import Path

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.tuflow_common import collect_files, process_files_in_parallel

files = collect_files(
    paths_to_process=[Path("results")],
    include_data_types=["Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF"],
    suffixes_config=SuffixesConfig.get_instance(),
)

with setup_logger(console_log_level="INFO") as log_queue:
    collection = process_files_in_parallel(
        file_list=files,
        log_queue=log_queue,
        log_level="INFO",
        entity_filters=["Culvert_01", "Culvert_02"],
    )

maximums = collection.combine_1d_maximums()
```

`entity_filters` may be one collection of identifiers or a mapping keyed by data type. Use
`collection.combine_1d_timeseries()`, `po_combine()`, `pomm_combine()` or `combine_raw()` when those formats match the
loaded data.

## Check PO peaks and stability

The direct functions are useful when a custom caller needs the individual result records:

```python
from pathlib import Path

from ryan_library.functions.tuflow.po_timeseries_checks import PeakCheckConfig, analyze_peak_csv

results = analyze_peak_csv(
    Path("M11_01p_00120m_TP01_PO.csv"),
    PeakCheckConfig(
        datatype_include=["Flow"],
        datatype_case_sensitive=False,
        location_include=[],
        location_exclude=[],
        location_case_sensitive=False,
        warn_2hours=2.0,
        warn_1hour=1.0,
        flat_tol=1e-6,
    ),
)

for result in results:
    print(result.location, result.status, result.peak_time)
```

The same module provides `StabilityCheckConfig`, `analyze_stability_csv()`, `analyze_stability_q_csv()`,
`flatten_peak_results()` and `flatten_stability_results()`.

## Build POMM summaries

```python
from pathlib import Path

from ryan_library.functions.tuflow.pomm_utils import aggregated_from_paths, find_aep_dur_max, find_aep_max

aggregated = aggregated_from_paths(paths=[Path("results")])
if not aggregated.empty:
    aep_duration_max = find_aep_dur_max(aggregated)
    aep_max = find_aep_max(aep_duration_max)
```

For alternative ensemble statistics, use `find_aep_dur_median()` with `find_aep_median_max()`, or
`find_aep_dur_mean()` with `find_aep_mean_max()`.
