# DataFrame and export examples

The shared helpers keep common table assembly and output behavior consistent across workflows.

```python
from pathlib import Path

import pandas as pd

from ryan_library.functions.dataframe_helpers import merge_and_sort_data, reorder_columns
from ryan_library.functions.misc_functions import ExcelExporter

frames = [
    pd.DataFrame({"run": ["A"], "value": [2.0]}),
    pd.DataFrame({"run": ["B"], "value": [1.0]}),
]
combined = merge_and_sort_data(frames, sort_column="value", ascending=False)
ordered = reorder_columns(combined, prioritized_columns=["run", "value"])

ExcelExporter().export_dataframes(
    export_dict={"summary": {"dataframes": [ordered], "sheets": ["Results"]}},
    output_directory=Path("outputs"),
    export_mode="both",
)
```

`export_mode="excel"` is the default. `"parquet"` skips Excel and `"both"` also writes companion Parquet output.
Use `include_data_dictionary=True` when a generic workbook needs generated column metadata; specialist orchestrators
may build their own richer dictionary.
