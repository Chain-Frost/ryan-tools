# Canonical RORB readers (standalone wheel)

This subproject packages the **same six maintained Python modules**
from `../../ryan_library/functions/RORB`. Do not copy them here.

Build from the full Git checkout, from any supported Python 3.13/3.14 interpreter:

```shell
python -m pip wheel --no-deps -w dist ./packaging/rorb-readers
python -m pip install --no-deps --target /tmp/ryan-reader-isolated dist/ryan_rorb_readers-*.whl
```

A consumer should install directly from the reviewed full **source archive** instead
of using pip's Git transport. Pip's Git transport recursively initialises
ryan-tools' unrelated test-data submodule and hits Windows long-path failures.
The immutable GitHub source archive omits all submodule contents:

```text
ryan-rorb-readers @ https://github.com/Chain-Frost/ryan-tools/archive/<commit>.tar.gz#subdirectory=packaging/rorb-readers
```

Replace `<commit>` with the exact 40-character reviewed commit SHA.
Only the six canonical RORB reader modules are packaged; neither submodule
retrieval nor changing global Git long-path settings is needed.
`read_rorb_file` and `read_rorb_bytes` are imported from
`ryan_library.functions.RORB.readers`. Generic report types come from
`ryan_library.functions.RORB.reader_types`.

This wheel has no pandas/GIS/native dependencies. It intentionally does not
ship pandas convenience readers or the full ryan-tools distribution. Do not
co-install with full ryan-tools, which owns overlapping import paths.

The licence is SUL-1.0, included as LICENSE. Original RORB output bytes and
source values remain under the user's control. Reading never launches RORB.
