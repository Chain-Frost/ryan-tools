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

The distribution explicitly includes the existing `ryan_library/py.typed` marker
and root `ryan_library/__init__.py`, so Pyright can type-check the inline
reader annotations under PEP 561. These are packaged alongside the original
six canonical modules, without copying or adding a second parser. The marker
was verified in a clean local setuptools wheel-layout experiment; the real
GitHub source-archive installed-wheel check remains an independent gate.


The full parent project targets Python 3.14, but **all six shipped reader
sources must parse under Python 3.13**. The reader-suite AST regression pins
that grammar independently of Ruff's whole-project 3.14 formatting target,
including multi-exception handlers. A successful full ryan-tools build alone
does not establish consumer Python 3.13 compatibility.
