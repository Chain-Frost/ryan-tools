# Canonical RORB readers (standalone wheel)

This subproject packages the **same six maintained Python modules**
from `../../ryan_library/functions/RORB`. Do not copy them here.

Build from the full Git checkout, from any supported Python 3.11–3.14 interpreter:

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


### Physical-site / printed-column binding

`bind_output_site_columns(document)` in `ryan_library.functions.RORB.output_reader`
binds the validated standard report's `Site Description` to **exact source**
`HydNNN` hydrograph table column spellings. `Hyd001` and `Hyd0001` have the
same ordinal. The `site_id` property is the **canonical**, zero-padded ID
(e.g. `Hyd0001`), not the raw site-legend text. The new `site_id_token`
property holds the exact numeric identifier printed in `Site Description`
(e.g. `01` or `0001`); `column` retains the exact printed Hyd label.
Physical names and line numbers are preserved. Matching never uses the site
or column position.

Invalid/missing sites, duplicated physical names, missing columns, and
alternate-padding duplicate ordinals fail closed with `ValueError`. Unnamed
sites also fail closed by default. For strictly quantitative source-token
comparison, `allow_unnamed=True` preserves an ordinal association but returns
`physical_name=None`: the physical print location is **not established** and
consumer-specific single/multiple-output restrictions still apply.

The binding does not establish native executable identity, catchment
geometry or numerical equivalence. Pin a reviewed source commit in consumers;
do not silently replace an installed-wheel dependency with an unmerged branch.

The full project and the six shipped readers support Python 3.11–3.14.
The reader-suite AST regression pins Python 3.11 grammar independently of
interpreter choice. The wheel includes only the canonical readers and their
package initializers/PEP 561 marker; parent convenience modules are not collected.
Build from a clean full checkout to avoid stale setuptools build products.
