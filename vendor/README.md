# Vendored numerical implementations

These modules are third-party sources retained in `ryan-tools` for specialized algorithms. They are **not** first-party `ryan_library` implementations, and should not be routinely reformatted or refactored with the rest of the repository.

| Component | Upstream | License | Maintained integration |
| --- | --- | --- | --- |
| `simil` | [caprieldeluca/simil](https://github.com/caprieldeluca/simil) | MIT (Gabriel De Luca) | `ryan_library.functions.geometry.similarity.find_similarity_transform` |
| `PyHMA` | [icobro/PyHMA](https://github.com/icobro/PyHMA) | MIT (Ico Broekhuizen) | Direct `vendor.PyHMA.HMA` use; no first-party API yet |

Each component includes a copy of the required upstream license text. Original upstream commit identifiers were not recorded when these files were first imported; do not assume they match current upstream HEAD. Compare and record an upstream revision before attempting to refresh either implementation.

Local integration adjustments:
- `simil`: public `numpy.typing.NDArray` and `collections.abc.Sequence` imports; added package initializer for stable imports; fixed complete-point uniqueness and effective weighted-control preconditions (no change to the fitting algorithm).
- `PyHMA`: algorithm remains upstream-derived and unmodified. Its import requirements (`scipy`, `bottleneck`, NumPy, pandas and matplotlib) are supplied by `ryan-tools` dependencies.
- `vendor.__init__`: preserves `from vendor import HMA` as a lazy compatibility import without importing PyHMA when using `simil`.

The package wheel distributes these modules and notices. Keep algorithm changes isolated, preserve upstream attribution and add focused regression tests for any numerical change.
