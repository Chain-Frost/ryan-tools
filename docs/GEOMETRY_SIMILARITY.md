# 3D similarity transformation

Use `ryan_library.functions.geometry.find_similarity_transform` for a similarity (Helmert) fit between corresponding 3D control points. It delegates the numerical solution to the third-party `simil` algorithm distributed with `ryan-tools`.

```python
import numpy as np
from ryan_library.functions.geometry import find_similarity_transform

source = np.array(
    [[0, 0, 0], [0, 2, 2], [2, 3, 1], [3, 1, 2], [1, 1, 3]],
    dtype=float,
)
target = np.array(
    [[3, 7, 5], [6, 7, 2], [4.5, 4, 0.5], [6, 2.5, 3.5], [7.5, 5.5, 3.5]],
    dtype=float,
)

scale, rotation, translation = find_similarity_transform(source, target)
transformed = (scale * rotation @ source.T + translation).T
np.testing.assert_allclose(transformed, target, atol=1e-10)
```

The returned multiplier is dimensionless; `rotation` is 3×3 and `translation` is 3×1. Input arrays are N×3, with matching row correspondence. Use `weights=[...]` for per-point weighting. `estimate_scale=False, initial_scale=1.0` fixes a unit scale; `initial_scale=-1.0` allows the upstream solver's reflected solution.

These coordinates are in the **same engineering frame** and units. This solver neither reprojects geographic CRS coordinates nor accounts for geodesy; do not substitute it for GDAL/PROJ CRS transformations. A fitted scale is not necessarily a rigid-body-only transform.

The upstream implementation requires NumPy, is maintained under [vendor/simil](../vendor/simil/README.md), and retains its own [MIT licence](../vendor/simil/LICENSE). The corresponding PyHMA vendor module remains separately importable without a first-party public algorithm wrapper; see [vendor inventory](../vendor/README.md).
