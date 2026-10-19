# PyHMA (vendored)

Upstream: [icobro/PyHMA](https://github.com/icobro/PyHMA), by Ico Broekhuizen.
License: MIT; full text in [LICENSE](LICENSE).
Source: [upstream `PyHMA/__init__.py`](https://github.com/icobro/PyHMA/blob/master/PyHMA/__init__.py).

This is the upstream-derived Hydrograph Matching Algorithm, implementing methods described by Ewen (2011). Its historical upstream revision was not recorded when initially copied into `ryan-tools`; the numerical implementation is deliberately left unchanged here.

## Usage

```python
import numpy as np
from vendor.PyHMA import HMA

observed = np.array([1., 2., 4., 3., 1.])
simulated = np.array([1., 2., 4., 3., 1.])
score = HMA(observed, simulated).calc_dense2()
```

`calc_dense2()` is the lower-memory approach, but cannot retain internals or connecting rays. Other supplied methods are `calc_orig()`, `calc_sparse()` and `calc_dense()`. For nonconstant time steps, consult the upstream examples and validate against representative hydrographs before engineering use.

This module requires NumPy, pandas, matplotlib, SciPy and Bottleneck; they are declared by `ryan-tools` for normal installs. Use `from vendor.PyHMA import HMA` rather than relying on the historic top-level `vendor.HMA` alias.

For component ownership, compatibility and licence details see [the vendor inventory](../README.md).
