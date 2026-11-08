from affine import Affine

class Window:
    col_off: float
    row_off: float
    width: float
    height: float
    def __init__(self, col_off: float, row_off: float, width: float, height: float) -> None: ...
    def round_offsets(self, **kwargs: object) -> Window: ...
    def round_lengths(self, **kwargs: object) -> Window: ...

def transform(window: Window, transform: Affine) -> Affine: ...
def from_bounds(
    left: float,
    bottom: float,
    right: float,
    top: float,
    transform: Affine | None = ...,
    height: float | None = ...,
    width: float | None = ...,
    precision: float | None = ...,
) -> Window: ...
