from affine import Affine

class Window:
    col_off: float
    row_off: float
    width: float
    height: float
    def __init__(self, col_off: float, row_off: float, width: float, height: float) -> None: ...

def transform(window: Window, transform: Affine) -> Affine: ...
