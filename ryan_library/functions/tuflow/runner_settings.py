"""Editable TUFLOW runner choices and accepted setting types.

Kept free of operational dependencies so copied wrappers can import these
values before their user-editable settings.
"""

from enum import StrEnum
from pathlib import Path
from typing import Literal


class OP(StrEnum):
    """Operation choices:

    - `OP.RUN` (`"run"`): run TUFLOW; may overwrite model outputs.
    - `OP.EXPORT_ONLY` (`"export_only"`): export command text without launching.
    - `OP.PREFLIGHT_ONLY` (`"preflight_only"`): check model inputs without launching.
    """

    RUN = "run"
    EXPORT_ONLY = "export_only"
    PREFLIGHT_ONLY = "preflight_only"


class SRC(StrEnum):
    """Simulation selection choices:

    - `SRC.PARAMETER_PRODUCT` (`"parameter_product"`): generate parameter combinations.
    - `SRC.TEXTFILES` (`"textfiles"`): import selections from BAT/CMD/TXT files.
    - `SRC.BOTH` (`"both"`): imported selections first, then generated combinations.
    """

    PARAMETER_PRODUCT = "parameter_product"
    TEXTFILES = "textfiles"
    BOTH = "both"


class CONT(StrEnum):
    """CSV continuation choices:

    - `CONT.RESUME` (`"resume"`): skip previously observed successful runs.
    - `CONT.ALL` (`"all"`): reset the completion baseline and rerun all selections.
    """

    RESUME = "resume"
    ALL = "all"


class PRIO(StrEnum):
    """Windows child process priority choices, from lowest to highest:

    - `PRIO.LOW` (`"LOW"`).
    - `PRIO.BELOWNORMAL` (`"BELOWNORMAL"`).
    - `PRIO.NORMAL` (`"NORMAL"`).
    - `PRIO.ABOVENORMAL` (`"ABOVENORMAL"`).
    - `PRIO.HIGH` (`"HIGH"`).
    - `PRIO.REALTIME` (`"REALTIME"`): can starve other processes; use with care.
    """

    LOW = "LOW"
    BELOWNORMAL = "BELOWNORMAL"
    NORMAL = "NORMAL"
    ABOVENORMAL = "ABOVENORMAL"
    HIGH = "HIGH"
    REALTIME = "REALTIME"


type OperationSetting = OP | Literal["run", "export_only", "preflight_only"]
type SourceSetting = SRC | Literal["parameter_product", "textfiles", "both"]
type ContinuationSetting = CONT | Literal["resume", "all"]
type PrioritySetting = PRIO | Literal["LOW", "BELOWNORMAL", "NORMAL", "ABOVENORMAL", "HIGH", "REALTIME"]
type PathSetting = str | Path
r"""A path string or `Path` object.

Examples: `r"C:\Models\run.tcf"`, `r"Q:\Models"`,
`r"\\server\share\Models"` or `Path("runs/model.tcf")`.

Relative paths use `WORKING_DIR`, except `WORKING_DIR` itself, which uses the
invoking directory. See the individual setting for its file or directory requirements.
Mapped-drive and UNC paths are supported; links are not dereferenced merely
to make a path absolute.
"""
type InputFilesSetting = PathSetting | list[PathSetting] | tuple[PathSetting, ...] | None
type GpuGroupSetting = str | list[str] | tuple[str, ...]
type GpuDevicesSetting = str | list[GpuGroupSetting] | tuple[GpuGroupSetting, ...] | None
"""GPU assignments: each outer entry is one concurrent simulation slot.

- `"-pu1"`: 1 run on GPU 1.
- `["-pu0", "-pu1"]`: 2 concurrent runs, one per GPU.
- `[["-pu0", "-pu1"]]`: 1 run using both GPUs.
- `[["-pu0", "-pu1"], "-pu2"]`: 2 concurrent runs, on GPUs 0+1 and GPU 2.
- `None`, `[]` or `()`: one automatic-placement slot, with no explicit GPU flag.

Each string must contain one `-puN` flag; `"-pu0 -pu1"` is invalid. GPU IDs
cannot repeat. Equivalent tuple forms are accepted. Imported BAT/TXT GPU switches are ignored;
this setting controls GPU assignments for Python launches.
"""
