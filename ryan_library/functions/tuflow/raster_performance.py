"""Memory, thread and phase-timing contracts for native raster operations.

Memory settings are expressed in MiB. Rasterio's integer ``GDAL_CACHEMAX``
setting is in bytes, unlike the similarly named GDAL command-line setting.
"""

from dataclasses import dataclass

import psutil

from ..gdal.raster_processing import plan_gdal_concurrency

_MIB = 1024 * 1024
_PROCESS_OVERHEAD_MB = 128
_COMPRESSION_THREAD_MB = 8


def _require_integer(value: object, *, name: str, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        msg = f"{name} must be an integer of at least {minimum}"
        raise ValueError(msg)
    return value


@dataclass(slots=True, frozen=True)
class RasterExecutionOptions:
    """Validated per-process limits; caches are independent of NumPy chunks."""

    cache_mb: int = 128
    chunk_mb: int = 256
    threads: int = 1
    vsi_cache_mb: int = 0

    def __post_init__(self) -> None:
        _require_integer(self.cache_mb, name="Raster cache MiB")
        _require_integer(self.chunk_mb, name="Raster chunk MiB")
        _require_integer(self.threads, name="GDAL threads")
        _require_integer(self.vsi_cache_mb, name="VSI cache MiB", minimum=0)

    @property
    def gdal_cache_bytes(self) -> int:
        """Return the decoded GDAL block-cache limit in bytes."""
        return self.cache_mb * _MIB

    @property
    def chunk_bytes(self) -> int:
        """Return the planned NumPy working-memory limit in bytes."""
        return self.chunk_mb * _MIB

    @property
    def vsi_cache_bytes(self) -> int:
        """Return the additional raw file-cache limit per input in bytes."""
        return self.vsi_cache_mb * _MIB

    @property
    def threads_text(self) -> str:
        """Return the explicit GDAL creation/open-option thread count."""
        return str(self.threads)

    def rasterio_env_options(self) -> dict[str, int | bool]:
        """Return explicit Rasterio options without implicit per-file caches."""
        return {
            "GDAL_CACHEMAX": self.gdal_cache_bytes,
            "GDAL_NUM_THREADS": self.threads,
            "VSI_CACHE": self.vsi_cache_mb > 0,
            "VSI_CACHE_SIZE": self.vsi_cache_bytes,
        }


@dataclass(slots=True)
class RasterOperationTimings:
    """Elapsed phase times and effective input settings for one raster job.

    Phase times are non-overlapping wall times within a process. Across jobs,
    their sum represents worker time rather than elapsed batch duration.
    """

    read_seconds: float = 0.0
    mask_seconds: float = 0.0
    calculate_seconds: float = 0.0
    source_seconds: float = 0.0
    write_seconds: float = 0.0
    close_seconds: float = 0.0
    provenance_seconds: float = 0.0
    total_seconds: float = 0.0
    windows: int = 0
    width: int = 0
    height: int = 0
    input_count: int = 0
    block_shape: tuple[int, int] | None = None
    gdal_cache_bytes: int = 0
    gdal_threads: int = 0

    def phase_seconds(self) -> dict[str, float]:
        """Return stable human-readable phase names and their durations."""
        return {
            "read": self.read_seconds,
            "masks": self.mask_seconds,
            "calculate": self.calculate_seconds,
            "source": self.source_seconds,
            "write": self.write_seconds,
            "close": self.close_seconds,
            "provenance": self.provenance_seconds,
        }


@dataclass(slots=True, frozen=True)
class RasterExecutionPlan:
    """A CPU and memory bounded plan for one collection of independent jobs."""

    workers: int
    options: RasterExecutionOptions
    memory_budget_mb: int
    estimated_worker_mb: int
    requested_workers: int
    cpu_count: int


def plan_raster_execution(
    *,
    job_count: int,
    max_input_count: int,
    requested_workers: int | None = None,
    cache_mb: int = 128,
    chunk_mb: int = 256,
    memory_budget_mb: int | None = None,
    threads: int | None = None,
    vsi_cache_mb: int = 0,
    available_memory_bytes: int | None = None,
) -> RasterExecutionPlan:
    """Limit concurrent jobs using an explicit or available-memory budget.

    Automatic memory budgeting uses 75% of currently available RAM. The
    estimate includes block cache, chunk workspace, optional per-input VSI
    caches, 128 MiB process overhead and 8 MiB per compression thread. It is
    an admission estimate, not an operating-system limit on process RSS.
    ``max_input_count`` includes nested source rasters when they are read.
    """
    _require_integer(job_count, name="Raster job count")
    _require_integer(max_input_count, name="Raster input count")
    requested = 6 if requested_workers is None else _require_integer(requested_workers, name="Worker count")
    options = RasterExecutionOptions(cache_mb, chunk_mb, threads or 1, vsi_cache_mb)
    if threads is not None:
        _require_integer(threads, name="GDAL threads")
    if memory_budget_mb is None:
        available = (
            int(psutil.virtual_memory().available)
            if available_memory_bytes is None
            else _require_integer(available_memory_bytes, name="Available memory bytes")
        )
        memory_budget_mb = max(1, available * 3 // (4 * _MIB))
    else:
        _require_integer(memory_budget_mb, name="Total raster memory budget MiB")

    cpu_plan = plan_gdal_concurrency(job_count, requested_workers=requested)
    cpu_count = cpu_plan.cpu_count
    if threads is not None and threads > cpu_count:
        msg = f"GDAL threads ({threads}) exceed the detected logical CPU count ({cpu_count})"
        raise ValueError(msg)
    worker_count = min(job_count, requested, cpu_count // (threads or 1))
    base_memory_mb = options.cache_mb + options.chunk_mb + options.vsi_cache_mb * max_input_count + _PROCESS_OVERHEAD_MB

    # Re-evaluate the per-worker thread allowance after reducing concurrency.
    # A lower worker count can give each job more compression threads.
    while worker_count > 0:
        thread_plan = plan_gdal_concurrency(job_count, requested_workers=worker_count)
        planned_threads = threads or (
            cpu_count if thread_plan.threads_per_dataset == "ALL_CPUS" else int(thread_plan.threads_per_dataset)
        )
        if threads is None:
            affordable_threads = (memory_budget_mb // worker_count - base_memory_mb) // _COMPRESSION_THREAD_MB
            planned_threads = min(planned_threads, max(1, affordable_threads))
        estimated_worker_mb = base_memory_mb + _COMPRESSION_THREAD_MB * planned_threads
        if worker_count * estimated_worker_mb <= memory_budget_mb:
            return RasterExecutionPlan(
                workers=worker_count,
                options=RasterExecutionOptions(cache_mb, chunk_mb, planned_threads, vsi_cache_mb),
                memory_budget_mb=memory_budget_mb,
                estimated_worker_mb=estimated_worker_mb,
                requested_workers=requested,
                cpu_count=cpu_count,
            )
        worker_count -= 1

    msg = (
        f"Raster memory budget ({memory_budget_mb} MiB) cannot fit one worker; "
        "reduce cache/chunk/thread settings or increase the total memory budget"
    )
    raise ValueError(msg)
