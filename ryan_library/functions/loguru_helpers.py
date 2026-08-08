"""Central Loguru configuration for serial and multiprocessing workflows.

Top-level wrappers and orchestrators own configuration. Reusable functions and
processors should only emit records through :data:`loguru.logger`.
"""

from __future__ import annotations

import atexit
import multiprocessing
import os
import pickle
import sys
import threading
import traceback
from multiprocessing import Process, Queue
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from loguru import logger

LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
CONSOLE_FORMAT = "<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {module}:{function}:{line} - {message}"
FILE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} - {message}"
FORWARDED_CONSOLE_FORMAT = "<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}"
FORWARDED_FILE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {message}"
NOTEBOOK_FORMAT = "<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
CONSOLE_COLORIZE = True
ROTATION = "10 MB"
RETENTION = "10 days"
COMPRESSION = "zip"
DIAGNOSE = False
LISTENER_SHUTDOWN_TIMEOUT_SECONDS = 5.0


if TYPE_CHECKING:
    from multiprocessing.queues import Queue as MPQueue


class SerializedLogRecord(TypedDict):
    """Minimal trusted-process payload sent from producers to the listener."""

    level: str
    message: str
    module: str
    function: str
    line: int
    exception: str | None


class LogQueue:
    """Picklable queue plus the minimum level required by downstream sinks."""

    def __init__(self, *, capture_log_level: str) -> None:
        self.capture_log_level: str = normalize_log_level(capture_log_level)
        self._queue: MPQueue[bytes | None] = cast("MPQueue[bytes | None]", Queue())

    def put(self, item: bytes | None) -> None:
        """Put a serialized record or shutdown sentinel on the queue."""

        self._queue.put(item)

    def get(self) -> bytes | None:
        """Return the next serialized record or shutdown sentinel."""

        return self._queue.get()

    def close(self) -> None:
        """Close the producer side of the queue."""

        self._queue.close()

    def join_thread(self) -> None:
        """Wait for the queue feeder thread to flush pending records."""

        self._queue.join_thread()


def normalize_log_level(level: str) -> str:
    """Return a canonical Loguru level name or raise a clear ``ValueError``."""

    normalized_level: str = level.strip().upper()
    if not normalized_level:
        raise ValueError("Log level must not be empty.")
    try:
        return logger.level(normalized_level).name
    except ValueError as exc:
        raise ValueError(f"Unknown Loguru level: {level!r}") from exc


def minimum_log_level(*levels: str) -> str:
    """Return the least restrictive of the supplied Loguru levels."""

    normalized_levels: list[str] = [normalize_log_level(level) for level in levels]
    if not normalized_levels:
        raise ValueError("At least one log level is required.")
    return min(normalized_levels, key=lambda level: logger.level(level).no)


def _add_console_sink(
    *,
    level: str,
    forwarded: bool,
    sink: Any | None = None,
    format_string: str | None = None,
) -> None:
    """Install the standard stdout sink."""

    logger.add(
        sink=sys.stdout if sink is None else sink,
        level=normalize_log_level(level),
        format=format_string or (FORWARDED_CONSOLE_FORMAT if forwarded else CONSOLE_FORMAT),
        colorize=CONSOLE_COLORIZE,
        backtrace=True,
        diagnose=DIAGNOSE,
        enqueue=False,
    )


def _add_file_sink(*, log_file: str, level: str, forwarded: bool) -> None:
    """Install the standard rotating file sink."""

    logger.add(
        sink=log_file,
        level=normalize_log_level(level),
        format=FORWARDED_FILE_FORMAT if forwarded else FILE_FORMAT,
        rotation=ROTATION,
        retention=RETENTION,
        compression=COMPRESSION,
        backtrace=True,
        diagnose=DIAGNOSE,
        enqueue=False,
    )


def worker_initializer(queue: LogQueue, level: str | None = None) -> None:
    """Configure a worker to send records to ``queue``.

    Args:
        queue: Queue returned by :func:`setup_logger`.
        level: Optional producer capture-level override. By default, use the
            lowest level required by the configured listener sinks.
    """

    worker_configurer(queue=queue, level=level)


def reset_logging() -> None:
    """Reset Loguru configuration by removing all sinks."""

    logger.remove()


def is_loguru_configured() -> bool:
    """Return whether Loguru has at least one sink configured."""

    handlers = getattr(getattr(logger, "_core", None), "handlers", {})
    return bool(handlers)


def configure_serial_logging(
    console_log_level: str = "INFO",
    log_file: str | None = None,
    file_log_level: str = "DEBUG",
) -> None:
    """Configure Loguru for a serial workflow.

    Args:
        console_log_level: Minimum console level.
        log_file: Optional file-sink path.
        file_log_level: Minimum file level when ``log_file`` is supplied.
    """

    if LoguruMultiprocessingLogger.has_active_context():
        raise RuntimeError("Cannot replace serial sinks while a multiprocessing logging context is active.")
    normalized_console_level: str = normalize_log_level(console_log_level)
    normalized_file_level: str = normalize_log_level(file_log_level)
    reset_logging()
    _add_console_sink(level=normalized_console_level, forwarded=False)
    if log_file:
        add_file_sink(log_file=log_file, file_log_level=normalized_file_level)


def configure_notebook_logging(
    console_log_level: str = "INFO",
    log_file: str | None = None,
    file_log_level: str = "DEBUG",
) -> None:
    """Configure a re-runnable, process-local logger for Jupyter/IPython.

    Re-running a notebook cell replaces the existing sinks, so output is never
    duplicated. Use ``console_log_level="SUCCESS"`` for low-volume AI/MCP
    consumption while retaining detailed records in ``log_file``.
    """

    if LoguruMultiprocessingLogger.has_active_context():
        raise RuntimeError("Cannot replace notebook sinks while a multiprocessing logging context is active.")
    normalized_console_level: str = normalize_log_level(console_log_level)
    normalized_file_level: str = normalize_log_level(file_log_level)
    reset_logging()
    _add_console_sink(
        level=normalized_console_level,
        forwarded=False,
        sink=sys.stderr,
        format_string=NOTEBOOK_FORMAT,
    )
    if log_file:
        add_file_sink(log_file=log_file, file_log_level=normalized_file_level)


def listener_process(
    queue: LogQueue,
    log_file: str | None = None,
    console_log_level: str = "INFO",
    file_log_level: str = "DEBUG",
) -> None:
    """Receive trusted worker records and render them through listener sinks."""

    reset_logging()
    if log_file:
        _add_file_sink(log_file=log_file, level=file_log_level, forwarded=True)
    _add_console_sink(level=console_log_level, forwarded=True)

    while True:
        try:
            queue_item: bytes | None = queue.get()
            if queue_item is None:
                break

            record: SerializedLogRecord = cast(SerializedLogRecord, pickle.loads(queue_item))
            formatted_message: str = f"{record['module']}:{record['function']}:{record['line']} - {record['message']}"
            exception_text: str | None = record["exception"]
            if exception_text:
                formatted_message = f"{formatted_message}\n{exception_text.rstrip()}"
            logger.log(record["level"], formatted_message)
        except Exception:
            logger.opt(exception=True).error("Error in logging listener")


def _format_exception(exception: Any) -> str | None:
    """Render a Loguru record exception into a picklable traceback string."""

    if exception is None:
        return None
    exception_type = getattr(exception, "type", None)
    exception_value = getattr(exception, "value", None)
    exception_traceback = getattr(exception, "traceback", None)
    if exception_type is None:
        return str(exception)
    return "".join(traceback.format_exception(exception_type, exception_value, exception_traceback))


def worker_configurer(queue: LogQueue, level: str | None = None) -> None:
    """Configure the current process to serialize records to ``queue``."""

    capture_level: str = normalize_log_level(level or queue.capture_log_level)

    class QueueSink:
        """Serialize the stable subset of each Loguru record."""

        def __init__(self, log_queue: LogQueue) -> None:
            self.log_queue: LogQueue = log_queue

        def write(self, message: Any) -> None:
            try:
                record: dict[str, Any] = cast(dict[str, Any], message.record)
                level_object: Any = record["level"]
                payload: SerializedLogRecord = {
                    "level": str(level_object.name),
                    "message": str(record["message"]),
                    "module": str(record["module"]),
                    "function": str(record["function"]),
                    "line": int(record["line"]),
                    "exception": _format_exception(record.get("exception")),
                }
                self.log_queue.put(pickle.dumps(payload))
            except Exception:
                sys.stderr.write("Failed to send log message to listener.\n")

        def flush(self) -> None:
            """Satisfy Loguru's file-like sink protocol."""

    reset_logging()
    logger.add(
        sink=QueueSink(log_queue=queue),
        level=capture_level,
        format="{message}",
        backtrace=True,
        diagnose=DIAGNOSE,
    )


class LoguruMultiprocessingLogger:
    """Own one process-wide listener and producer queue for a workflow."""

    _active_context: ClassVar[LoguruMultiprocessingLogger | None] = None
    _context_lock: ClassVar[Any] = threading.Lock()

    @classmethod
    def has_active_context(cls) -> bool:
        """Return whether this process currently owns a listener context."""

        with cls._context_lock:
            return cls._active_context is not None

    def __init__(
        self,
        log_file: str | None = None,
        console_log_level: str = "INFO",
        file_log_level: str = "DEBUG",
    ) -> None:
        self.log_file: str | None = log_file
        self.console_log_level: str = normalize_log_level(console_log_level)
        self.file_log_level: str = normalize_log_level(file_log_level)
        active_levels: list[str] = [self.console_log_level]
        if self.log_file:
            active_levels.append(self.file_log_level)
        self.capture_log_level: str = minimum_log_level(*active_levels)
        self.queue: LogQueue | None = None
        self.listener: Process | None = None
        self._entered: bool = False
        self._shutdown: bool = False
        self._atexit_callback: Any = self.shutdown

    def __enter__(self) -> LogQueue:
        with self._context_lock:
            if self._entered or self._shutdown:
                raise RuntimeError("This logging context cannot be entered more than once.")
            if type(self)._active_context is not None:
                raise RuntimeError("A multiprocessing logging context is already active in this process.")
            type(self)._active_context = self
            self._entered = True

        try:
            self.queue = LogQueue(capture_log_level=self.capture_log_level)
            self.listener = Process(
                target=listener_process,
                args=(self.queue, self.log_file, self.console_log_level, self.file_log_level),
            )
            self.listener.start()
            worker_configurer(queue=self.queue)
            atexit.register(self._atexit_callback)
            return self.queue
        except BaseException:
            self._release_active_context()
            self._close_queue()
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.shutdown()

    def _release_active_context(self) -> None:
        with self._context_lock:
            if type(self)._active_context is self:
                type(self)._active_context = None

    def _close_queue(self) -> None:
        if self.queue is None:
            return
        self.queue.close()
        self.queue.join_thread()
        self.queue = None

    def shutdown(self) -> None:
        """Drain and close logging resources; repeated calls are harmless."""

        if self._shutdown:
            return
        self._shutdown = True
        try:
            atexit.unregister(self._atexit_callback)
            if self.listener is not None and self.listener.is_alive() and self.queue is not None:
                self.queue.put(None)
                self.listener.join(timeout=LISTENER_SHUTDOWN_TIMEOUT_SECONDS)
                if self.listener.is_alive():
                    sys.stderr.write(
                        "Logging listener did not stop cleanly; terminating it and some queued records may be lost.\n"
                    )
                    self.listener.terminate()
                    self.listener.join(timeout=LISTENER_SHUTDOWN_TIMEOUT_SECONDS)
            self.listener = None
            reset_logging()
            self._close_queue()
        finally:
            self._release_active_context()


def setup_logger(
    console_log_level: str = "INFO",
    log_file: str | None = None,
    file_log_level: str = "DEBUG",
) -> LoguruMultiprocessingLogger:
    """Return a multiprocessing logging context with independent sink levels."""

    if log_file and not os.path.isabs(log_file):
        log_file = os.path.join(os.getcwd(), log_file)
    return LoguruMultiprocessingLogger(
        log_file=log_file,
        console_log_level=console_log_level,
        file_log_level=file_log_level,
    )


def add_file_sink(log_file: str, file_log_level: str = "DEBUG") -> None:
    """Add a standard rotating file sink outside a multiprocessing context."""

    if not os.path.isabs(log_file):
        log_file = os.path.join(os.getcwd(), log_file)
    _add_file_sink(log_file=log_file, level=file_log_level, forwarded=False)


def log_exception(err: str | None) -> None:
    """Log the current exception with a stack trace."""

    message: str = "An exception occurred" + (err or "")
    logger.exception(message)


class LoggerManager:
    """Backward-compatible singleton around :class:`LoguruMultiprocessingLogger`."""

    _instance: ClassVar[LoggerManager | None] = None
    _lock: ClassVar[Any] = multiprocessing.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> LoggerManager:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        log_level: str = "INFO",
        log_file: str | None = "app.log",
        log_dir: Path | None = None,
        max_bytes: int = 10**6,
        backup_count: int = 5,
        enable_color: bool = True,
        additional_sinks: list[Any] | None = None,
    ) -> None:
        del max_bytes, backup_count, enable_color, additional_sinks
        if getattr(self, "_initialized", False):
            return

        resolved_log_path: str | None = None
        if log_file:
            resolved_log_path = str(Path(log_dir or os.getcwd()) / log_file)
        self._logger_context: LoguruMultiprocessingLogger | None = setup_logger(
            console_log_level=log_level,
            log_file=resolved_log_path,
        )
        self._log_queue: LogQueue = self._logger_context.__enter__()
        self._listener: Process | None = self._logger_context.listener
        self._initialized: bool = True

    def shutdown(self) -> None:
        """Shut down the owned context and permit later reinitialization."""

        if self._logger_context is not None:
            self._logger_context.shutdown()
            self._logger_context = None
        self._listener = None
        self._initialized = False


def worker_process(log_queue: LogQueue) -> None:
    """Compatibility wrapper that configures logging in a worker process."""

    worker_configurer(log_queue)
