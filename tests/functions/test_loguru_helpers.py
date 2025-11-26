# tests/functions/test_loguru_helpers.py

import unittest
from ryan_library.functions.loguru_helpers import LoggerManager, worker_process
from pathlib import Path
import multiprocessing
import time
import tempfile
import shutil


def worker_func(log_queue) -> None:
    from loguru import logger
    import pickle

    logger.remove()
    # We need to match the serialization expected by the listener
    # The listener expects a pickled record dict
    logger.add(lambda message: log_queue.put(pickle.dumps(message.record)), format="{message}")
    logger.info("Test log from worker.")


class TestLoggerManager(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir: str = tempfile.mkdtemp()
        self.log_dir = Path(self.test_dir)

        # Reset singleton for testing
        LoggerManager._instance = None

        self.logger_manager = LoggerManager(
            log_level="DEBUG",
            log_file="test.log",
            log_dir=self.log_dir,
            max_bytes=10**5,  # 100 KB
            backup_count=2,
            enable_color=False,
        )
        self.log_queue = self.logger_manager._log_queue

    def tearDown(self) -> None:
        if self.logger_manager:
            self.logger_manager.shutdown()

        # Clean up log files
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_singleton(self) -> None:
        second_instance = LoggerManager()
        self.assertIs(self.logger_manager, second_instance)

    def test_logging(self) -> None:
        worker = multiprocessing.Process(target=worker_func, args=(self.log_queue,))
        worker.start()
        worker.join()

        # Allow some time for the listener to process the log
        time.sleep(2)

        # Verify that the log was received
        # Note: The listener consumes the queue, so we can't easily check the queue directly
        # unless we intercept it. But we can check if the log file was created and contains the message.

        log_file: Path = self.log_dir / "test.log"
        self.assertTrue(log_file.exists())

        content: str = log_file.read_text()
        self.assertIn(member="Test log from worker.", container=content)

    def test_shutdown(self) -> None:
        self.logger_manager.shutdown()
        # Wait a bit for the process to actually terminate
        time.sleep(1)
        if self.logger_manager._listener:
            self.assertFalse(self.logger_manager._listener.is_alive())


if __name__ == "__main__":
    unittest.main()
