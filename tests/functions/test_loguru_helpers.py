# tests/functions/test_loguru_helpers.py

import unittest
from ryan_library.functions.loguru_helpers import LoggerManager, worker_process
from pathlib import Path
import multiprocessing
import time


class TestLoggerManager(unittest.TestCase):
    def setUp(self):
        self.log_dir = Path("/tmp/test_logs")
        self.logger_manager = LoggerManager(
            log_level="DEBUG",
            log_file="test.log",
            log_dir=self.log_dir,
            max_bytes=10**5,  # 100 KB
            backup_count=2,
            enable_color=False,
        )
        self.log_queue = self.logger_manager._log_queue

    def tearDown(self):
        self.logger_manager.shutdown()
        # Clean up log files
        for file in self.log_dir.glob("*"):
            file.unlink()
        self.log_dir.rmdir()

    def test_singleton(self):
        second_instance = LoggerManager()
        self.assertIs(self.logger_manager, second_instance)

    def test_logging(self):
        # Start a worker process
        def test_worker(log_queue):
            from loguru import logger

            logger.remove()
            logger.add(lambda record: log_queue.put(record), serialize=True)
            logger.info("Test log from worker.")

        worker = multiprocessing.Process(target=test_worker, args=(self.log_queue,))
        worker.start()
        worker.join()

        # Allow some time for the listener to process the log
        time.sleep(1)

        # Verify that the log was received
        record = self.log_queue.get_nowait()
        self.assertEqual(record["message"], "Test log from worker.")
        self.assertEqual(record["level"]["name"], "INFO")

    def test_shutdown(self):
        self.logger_manager.shutdown()
        self.assertFalse(self.logger_manager._listener.is_alive())


if __name__ == "__main__":
    unittest.main()
