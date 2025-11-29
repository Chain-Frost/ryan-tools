from pathlib import Path
import unittest
from loguru import logger

# import sys
# ROOT: Path = Path(__file__).resolve().parents[2]
# if str(ROOT) not in sys.path:
#     sys.path.insert(0, str(ROOT))

from ryan_library.functions.data_processing import (
    check_string_TP,
    check_string_duration,
    check_string_aep,
    safe_apply,
)

# python -m unittest test_check_functions.py


class TestCheckFunctions(unittest.TestCase):

    def test_check_string_TP_valid(self):
        # Valid cases
        self.assertEqual(check_string_TP("TP01_somefile.csv"), "01")
        self.assertEqual(check_string_TP("example_TP12.csv"), "12")
        self.assertEqual(check_string_TP("_TP99+file.csv"), "99")
        self.assertEqual(check_string_TP("TP1_valid.csv"), "1")  # 1 digit is valid

    def test_check_string_TP_invalid(self):
        # Invalid cases
        with self.assertRaises(ValueError):
            check_string_TP("somefile.csv")  # No TP found
        with self.assertRaises(ValueError):
            check_string_TP("example_TP123.csv")  # Too many digits

    def test_check_string_duration_valid(self):
        # Valid cases
        self.assertEqual(check_string_duration("somefile_00360m.csv"), "00360")
        self.assertEqual(check_string_duration("example+00900m.csv"), "00900")
        self.assertEqual(check_string_duration("_010m+file.csv"), "010")

    def test_check_string_duration_invalid(self):
        # Invalid cases
        with self.assertRaises(ValueError):
            check_string_duration("no_duration_here.csv")
        with self.assertRaises(ValueError):
            check_string_duration("1234n_invalid.csv")  # Incorrect suffix

    def test_check_string_aep_valid(self):
        # Valid cases
        self.assertEqual(check_string_aep("example_01.00p_file.csv"), "01.00")
        self.assertEqual(check_string_aep("+05.50p_file.csv"), "05.50")
        self.assertEqual(check_string_aep("03.5p_+example.csv"), "03.5")

    def test_check_string_aep_invalid(self):
        # Invalid cases
        with self.assertRaises(ValueError):
            check_string_aep("no_aep_here.csv")
        with self.assertRaises(ValueError):
            check_string_aep("example_1.000p.csv")  # Incorrect format (too many digits)


def test_safe_apply_returns_value():
    """safe_apply should return the wrapped function's value when no exception occurs."""

    def double(value: int) -> int:
        return value * 2

    assert safe_apply(double, 5) == 10


def test_safe_apply_handles_exception(caplog):
    """safe_apply should swallow exceptions and emit a debug log message."""

    def divide_by(value: int) -> float:
        return 10 / value

    with caplog.at_level("DEBUG"):
        handler_id = logger.add(caplog.handler, level="DEBUG")
        try:
            result = safe_apply(divide_by, 0)
        finally:
            logger.remove(handler_id)

    assert result is None
    assert any("Error applying function" in message for message in caplog.messages)


if __name__ == "__main__":
    unittest.main()
