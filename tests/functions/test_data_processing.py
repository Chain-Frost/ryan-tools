import unittest
from ryan_library.functions.data_processing import (
    check_string_TP,
    check_string_duration,
    check_string_aep,
)

# python -m unittest test_check_functions.py


class TestCheckFunctions(unittest.TestCase):

    def test_check_string_TP_valid(self):
        # Valid cases
        self.assertEqual(check_string_TP("TP01_somefile.csv"), "01")
        self.assertEqual(check_string_TP("example_TP12.csv"), "12")
        self.assertEqual(check_string_TP("_TP99+file.csv"), "99")

    def test_check_string_TP_invalid(self):
        # Invalid cases
        with self.assertRaises(ValueError):
            check_string_TP("TP1_invalid.csv")  # Only 1 digit
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


if __name__ == "__main__":
    unittest.main()
