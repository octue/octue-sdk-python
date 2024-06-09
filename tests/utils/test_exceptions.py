import unittest

from octue.utils.exceptions import convert_exception_to_primitives


class TestExceptions(unittest.TestCase):
    def test_with_caught_exception(self):
        """Test converting a caught exception (without passing the exception directly)."""
        try:
            raise ValueError("Deliberately raised for test.")
        except Exception:
            converted_exception = convert_exception_to_primitives()

        self.assertEqual(converted_exception["type"], "ValueError")
        self.assertEqual(converted_exception["message"], "Deliberately raised for test.")

        self.assertIn(
            'in test_with_raised_exception\n    raise ValueError("Deliberately raised for test.")',
            converted_exception["traceback"][0],
        )

    def test_with_passed_unraised_exception(self):
        """Test converting an unraised exception passed in to the function."""
        exception = ValueError("Deliberately raised for test.")
        converted_exception = convert_exception_to_primitives(exception)

        self.assertEqual(converted_exception["type"], "ValueError")
        self.assertEqual(converted_exception["message"], "Deliberately raised for test.")
        self.assertIn("in convert_exception_to_primitives\n    raise exception", converted_exception["traceback"][0])
