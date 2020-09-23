import os

from octue import exceptions
from octue.resources import Analysis
from twined import Twine
from .base import BaseTestCase


class AnalysisTestCase(BaseTestCase):
    """ Tests the Analysis class
    """

    def test_instantiate_analysis(self):
        """ Ensures that the base analysis class can be instantiated
        """
        # from octue import runner  # <-- instantiated in the library, not here
        analysis = Analysis(twine="{}")
        self.assertEqual(analysis.__class__.__name__, "Analysis")

    def test_instantiate_analysis_with_twine(self):
        """ Ensures that the base analysis class can be instantiated
        """
        analysis = Analysis(twine=Twine(source="{}"))
        self.assertEqual(analysis.__class__.__name__, "Analysis")

    def test_protected_setter(self):
        """ Ensures that protected attributes can't be set
        """
        analysis = Analysis(twine="{}")
        with self.assertRaises(exceptions.ProtectedAttributeException) as error:
            analysis.configuration_values = {}

        self.assertIn("You cannot set configuration_values on an instantiated Analysis", error.exception.args[0])

    def test_protected_getter(self):
        """ Ensures that protected attributes can't be set
        """
        analysis = Analysis(
            twine=str(os.path.join(self.data_path, "twines", "valid_schema_twine.json")),
            configuration_values={"n_iterations": 5},
            input_values={"height": 5},
            output_values={},
        )
        cfg = analysis.configuration_values
        self.assertIn("n_iterations", cfg.keys())

    def test_exception_raised_when_strand_data_missing(self):
        """ Ensures that protected attributes can't be set
        """
        with self.assertRaises(exceptions.InvalidInputException) as error:
            Analysis(
                twine=str(os.path.join(self.data_path, "twines", "valid_schema_twine.json")),
                configuration_values={"n_iterations": 5},
            )

        self.assertIn(
            "The input_values strand is defined in the twine, but no data is provided to Analysis()",
            error.exception.args[0],
        )

    def test_exception_raised_when_strand_missing(self):
        """ Ensures that protected attributes can't be set
        """
        with self.assertRaises(exceptions.InvalidInputException) as error:
            Analysis(
                twine="{}", configuration_values={},
            )

        self.assertIn(
            "Data is provided for configuration_values but no such strand is defined in the twine",
            error.exception.args[0],
        )
