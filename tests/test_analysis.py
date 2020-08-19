from .base import BaseTestCase


class AnalysisTestCase(BaseTestCase):
    """ Base test case for twined:
        - sets a path to the test data directory
    """

    def test_instantiate_analysis(self):
        """ Ensures that the base analysis class can be instantiated
        """
        from octue import analysis

        a = analysis
        self.assertEqual(a.__class__.__name__, "Analysis")

    def test_separate_in_separate_templates(self):
        pass
