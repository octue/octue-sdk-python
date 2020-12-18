from octue.resources import Analysis
from twined import Twine
from ..base import BaseTestCase


class AnalysisTestCase(BaseTestCase):
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

    def test_non_existent_attributes_cannot_be_retrieved(self):
        """ Ensure attributes that don't exist on Analysis aren't retrieved as None and instead raise an error. See
        https://github.com/octue/octue-sdk-python/issues/45 for reasoning behind adding this.
        """
        analysis = Analysis(twine=Twine(source="{}"))

        with self.assertRaises(AttributeError):
            analysis.furry_purry_cat
