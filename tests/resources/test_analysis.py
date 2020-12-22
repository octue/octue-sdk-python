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

    def test_analysis_hash_attributes_are_none_when_no_relevant_strands(self):
        """ Ensures that the hash attributes of Analysis instances are None if none of the relevant strands are in the
        Twine. """
        analysis = Analysis(twine="{}")
        for strand_name in "configuration_values", "configuration_manifest", "input_values", "input_manifest":
            self.assertIsNone(getattr(analysis, f"{strand_name}_hash"))
