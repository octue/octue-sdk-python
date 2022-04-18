import logging
import os
import tempfile
from unittest.mock import patch

import twined.exceptions
from octue.cloud import storage
from octue.resources import Datafile, Dataset, Manifest
from octue.resources.analysis import HASH_FUNCTIONS, Analysis
from tests import TEST_BUCKET_NAME
from tests.base import BaseTestCase
from tests.mocks import mock_generate_signed_url
from twined import Twine


class AnalysisTestCase(BaseTestCase):
    def test_instantiate_analysis(self):
        """Ensures that the base analysis class can be instantiated"""
        # from octue import runner  # <-- instantiated in the library, not here
        analysis = Analysis(twine="{}")
        self.assertEqual(analysis.__class__.__name__, "Analysis")

    def test_instantiate_analysis_with_twine(self):
        """Ensures that the base analysis class can be instantiated"""
        analysis = Analysis(twine=Twine(source="{}"))
        self.assertEqual(analysis.__class__.__name__, "Analysis")

    def test_non_existent_attributes_cannot_be_retrieved(self):
        """Ensure attributes that don't exist on Analysis aren't retrieved as None and instead raise an error. See
        https://github.com/octue/octue-sdk-python/issues/45 for reasoning behind adding this.
        """
        analysis = Analysis(twine=Twine(source="{}"))

        with self.assertRaises(AttributeError):
            analysis.furry_purry_cat

    def test_analysis_hash_attributes_are_none_when_no_relevant_strands(self):
        """Ensures that the hash attributes of Analysis instances are None if none of the relevant strands are provided"""
        analysis = Analysis(twine="{}")
        for strand_name in HASH_FUNCTIONS:
            self.assertIsNone(getattr(analysis, f"{strand_name}_hash"))

    def test_analysis_hash_attributes_are_populated_when_relevant_strands_are_present(self):
        """Ensures that the hash attributes of Analysis instances are valid if the relevant strands are provided."""
        analysis = Analysis(
            twine="{}",
            configuration_values={"resistance_setting": 7},
            configuration_manifest=self.create_valid_manifest(),
            input_values={"quality_factor": 5},
            input_manifest=self.create_valid_manifest(),
        )

        for strand_name in HASH_FUNCTIONS:
            hash_ = getattr(analysis, f"{strand_name}_hash")
            self.assertTrue(isinstance(hash_, str))
            self.assertTrue(len(hash_) == 8)

    def test_warning_raised_if_attempting_to_send_a_monitoring_update_but_no_monitoring_callback_is_provided(self):
        """Test that a warning is raised if attempting to send a monitoring update but no monitoring callback is
        provided.
        """
        analysis = Analysis(twine='{"monitor_message_schema": {}}')

        with self.assertLogs(level=logging.WARNING) as logging_context:
            analysis.send_monitor_message(data=[])

        self.assertIn(
            "Attempted to send a monitor message but no handler is specified.",
            logging_context.output[0],
        )

    def test_error_raised_if_output_fails_to_validate_against_twine_in_finalise(self):
        """Test that an error is raised if the analysis output fails to validate against the twine."""
        analysis = Analysis(
            twine={
                "output_values_schema": {
                    "type": "object",
                    "properties": {"blah": {"type": "integer"}},
                    "required": ["blah"],
                }
            },
            output_values={"wrong": "hello"},
        )

        with self.assertRaises(twined.exceptions.InvalidValuesContents):
            analysis.finalise()

    def test_finalise_validates_output(self):
        """Test that the `finalise` method with no other arguments just validates the output manifest and values."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset_path = os.path.join(temporary_directory, "the_dataset")

            with Datafile(path=os.path.join(dataset_path, "my_file.dat"), mode="w") as (datafile, f):
                f.write("hello")

            output_manifest = Manifest(datasets={"the_dataset": Dataset(path=dataset_path, files={datafile.path})})

            analysis = Analysis(
                twine={
                    "output_values_schema": {"type": "object", "properties": {"blah": {"type": "integer"}}},
                    "output_manifest": {"datasets": {"the_dataset": {"purpose": "testing"}}},
                },
                output_values={"blah": 3},
                output_manifest=output_manifest,
            )

            analysis.finalise()

    def test_finalise_with_upload(self):
        """Test that the `finalise` method can be used to upload the output manifest's datasets to a cloud location
        and that it updates the manifest with signed URLs for accessing them.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset_path = os.path.join(temporary_directory, "the_dataset")

            with Datafile(path=os.path.join(dataset_path, "my_file.dat"), mode="w") as (datafile, f):
                f.write("hello")

            output_manifest = Manifest(
                datasets={
                    "the_dataset": Dataset(path=dataset_path, files={datafile.path}, labels={"one", "two", "three"})
                }
            )

            analysis = Analysis(
                twine={
                    "output_values_schema": {"type": "object", "properties": {"blah": {"type": "integer"}}},
                    "output_manifest": {"datasets": {"the_dataset": {"purpose": "testing"}}},
                },
                output_values={"blah": 3},
                output_manifest=output_manifest,
            )

            with patch("google.cloud.storage.blob.Blob.generate_signed_url", new=mock_generate_signed_url):
                analysis.finalise(upload_output_datasets_to=f"gs://{TEST_BUCKET_NAME}/datasets")

        signed_url_for_dataset = analysis.output_manifest.datasets["the_dataset"].path
        self.assertTrue(storage.path.is_url(signed_url_for_dataset))

        self.assertTrue(
            signed_url_for_dataset.startswith(
                f"{self.test_result_modifier.storage_emulator_host}/{TEST_BUCKET_NAME}/datasets/the_dataset"
            )
        )

        downloaded_dataset = Dataset.from_cloud(signed_url_for_dataset)
        self.assertEqual(downloaded_dataset.name, "the_dataset")
        self.assertEqual(len(downloaded_dataset.files), 1)
        self.assertEqual(downloaded_dataset.labels, {"one", "two", "three"})

        with downloaded_dataset.files.one() as (downloaded_datafile, f):
            self.assertEqual(f.read(), "hello")
