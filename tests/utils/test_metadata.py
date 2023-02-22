import json
import logging
import os
import tempfile
from unittest.mock import patch

from octue.utils.metadata import cached_local_metadata_files, load_local_metadata_file, overwrite_local_metadata_file
from tests.base import BaseTestCase


class TestMetadata(BaseTestCase):
    def test_warning_raised_if_local_metadata_file_corrupted(self):
        """Test that a warning is raised and an emptry dictionary is returned if trying to load a corrupted local
        metadata file (e.g. not in JSON format).
        """
        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                f.write("some gobbledeegook")

            with self.assertLogs(level=logging.WARNING) as logging_context:
                local_metadata = load_local_metadata_file(temporary_file.name)

        self.assertEqual(local_metadata, {})

        self.assertIn(
            "is incorrectly formatted so no metadata can be read from it. Please fix or delete it.",
            logging_context.records[0].message,
        )

    def test_empty_dictionary_returned_if_local_metadata_file_does_not_exist(self):
        """Test that an empty dictionary is returned if trying to load a local metadata file that doesn't exist."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            self.assertEqual(load_local_metadata_file(path=os.path.join(temporary_directory, ".octue")), {})

    def test_local_metadata_is_cached_once_loaded_in_python_session(self):
        """Test that, if a local metadata file has been loaded once during the python session, it is loaded from the
        cache instead of from disk for the rest of the session.
        """
        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                json.dump({"some": "data"}, f)

            # Load the metadata file once and check its contents have been cached.
            load_local_metadata_file(temporary_file.name)
            self.assertEqual(cached_local_metadata_files[temporary_file.name], {"some": "data"})

            # Check that it's not loaded from disk again.
            with patch("builtins.open") as mock_open:
                local_metadata = load_local_metadata_file(temporary_file.name)

        mock_open.assert_not_called()
        self.assertEqual(local_metadata, {"some": "data"})

    def test_local_metadata_is_cached_if_already_written_to_in_python_session(self):
        """Test that, if a local metadata file has been written to during the python session, it is loaded from the
        cache instead of from disk for the rest of the session.
        """
        with tempfile.NamedTemporaryFile() as temporary_file:
            # Write the metadata file and check its contents have been cached.
            overwrite_local_metadata_file(data={"some": "data"}, path=temporary_file.name)
            self.assertEqual(cached_local_metadata_files[temporary_file.name], {"some": "data"})

            with open(temporary_file.name) as f:
                self.assertEqual(json.load(f), {"some": "data"})

            with patch("builtins.open") as mock_open:
                local_metadata = load_local_metadata_file(temporary_file.name)

        mock_open.assert_not_called()
        self.assertEqual(local_metadata, {"some": "data"})

    def test_cache_not_busted_if_overwriting_with_same_data(self):
        """Test that the cache is not busted and the local metadata file is not rewritten if trying to overwrite it with
        the same data as is in the cache.
        """
        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                json.dump({"some": "data"}, f)

            # Load the metadata file once and check its contents have been cached.
            load_local_metadata_file(temporary_file.name)
            self.assertEqual(cached_local_metadata_files[temporary_file.name], {"some": "data"})

            # Overwrite the metadata file with the same data.
            with patch("builtins.open") as mock_open:
                overwrite_local_metadata_file({"some": "data"}, path=temporary_file.name)

        mock_open.assert_not_called()

    def test_cache_busted_if_overwriting_with_new_data(self):
        """Test that the cache is busted and the local metadata file is rewritten if trying to overwrite it with
        data different from what's in the cache.
        """
        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                json.dump({"some": "data"}, f)

            # Load the metadata file once and check its contents have been cached.
            load_local_metadata_file(temporary_file.name)
            self.assertEqual(cached_local_metadata_files[temporary_file.name], {"some": "data"})

            with patch("builtins.open") as mock_open:
                overwrite_local_metadata_file({"new": "information"}, path=temporary_file.name)

        mock_open.assert_called_once()
