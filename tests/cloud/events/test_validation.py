import unittest

import jsonschema

from octue.cloud.events.validation import _get_validator, cached_validator, SERVICE_COMMUNICATION_SCHEMA


class TestGetValidator(unittest.TestCase):
    def test_cached_validator_returned_if_event_schema_is_official(self):
        """Test that the cached validator is returned if the official event schema is provided."""
        self.assertEqual(_get_validator(schema=SERVICE_COMMUNICATION_SCHEMA), cached_validator.validate)

    def test_uncached_validator_returned_if_custom_event_schema_provided(self):
        """Test that the uncached validator is returned if a custom event schema is provided."""
        validator = _get_validator(schema={})
        self.assertIs(validator.func, jsonschema.validate)
        self.assertEqual(validator.keywords, {"schema": {}})
