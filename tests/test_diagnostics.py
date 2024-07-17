import os
import unittest
from unittest.mock import patch

from octue.diagnostics import Diagnostics


class TestDiagnostics(unittest.TestCase):
    def test_instantiating_without_credentials(self):
        """Test that a `Diagnostics` instance can be created without Google Cloud credentials."""
        with patch.dict(os.environ, clear=True):
            Diagnostics(cloud_path="blah")
