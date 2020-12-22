from tests.base import BaseTestCase


class TestManifest(BaseTestCase):
    def test_hash(self):
        """ Test hashing a manifest with multiple datasets gives a hash of length 128. """
        manifest = self.create_valid_manifest()
        hash = manifest.blake2b_hash
        self.assertTrue(isinstance(hash, str))
        self.assertTrue(len(hash) == 128)
