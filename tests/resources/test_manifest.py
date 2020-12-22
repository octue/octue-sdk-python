from tests.base import BaseTestCase


class TestManifest(BaseTestCase):
    def test_hash(self):
        """ Test hashing a manifest with multiple datasets gives a hash of length 128. """
        manifest = self.create_valid_manifest()
        hash_ = manifest.blake3_hash
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 64)
