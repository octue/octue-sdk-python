import copy
from tests.base import BaseTestCase


class TestManifest(BaseTestCase):
    def test_blake3_hash(self):
        """ Test hashing a manifest with multiple datasets gives a hash of length 128. """
        manifest = self.create_valid_manifest()
        hash_ = manifest.blake3_hash
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 64)

    def test_blake3_hashes_for_the_same_manifest_are_the_same(self):
        """ Ensure the hashes for two manifests that are exactly the same are the same."""
        first_file = self.create_valid_manifest()
        second_file = copy.deepcopy(first_file)
        self.assertEqual(first_file.blake3_hash, second_file.blake3_hash)
