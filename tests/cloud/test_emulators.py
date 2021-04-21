import unittest

from octue.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier


class TestGoogleCloudStorageEmulatorTestResultModifier(unittest.TestCase):
    def test_multiple_emulators_can_be_created_at_once(self):
        """Test that multiple storage emulators can be created at once without error and that they are given different
        ports.
        """
        emulator_0 = GoogleCloudStorageEmulatorTestResultModifier()
        emulator_1 = GoogleCloudStorageEmulatorTestResultModifier()
        self.assertNotEqual(emulator_0.storage_emulator_host, emulator_1.storage_emulator_host)

    def test_multiple_emulators_can_start_at_once(self):
        """Test that multiple storage emulators can be created and started at once without error."""
        emulator_0 = GoogleCloudStorageEmulatorTestResultModifier()
        emulator_0.storage_emulator.start()
        emulator_1 = GoogleCloudStorageEmulatorTestResultModifier()
        emulator_1.storage_emulator.start()
        emulator_0.storage_emulator.stop()
        emulator_1.storage_emulator.stop()
