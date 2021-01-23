from tests.base import BaseTestCase

from octue.mixins import CoolNameable


class TestCoolNameable(BaseTestCase):
    def test_name_has_two_components(self):
        """ Test that the cool name is in the format of <word>-<word>. """
        cool_nameable = CoolNameable()
        self.assertEqual(len(cool_nameable.cool_name.split("-")), 2)

    def test_names_are_different(self):
        """ Test that the cool names of two different instances are different. """
        cool_nameables = {CoolNameable().cool_name for _ in range(5)}
        self.assertEqual(len(cool_nameables), 5)
