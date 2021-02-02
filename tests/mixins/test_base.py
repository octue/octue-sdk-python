from octue.mixins import MixinBase
from ..base import BaseTestCase


class MixinBaseTestCase(BaseTestCase):
    def test_instantiates_with_no_args(self):
        """Ensures the class instantiates without arguments"""
        MixinBase()

    def test_raises_exception_if_passed_args(self):
        """Ensures that the base mixin won't silently fail if passed arguments it's not supposed to have"""
        with self.assertRaises(TypeError):
            MixinBase("an_argument")

        with self.assertRaises(TypeError):
            MixinBase(an_argument="an_argument")
