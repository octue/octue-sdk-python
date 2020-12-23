from tests.base import BaseTestCase

from octue.mixins import Filterable


class TestFilterable(BaseTestCase):
    def test_get_nested_attribute(self):
        """ Test that a nested attribute (e.g. a.b.c) can be accessed dynamically via a single function call. """

        class InnerClass:
            surprise = "Hello, world!"

        class Hello(Filterable):
            pass

        hello = Hello()
        hello.world = InnerClass()
        nested_attribute = hello._get_nested_attribute("world.surprise")
        self.assertEqual(nested_attribute, hello.world.surprise)
