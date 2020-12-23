from tests.base import BaseTestCase

from octue.mixins import Filterable


class MyClass(Filterable):
    pass


class TestFilterable(BaseTestCase):
    def test_no_attributes_to_filter_by_results_in_error(self):
        """ Test that an AttributeError is raised if _ATTRIBUTES_TO_FILTER_BY is not specified. """
        with self.assertRaises(AttributeError):
            MyClass()

    def test_empty_tuple_of_attributes_to_filter_by_results_in_error(self):
        """ Test that an AttributeError is raised if _ATTRIBUTES_TO_FILTER_BY is empty. """
        MyClass._ATTRIBUTES_TO_FILTER_BY = tuple()

        with self.assertRaises(AttributeError):
            MyClass()

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
