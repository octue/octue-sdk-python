from tests.base import BaseTestCase

from octue.mixins import Filterable


class MyClass(Filterable):
    my_attribute = "This is a string."
    another_attribute = "Cat"


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

        class Hello:
            pass

        hello = Hello()
        hello.world = InnerClass()
        nested_attribute = Filterable._get_nested_attribute(hello, "world.surprise")
        self.assertEqual(nested_attribute, hello.world.surprise)

    def test_filter_names_are_built_correctly(self):
        """ Ensure the filter names are built correctly for each attribute to be filtered_by. """
        MyClass._ATTRIBUTES_TO_FILTER_BY = ("my_attribute", "another_attribute")
        my_class = MyClass()

        self.assertEqual(
            set(my_class._filters.keys()),
            {
                "my_attribute__icontains",
                "my_attribute__contains",
                "my_attribute__ends_with",
                "my_attribute__starts_with",
                "my_attribute__exact",
                "my_attribute__notnone",
                "another_attribute__icontains",
                "another_attribute__contains",
                "another_attribute__ends_with",
                "another_attribute__starts_with",
                "another_attribute__exact",
                "another_attribute__notnone",
            },
        )

    def test_only_attributes_specified_used_to_create_filters(self):
        """ Ensure only the attributes specified are used to create filters. """
        MyClass._ATTRIBUTES_TO_FILTER_BY = ("my_attribute",)
        my_class = MyClass()

        self.assertEqual(
            set(my_class._filters.keys()),
            {
                "my_attribute__icontains",
                "my_attribute__contains",
                "my_attribute__ends_with",
                "my_attribute__starts_with",
                "my_attribute__exact",
                "my_attribute__notnone",
            },
        )
