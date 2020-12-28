from tests.base import BaseTestCase

from octue.mixins import Filterable


class MyClass(Filterable):
    my_attribute = "This is a string."
    another_attribute = "Cat"


class TestFilterable(BaseTestCase):
    def test_no_filterable_attributes_results_in_error(self):
        """ Test that an AttributeError is raised if _FILTERABLE_ATTRIBUTES is not specified. """

        class ClassWithNoFilterableAttributes(Filterable):
            pass

        with self.assertRaises(AttributeError):
            ClassWithNoFilterableAttributes()

    def test_empty_tuple_of_filterable_attributes_results_in_error(self):
        """ Test that an AttributeError is raised if _FILTERABLE_ATTRIBUTES is empty. """

        class ClassWithEmptyFilterableAttributes(Filterable):
            _FILTERABLE_ATTRIBUTES = tuple()

        with self.assertRaises(AttributeError):
            ClassWithEmptyFilterableAttributes()

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
        MyClass._FILTERABLE_ATTRIBUTES = ("my_attribute", "another_attribute")
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
        MyClass._FILTERABLE_ATTRIBUTES = ("my_attribute",)
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

    def test_filter(self):
        class ClassToFilter(Filterable):
            _FILTERABLE_ATTRIBUTES = ("a", "b")

            def __init__(self, a, b, *args, **kwargs):
                self.a = a
                self.b = b
                super().__init__(*args, **kwargs)

        instance_to_filter = ClassToFilter(a={1, 2, 3}, b=["filter", "this", "up"])
        filtered_instance = instance_to_filter.filter("b__contains", "p")
        self.assertEqual(filtered_instance.a, {1, 2, 3})
        self.assertEqual(filtered_instance.b, ["up"])
