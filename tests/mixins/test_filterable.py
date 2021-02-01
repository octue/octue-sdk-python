from octue import exceptions
from octue.mixins.filterable import Filterable
from octue.resources.tag import TagSet
from tests.base import BaseTestCase


class FilterableSubclass(Filterable):
    def __init__(self, name=None, is_alive=None, iterable=None, age=None, owner=None):
        self.name = name
        self.is_alive = is_alive
        self.iterable = iterable
        self.age = age
        self.owner = owner


class TestFilterable(BaseTestCase):
    def test_error_raised_when_invalid_filter_name_received(self):
        """ Ensure an error is raised when an invalid filter name is provided. """
        with self.assertRaises(exceptions.InvalidInputException):
            FilterableSubclass().satisfies(filter_name="invalid_filter_name", filter_value=None)

    def test_error_raised_when_non_existent_attribute_name_received(self):
        """ Ensure an error is raised when a non-existent attribute name is used in the filter name. """
        with self.assertRaises(AttributeError):
            FilterableSubclass().satisfies(filter_name="boogaloo__is_a_dance", filter_value=True)

    def test_error_raised_when_valid_but_non_existent_filter_name_received(self):
        """ Ensure an error is raised when a valid but non-existent filter name is received. """
        with self.assertRaises(exceptions.InvalidInputException):
            FilterableSubclass().satisfies(filter_name="age__is_secret", filter_value=True)

    def test_error_raised_when_attribute_type_has_no_filters_defined(self):
        """Ensure an error is raised when a filter for an attribute whose type doesn't have any filters defined is
        received.
        """
        with self.assertRaises(exceptions.InvalidInputException):
            FilterableSubclass(age=lambda: None).satisfies(filter_name="age__equals", filter_value=True)

    def test_bool_filters(self):
        """ Test that the boolean filters work as expected. """
        filterable_thing = FilterableSubclass(is_alive=True)
        self.assertTrue(filterable_thing.satisfies("is_alive__is", True))
        self.assertFalse(filterable_thing.satisfies("is_alive__is", False))
        self.assertTrue(filterable_thing.satisfies("is_alive__is_not", False))
        self.assertFalse(filterable_thing.satisfies("is_alive__is_not", True))

    def test_str_filters(self):
        """ Test that the string filters work as expected. """
        filterable_thing = FilterableSubclass(name="Michael")
        self.assertTrue(filterable_thing.satisfies("name__icontains", "m"))
        self.assertFalse(filterable_thing.satisfies("name__icontains", "d"))
        self.assertTrue(filterable_thing.satisfies("name__not_icontains", "d"))
        self.assertFalse(filterable_thing.satisfies("name__not_icontains", "m"))
        self.assertTrue(filterable_thing.satisfies("name__contains", "M"))
        self.assertFalse(filterable_thing.satisfies("name__contains", "d"))
        self.assertTrue(filterable_thing.satisfies("name__ends_with", "l"))
        self.assertFalse(filterable_thing.satisfies("name__ends_with", "M"))
        self.assertTrue(filterable_thing.satisfies("name__not_ends_with", "M"))
        self.assertFalse(filterable_thing.satisfies("name__not_ends_with", "l"))
        self.assertTrue(filterable_thing.satisfies("name__starts_with", "M"))
        self.assertFalse(filterable_thing.satisfies("name__starts_with", "l"))
        self.assertTrue(filterable_thing.satisfies("name__not_starts_with", "l"))
        self.assertFalse(filterable_thing.satisfies("name__not_starts_with", "M"))
        self.assertTrue(filterable_thing.satisfies("name__equals", "Michael"))
        self.assertFalse(filterable_thing.satisfies("name__equals", "Clive"))
        self.assertTrue(filterable_thing.satisfies("name__not_equals", "Clive"))
        self.assertFalse(filterable_thing.satisfies("name__not_equals", "Michael"))
        self.assertTrue(filterable_thing.satisfies("name__iequals", "michael"))
        self.assertFalse(filterable_thing.satisfies("name__iequals", "James"))
        self.assertTrue(filterable_thing.satisfies("name__not_iequals", "James"))
        self.assertFalse(filterable_thing.satisfies("name__not_iequals", "michael"))
        self.assertTrue(filterable_thing.satisfies("name__is", "Michael"))
        self.assertFalse(filterable_thing.satisfies("name__is", "Clive"))
        self.assertTrue(filterable_thing.satisfies("name__is_not", "Clive"))
        self.assertFalse(filterable_thing.satisfies("name__is_not", "Michael"))
        self.assertTrue(filterable_thing.satisfies("name__lt", "Noel"))
        self.assertFalse(filterable_thing.satisfies("name__lt", "Harry"))
        self.assertTrue(filterable_thing.satisfies("name__lte", "Michael"))
        self.assertFalse(filterable_thing.satisfies("name__lte", "Harry"))
        self.assertTrue(filterable_thing.satisfies("name__gt", "Clive"))
        self.assertFalse(filterable_thing.satisfies("name__gt", "Noel"))
        self.assertTrue(filterable_thing.satisfies("name__gte", "Michael"))
        self.assertFalse(filterable_thing.satisfies("name__gte", "Noel"))

    def test_none_filters(self):
        """ Test that the None filters work as expected. """
        filterable_thing = FilterableSubclass(owner=None)
        self.assertTrue(filterable_thing.satisfies("owner__is", None))
        self.assertFalse(filterable_thing.satisfies("owner__is", True))
        self.assertTrue(filterable_thing.satisfies("owner__is_not", True))
        self.assertFalse(filterable_thing.satisfies("owner__is_not", None))

    def test_number_filters_with_integers_and_floats(self):
        """ Test that the number filters work as expected for integers and floats. """
        for age in (5, 5.2):
            filterable_thing = FilterableSubclass(age=age)
            self.assertTrue(filterable_thing.satisfies("age__equals", age))
            self.assertFalse(filterable_thing.satisfies("age__equals", 63))
            self.assertTrue(filterable_thing.satisfies("age__not_equals", 63))
            self.assertFalse(filterable_thing.satisfies("age__not_equals", age))
            self.assertTrue(filterable_thing.satisfies("age__lt", 6))
            self.assertFalse(filterable_thing.satisfies("age__lt", 0))
            self.assertTrue(filterable_thing.satisfies("age__lte", age))
            self.assertFalse(filterable_thing.satisfies("age__lte", 0))
            self.assertTrue(filterable_thing.satisfies("age__gt", 4))
            self.assertFalse(filterable_thing.satisfies("age__gt", 63))
            self.assertTrue(filterable_thing.satisfies("age__gte", age))
            self.assertFalse(filterable_thing.satisfies("age__gte", 63))
            self.assertTrue(filterable_thing.satisfies("age__is", age))
            self.assertFalse(filterable_thing.satisfies("age__is", 63))
            self.assertTrue(filterable_thing.satisfies("age__is_not", 63))
            self.assertFalse(filterable_thing.satisfies("age__is_not", age))

    def test_iterable_filters(self):
        """ Test that the iterable filters work as expected with lists, sets, and tuples. """
        for iterable in ([1, 2, 3], {1, 2, 3}, (1, 2, 3)):
            filterable_thing = FilterableSubclass(iterable=iterable)
            self.assertTrue(filterable_thing.satisfies("iterable__contains", 1))
            self.assertFalse(filterable_thing.satisfies("iterable__contains", 5))
            self.assertTrue(filterable_thing.satisfies("iterable__not_contains", 5))
            self.assertFalse(filterable_thing.satisfies("iterable__not_contains", 1))
            self.assertTrue(filterable_thing.satisfies("iterable__is", iterable))
            self.assertFalse(filterable_thing.satisfies("iterable__is", None))
            self.assertTrue(filterable_thing.satisfies("iterable__is_not", None))
            self.assertFalse(filterable_thing.satisfies("iterable__is_not", iterable))

    def test_tag_set_filters(self):
        """ Test the filters for TagSet. """
        filterable_thing = FilterableSubclass(iterable=TagSet({"fred", "charlie"}))
        self.assertTrue(filterable_thing.satisfies("iterable__any_tag_contains", "a"))
        self.assertFalse(filterable_thing.satisfies("iterable__any_tag_contains", "z"))
        self.assertTrue(filterable_thing.satisfies("iterable__not_any_tag_contains", "z"))
        self.assertFalse(filterable_thing.satisfies("iterable__not_any_tag_contains", "a"))
        self.assertTrue(filterable_thing.satisfies("iterable__any_tag_starts_with", "f"))
        self.assertFalse(filterable_thing.satisfies("iterable__any_tag_starts_with", "e"))
        self.assertTrue(filterable_thing.satisfies("iterable__any_tag_ends_with", "e"))
        self.assertFalse(filterable_thing.satisfies("iterable__any_tag_ends_with", "i"))
        self.assertTrue(filterable_thing.satisfies("iterable__not_any_tag_starts_with", "e"))
        self.assertFalse(filterable_thing.satisfies("iterable__not_any_tag_starts_with", "f"))
        self.assertTrue(filterable_thing.satisfies("iterable__not_any_tag_ends_with", "i"))
        self.assertFalse(filterable_thing.satisfies("iterable__not_any_tag_ends_with", "e"))

    def test_filtering_different_attributes_on_same_instance(self):
        """ Ensure all filterable attributes on an instance can be checked for filter satisfaction. """
        filterable_thing = FilterableSubclass(name="Fred", is_alive=True, iterable={1, 2, 3}, age=5.2, owner=None)
        self.assertTrue(filterable_thing.satisfies("name__icontains", "f"))
        self.assertTrue(filterable_thing.satisfies("name__not_icontains", "j"))
        self.assertFalse(filterable_thing.satisfies("is_alive__is", False))
        self.assertTrue(filterable_thing.satisfies("iterable__contains", 3))
        self.assertTrue(filterable_thing.satisfies("age__equals", 5.2))
        self.assertTrue(filterable_thing.satisfies("age__not_equals", 5))
        self.assertTrue(filterable_thing.satisfies("owner__is", None))
