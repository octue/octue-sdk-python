from tests.base import BaseTestCase

from octue import exceptions
from octue.mixins.filteree import Filteree
from octue.resources.tag import TagGroup


class FiltereeSubclass(Filteree):
    def __init__(self, name=None, is_alive=None, iterable=None, age=None, owner=None):
        self.name = name
        self.is_alive = is_alive
        self.iterable = iterable
        self.age = age
        self.owner = owner


class TestFilteree(BaseTestCase):
    def test_error_raised_when_invalid_filter_name_received(self):
        """ Ensure an error is raised when an invalid filter name is provided. """
        with self.assertRaises(exceptions.InvalidInputException):
            FiltereeSubclass().satisfies(filter_name="invalid_filter_name", filter_value=None)

    def test_error_raised_when_valid_but_non_existent_filter_name_received(self):
        """ Ensure an error is raised when a valid but non-existent filter name is received. """
        with self.assertRaises(exceptions.InvalidInputException):
            FiltereeSubclass().satisfies(filter_name="age__is_secret", filter_value=True)

    def test_error_raised_when_attribute_type_has_no_filters_defined(self):
        """ Ensure an error is raised when a filter for an attribute whose type doesn't have any filters defined is
        received.
        """
        with self.assertRaises(exceptions.InvalidInputException):
            FiltereeSubclass(age=lambda: None).satisfies(filter_name="age__equals", filter_value=True)

    def test_bool_filters(self):
        """ Test that the boolean filters work as expected. """
        filterable_thing = FiltereeSubclass(is_alive=True)
        self.assertTrue(filterable_thing.satisfies("is_alive__is", True))
        self.assertFalse(filterable_thing.satisfies("is_alive__is", False))
        self.assertTrue(filterable_thing.satisfies("is_alive__is_not", False))
        self.assertFalse(filterable_thing.satisfies("is_alive__is_not", True))

    def test_str_filters(self):
        """ Test that the string filters work as expected. """
        filterable_thing = FiltereeSubclass(name="Michael")
        self.assertTrue(filterable_thing.satisfies("name__icontains", "m"))
        self.assertFalse(filterable_thing.satisfies("name__icontains", "d"))
        self.assertTrue(filterable_thing.satisfies("name__contains", "M"))
        self.assertFalse(filterable_thing.satisfies("name__contains", "d"))
        self.assertTrue(filterable_thing.satisfies("name__ends_with", "l"))
        self.assertFalse(filterable_thing.satisfies("name__ends_with", "M"))
        self.assertTrue(filterable_thing.satisfies("name__starts_with", "M"))
        self.assertFalse(filterable_thing.satisfies("name__starts_with", "l"))
        self.assertTrue(filterable_thing.satisfies("name__equals", "Michael"))
        self.assertFalse(filterable_thing.satisfies("name__equals", "Clive"))
        self.assertTrue(filterable_thing.satisfies("name__is", "Michael"))
        self.assertFalse(filterable_thing.satisfies("name__is", "Clive"))
        self.assertTrue(filterable_thing.satisfies("name__is_not", "Clive"))
        self.assertFalse(filterable_thing.satisfies("name__is_not", "Michael"))

    def test_none_filters(self):
        """ Test that the None filters work as expected. """
        filterable_thing = FiltereeSubclass(owner=None)
        self.assertTrue(filterable_thing.satisfies("owner__is", None))
        self.assertFalse(filterable_thing.satisfies("owner__is", True))
        self.assertTrue(filterable_thing.satisfies("owner__is_not", True))
        self.assertFalse(filterable_thing.satisfies("owner__is_not", None))

    def test_number_filters_with_integers_and_floats(self):
        """ Test that the number filters work as expected for integers and floats. """
        for age in (5, 5.2):
            filterable_thing = FiltereeSubclass(age=age)
            self.assertTrue(filterable_thing.satisfies("age__equals", age))
            self.assertFalse(filterable_thing.satisfies("age__equals", 63))
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
            filterable_thing = FiltereeSubclass(iterable=iterable)
            self.assertTrue(filterable_thing.satisfies("iterable__contains", 1))
            self.assertFalse(filterable_thing.satisfies("iterable__contains", 5))
            self.assertTrue(filterable_thing.satisfies("iterable__not_contains", 5))
            self.assertFalse(filterable_thing.satisfies("iterable__not_contains", 1))
            self.assertTrue(filterable_thing.satisfies("iterable__is", iterable))
            self.assertFalse(filterable_thing.satisfies("iterable__is", None))
            self.assertTrue(filterable_thing.satisfies("iterable__is_not", None))
            self.assertFalse(filterable_thing.satisfies("iterable__is_not", iterable))

    def test_tag_group_filters(self):
        """ Test the filters for TagGroup. """
        filterable_thing = FiltereeSubclass(iterable=TagGroup({"fred", "charlie"}))
        self.assertTrue(filterable_thing.satisfies("iterable__starts_with", "f"))
        self.assertFalse(filterable_thing.satisfies("iterable__starts_with", "e"))
        self.assertTrue(filterable_thing.satisfies("iterable__ends_with", "e"))
        self.assertFalse(filterable_thing.satisfies("iterable__ends_with", "i"))

    def test_filtering_different_attributes_on_same_instance(self):
        """ Ensure all filterable attributes on an instance can be checked for filter satisfaction. """
        filterable_thing = FiltereeSubclass(name="Fred", is_alive=True, iterable={1, 2, 3}, age=5.2, owner=None)
        self.assertTrue(filterable_thing.satisfies("name__icontains", "f"))
        self.assertFalse(filterable_thing.satisfies("is_alive__is", False))
        self.assertTrue(filterable_thing.satisfies("iterable__contains", 3))
        self.assertTrue(filterable_thing.satisfies("age__equals", 5.2))
        self.assertTrue(filterable_thing.satisfies("owner__is", None))
