from tests.base import BaseTestCase

from octue import exceptions
from octue.mixins.filteree import Filteree


class FiltereeSubclass(Filteree):
    _FILTERABLE_ATTRIBUTES = ("name", "is_alive", "iterable", "age", "owner")

    def __init__(self, name=None, is_alive=None, iterable=None, age=None, owner=None):
        self.name = name
        self.is_alive = is_alive
        self.iterable = iterable
        self.age = age
        self.owner = owner


class TestFilteree(BaseTestCase):
    def test_error_raised_when_no_filterable_attributes(self):
        """ Ensure an error is raised if attempting to check attributes when no filterable attributes have been defined
        in the Filteree class.
        """
        filteree = Filteree()
        filteree.name = "Fred"

        with self.assertRaises(ValueError):
            filteree.satisfies("name__contains", "F")

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

    def test_check_attribute(self):
        """ Ensure filterable attributes can be checked for filter satisfaction. """
        filterable_thing = FiltereeSubclass(name="Fred", is_alive=True, iterable={1, 2, 3}, age=5.2, owner=None)
        self.assertTrue(filterable_thing.satisfies("name__icontains", "f"))
        self.assertFalse(filterable_thing.satisfies("is_alive__is", False))
        self.assertTrue(filterable_thing.satisfies("iterable__contains", 3))
        self.assertTrue(filterable_thing.satisfies("age__equals", 5.2))
        self.assertTrue(filterable_thing.satisfies("owner__is_none", True))
