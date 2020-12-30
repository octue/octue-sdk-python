from tests.base import BaseTestCase

from octue import exceptions
from octue.mixins.filteree import Filteree


class FiltereeSubclass(Filteree):
    _FILTERABLE_ATTRIBUTES = ("cat",)
    cat = "cat"


class TestFilteree(BaseTestCase):
    def test_error_raised_when_no_filterable_attributes(self):
        """ Ensure an error is raised if attempting to check attributes when no filterable attributes have been defined
        in the Filteree class.
        """
        with self.assertRaises(ValueError):
            Filteree().check_attribute(None, None)

    def test_error_raised_when_invalid_filter_name_received(self):
        """ Ensure an error is raised when an invalid filter name is provided. """
        with self.assertRaises(exceptions.InvalidInputException):
            FiltereeSubclass().check_attribute(filter_name="invalid_filter_name", filter_value=None)

    def test_error_raised_when_valid_but_non_existent_filter_name_received(self):
        """ Ensure an error is raised when a valid but non-existent filter name is received. """
        with self.assertRaises(exceptions.InvalidInputException):
            FiltereeSubclass().check_attribute(filter_name="cat__out_of_the_bag", filter_value=True)

    def test_error_raised_when_attribute_type_has_no_filters_defined(self):
        """ Ensure an error is raised when a filter for an attribute whose type doesn't have any filters defined is
        received.
        """
        filteree_subclass = FiltereeSubclass()
        filteree_subclass.cat = lambda: None

        with self.assertRaises(exceptions.InvalidInputException):
            filteree_subclass.check_attribute(filter_name="cat__contains", filter_value=True)

    def test_check_attribute(self):
        """ Ensure filterable attributes can be checked for filter satisfaction. """

        class MyClass(Filteree):
            _FILTERABLE_ATTRIBUTES = ("name", "is_alive", "iterable")

            def __init__(self, name, is_alive, iterable):
                self.name = name
                self.is_alive = is_alive
                self.iterable = iterable

        filterable_thing = MyClass(name="Fred", is_alive=True, iterable={1, 2, 3})
        self.assertTrue(filterable_thing.check_attribute("name__icontains", "f"))
        self.assertFalse(filterable_thing.check_attribute("is_alive__is", False))
        self.assertTrue(filterable_thing.check_attribute("iterable__contains", 3))
