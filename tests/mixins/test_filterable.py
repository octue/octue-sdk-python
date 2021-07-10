from datetime import date, datetime, time
from unittest.mock import Mock

from octue import exceptions
from octue.mixins.filterable import Filterable
from octue.resources.label import LabelSet
from octue.resources.tag import TagDict
from tests.base import BaseTestCase


class FilterableSubclass(Filterable):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestFilterable(BaseTestCase):
    def test_error_raised_when_invalid_filter_name_received(self):
        """ Ensure an error is raised when an invalid filter name is provided. """
        with self.assertRaises(exceptions.InvalidInputException):
            FilterableSubclass().satisfies(invalid_filter_name=None)

    def test_error_raised_when_non_existent_attribute_name_received(self):
        """ Ensure an error is raised when a non-existent attribute name is used in the filter name. """
        with self.assertRaises(AttributeError):
            FilterableSubclass().satisfies(boogaloo__is_a_dance=True)

    def test_error_raised_when_valid_but_non_existent_filter_name_received(self):
        """ Ensure an error is raised when a valid but non-existent filter name is received. """
        with self.assertRaises(exceptions.InvalidInputException):
            FilterableSubclass(age=23).satisfies(age__is_secret=True)

    def test_error_raised_when_attribute_type_has_no_filters_defined(self):
        """Ensure an error is raised when a filter for an attribute whose type doesn't have any filters defined is
        received.
        """
        with self.assertRaises(exceptions.InvalidInputException):
            FilterableSubclass(age=lambda: None).satisfies(age__equals=True)

    def test_error_raised_if_more_than_one_filter_is_provided(self):
        """Test that an error is raised if more than one filter is provided to the satisfies method."""
        with self.assertRaises(ValueError):
            FilterableSubclass(age=23).satisfies(age__equals=23, age__equals__gt=20)

    def test_equals_filter_shortcut(self):
        """Test that the shortcut for simple equals filters works (e.g. `a=7` instead of `a__equals=7`)."""
        self.assertTrue(FilterableSubclass(age=23).satisfies(age=23))
        self.assertFalse(FilterableSubclass(age=23).satisfies(age=32))

    def test_equals_filter_shortcut_with_nested_attributes(self):
        """Test that the shortcut for simple equals filters works for nested attributes e.g. `a__b=7` instead of
        `a__b__equals=7`.
        """
        inner_mock = FilterableSubclass(b=3)
        outer_mock = FilterableSubclass(a=inner_mock)
        filterable_thing = FilterableSubclass(name=outer_mock)
        self.assertTrue(filterable_thing.satisfies(name__a__b=3))

    def test_bool_filters(self):
        """ Test that the boolean filters work as expected. """
        filterable_thing = FilterableSubclass(is_alive=True)
        self.assertTrue(filterable_thing.satisfies(is_alive__is=True))
        self.assertFalse(filterable_thing.satisfies(is_alive__is=False))
        self.assertTrue(filterable_thing.satisfies(is_alive__is_not=False))
        self.assertFalse(filterable_thing.satisfies(is_alive__is_not=True))
        self.assertTrue(filterable_thing.satisfies(is_alive__equals=True))
        self.assertFalse(filterable_thing.satisfies(is_alive__equals=False))
        self.assertTrue(filterable_thing.satisfies(is_alive__not_equals=False))
        self.assertFalse(filterable_thing.satisfies(is_alive__not_equals=True))

    def test_str_filters(self):
        """ Test that the string filters work as expected. """
        filterable_thing = FilterableSubclass(name="Michael")
        self.assertTrue(filterable_thing.satisfies(name__icontains="m"))
        self.assertFalse(filterable_thing.satisfies(name__icontains="d"))
        self.assertTrue(filterable_thing.satisfies(name__not_icontains="d"))
        self.assertFalse(filterable_thing.satisfies(name__not_icontains="m"))
        self.assertTrue(filterable_thing.satisfies(name__contains="M"))
        self.assertFalse(filterable_thing.satisfies(name__contains="d"))
        self.assertTrue(filterable_thing.satisfies(name__ends_with="l"))
        self.assertFalse(filterable_thing.satisfies(name__ends_with="M"))
        self.assertTrue(filterable_thing.satisfies(name__not_ends_with="M"))
        self.assertFalse(filterable_thing.satisfies(name__not_ends_with="l"))
        self.assertTrue(filterable_thing.satisfies(name__starts_with="M"))
        self.assertFalse(filterable_thing.satisfies(name__starts_with="l"))
        self.assertTrue(filterable_thing.satisfies(name__not_starts_with="l"))
        self.assertFalse(filterable_thing.satisfies(name__not_starts_with="M"))
        self.assertTrue(filterable_thing.satisfies(name__equals="Michael"))
        self.assertFalse(filterable_thing.satisfies(name__equals="Clive"))
        self.assertTrue(filterable_thing.satisfies(name__not_equals="Clive"))
        self.assertFalse(filterable_thing.satisfies(name__not_equals="Michael"))
        self.assertTrue(filterable_thing.satisfies(name__iequals="michael"))
        self.assertFalse(filterable_thing.satisfies(name__iequals="James"))
        self.assertTrue(filterable_thing.satisfies(name__not_iequals="James"))
        self.assertFalse(filterable_thing.satisfies(name__not_iequals="michael"))
        self.assertTrue(filterable_thing.satisfies(name__is="Michael"))
        self.assertFalse(filterable_thing.satisfies(name__is="Clive"))
        self.assertTrue(filterable_thing.satisfies(name__is_not="Clive"))
        self.assertFalse(filterable_thing.satisfies(name__is_not="Michael"))
        self.assertTrue(filterable_thing.satisfies(name__lt="Noel"))
        self.assertFalse(filterable_thing.satisfies(name__lt="Harry"))
        self.assertTrue(filterable_thing.satisfies(name__lte="Michael"))
        self.assertFalse(filterable_thing.satisfies(name__lte="Harry"))
        self.assertTrue(filterable_thing.satisfies(name__gt="Clive"))
        self.assertFalse(filterable_thing.satisfies(name__gt="Noel"))
        self.assertTrue(filterable_thing.satisfies(name__gte="Michael"))
        self.assertFalse(filterable_thing.satisfies(name__gte="Noel"))
        self.assertTrue(filterable_thing.satisfies(name__in_range=("Amy", "Zoe")))
        self.assertFalse(filterable_thing.satisfies(name__in_range=("Noel", "Peter")))
        self.assertTrue(filterable_thing.satisfies(name__not_in_range=("Noel", "Peter")))
        self.assertFalse(filterable_thing.satisfies(name__not_in_range=("Amy", "Zoe")))

    def test_none_filters(self):
        """ Test that the None filters work as expected. """
        filterable_thing = FilterableSubclass(owner=None)
        self.assertTrue(filterable_thing.satisfies(owner__is=None))
        self.assertFalse(filterable_thing.satisfies(owner__is=True))
        self.assertTrue(filterable_thing.satisfies(owner__is_not=True))
        self.assertFalse(filterable_thing.satisfies(owner__is_not=None))
        self.assertTrue(filterable_thing.satisfies(owner__equals=None))
        self.assertFalse(filterable_thing.satisfies(owner__equals=True))
        self.assertTrue(filterable_thing.satisfies(owner__not_equals=True))
        self.assertFalse(filterable_thing.satisfies(owner__not_equals=None))

    def test_number_filters_with_integers_and_floats(self):
        """ Test that the number filters work as expected for integers and floats. """
        for age in (5, 5.2):
            filterable_thing = FilterableSubclass(age=age)
            self.assertTrue(filterable_thing.satisfies(age__equals=age))
            self.assertFalse(filterable_thing.satisfies(age__equals=63))
            self.assertTrue(filterable_thing.satisfies(age__not_equals=63))
            self.assertFalse(filterable_thing.satisfies(age__not_equals=age))
            self.assertTrue(filterable_thing.satisfies(age__lt=6))
            self.assertFalse(filterable_thing.satisfies(age__lt=0))
            self.assertTrue(filterable_thing.satisfies(age__lte=age))
            self.assertFalse(filterable_thing.satisfies(age__lte=0))
            self.assertTrue(filterable_thing.satisfies(age__gt=4))
            self.assertFalse(filterable_thing.satisfies(age__gt=63))
            self.assertTrue(filterable_thing.satisfies(age__gte=age))
            self.assertFalse(filterable_thing.satisfies(age__gte=63))
            self.assertTrue(filterable_thing.satisfies(age__is=age))
            self.assertFalse(filterable_thing.satisfies(age__is=63))
            self.assertTrue(filterable_thing.satisfies(age__is_not=63))
            self.assertFalse(filterable_thing.satisfies(age__is_not=age))
            self.assertTrue(filterable_thing.satisfies(age__in_range=(0, 10)))
            self.assertFalse(filterable_thing.satisfies(age__in_range=(0, 3)))
            self.assertTrue(filterable_thing.satisfies(age__not_in_range=(0, 3)))
            self.assertFalse(filterable_thing.satisfies(age__not_in_range=(0, 10)))

    def test_iterable_filters(self):
        """ Test that the iterable filters work as expected with lists, sets, and tuples. """
        for iterable in ([1, 2, 3], {1, 2, 3}, (1, 2, 3)):
            filterable_thing = FilterableSubclass(iterable=iterable)
            self.assertTrue(filterable_thing.satisfies(iterable__contains=1))
            self.assertFalse(filterable_thing.satisfies(iterable__contains=5))
            self.assertTrue(filterable_thing.satisfies(iterable__not_contains=5))
            self.assertFalse(filterable_thing.satisfies(iterable__not_contains=1))
            self.assertTrue(filterable_thing.satisfies(iterable__is=iterable))
            self.assertFalse(filterable_thing.satisfies(iterable__is=None))
            self.assertTrue(filterable_thing.satisfies(iterable__is_not=None))
            self.assertFalse(filterable_thing.satisfies(iterable__is_not=iterable))

    def test_label_set_filters(self):
        """ Test the filters for Labelset. """
        filterable_thing = FilterableSubclass(iterable=LabelSet({"fred", "charlie"}))
        self.assertTrue(filterable_thing.satisfies(iterable__any_label_contains="a"))
        self.assertFalse(filterable_thing.satisfies(iterable__any_label_contains="z"))
        self.assertTrue(filterable_thing.satisfies(iterable__not_any_label_contains="z"))
        self.assertFalse(filterable_thing.satisfies(iterable__not_any_label_contains="a"))
        self.assertTrue(filterable_thing.satisfies(iterable__any_label_starts_with="f"))
        self.assertFalse(filterable_thing.satisfies(iterable__any_label_starts_with="e"))
        self.assertTrue(filterable_thing.satisfies(iterable__any_label_ends_with="e"))
        self.assertFalse(filterable_thing.satisfies(iterable__any_label_ends_with="i"))
        self.assertTrue(filterable_thing.satisfies(iterable__not_any_label_starts_with="e"))
        self.assertFalse(filterable_thing.satisfies(iterable__not_any_label_starts_with="f"))
        self.assertTrue(filterable_thing.satisfies(iterable__not_any_label_ends_with="i"))
        self.assertFalse(filterable_thing.satisfies(iterable__not_any_label_ends_with="e"))

    def test_datetime_filters(self):
        my_datetime = datetime(2000, 1, 1)
        filterable_thing = FilterableSubclass(timestamp=my_datetime)
        self.assertTrue(filterable_thing.satisfies(timestamp__equals=my_datetime))
        self.assertFalse(filterable_thing.satisfies(timestamp__equals=datetime(2, 2, 2)))
        self.assertTrue(filterable_thing.satisfies(timestamp__not_equals=datetime(2, 2, 2)))
        self.assertFalse(filterable_thing.satisfies(timestamp__not_equals=my_datetime))
        self.assertTrue(filterable_thing.satisfies(timestamp__is=my_datetime))
        self.assertFalse(filterable_thing.satisfies(timestamp__is=datetime(2, 2, 2)))
        self.assertTrue(filterable_thing.satisfies(timestamp__is_not=datetime(2, 2, 2)))
        self.assertFalse(filterable_thing.satisfies(timestamp__is_not=my_datetime))
        self.assertTrue(filterable_thing.satisfies(timestamp__gt=datetime(1900, 1, 2)))
        self.assertFalse(filterable_thing.satisfies(timestamp__gt=datetime(3000, 1, 2)))
        self.assertTrue(filterable_thing.satisfies(timestamp__gte=my_datetime))
        self.assertFalse(filterable_thing.satisfies(timestamp__gte=datetime(3000, 1, 2)))
        self.assertTrue(filterable_thing.satisfies(timestamp__lt=datetime(3000, 1, 2)))
        self.assertFalse(filterable_thing.satisfies(timestamp__lt=datetime(1990, 1, 2)))
        self.assertTrue(filterable_thing.satisfies(timestamp__lte=my_datetime))
        self.assertFalse(filterable_thing.satisfies(timestamp__lte=datetime(1900, 1, 2)))
        self.assertTrue(filterable_thing.satisfies(timestamp__in_range=(datetime(1900, 1, 2), datetime(3000, 1, 2))))
        self.assertFalse(filterable_thing.satisfies(timestamp__in_range=(datetime(2100, 1, 2), datetime(3000, 1, 2))))
        self.assertTrue(
            filterable_thing.satisfies(timestamp__not_in_range=(datetime(2100, 1, 2), datetime(3000, 1, 2)))
        )
        self.assertFalse(
            filterable_thing.satisfies(timestamp__not_in_range=(datetime(1900, 1, 2), datetime(3000, 1, 2)))
        )
        self.assertTrue(filterable_thing.satisfies(timestamp__year_equals=2000))
        self.assertFalse(filterable_thing.satisfies(timestamp__year_equals=3000))
        self.assertTrue(filterable_thing.satisfies(timestamp__year_in={2000, 3000, 4000}))
        self.assertFalse(filterable_thing.satisfies(timestamp__year_in={3000, 4000}))
        self.assertTrue(filterable_thing.satisfies(timestamp__month_equals=1))
        self.assertFalse(filterable_thing.satisfies(timestamp__month_equals=9))
        self.assertTrue(filterable_thing.satisfies(timestamp__month_in={1, 2, 3}))
        self.assertFalse(filterable_thing.satisfies(timestamp__month_in={2, 3}))
        self.assertTrue(filterable_thing.satisfies(timestamp__day_equals=1))
        self.assertFalse(filterable_thing.satisfies(timestamp__day_equals=2))
        self.assertTrue(filterable_thing.satisfies(timestamp__day_in={1, 2, 3}))
        self.assertFalse(filterable_thing.satisfies(timestamp__day_in={2, 3}))
        self.assertTrue(filterable_thing.satisfies(timestamp__weekday_equals=5))
        self.assertFalse(filterable_thing.satisfies(timestamp__weekday_equals=3))
        self.assertTrue(filterable_thing.satisfies(timestamp__weekday_in={5, 6, 7}))
        self.assertFalse(filterable_thing.satisfies(timestamp__weekday_in={6, 7}))
        self.assertTrue(filterable_thing.satisfies(timestamp__iso_weekday_equals=6))
        self.assertFalse(filterable_thing.satisfies(timestamp__iso_weekday_equals=4))
        self.assertTrue(filterable_thing.satisfies(timestamp__iso_weekday_in={5, 6, 7}))
        self.assertFalse(filterable_thing.satisfies(timestamp__iso_weekday_in={7, 8}))
        self.assertTrue(filterable_thing.satisfies(timestamp__time_equals=time(0, 0, 0)))
        self.assertFalse(filterable_thing.satisfies(timestamp__time_equals=time(1, 2, 3)))
        self.assertTrue(filterable_thing.satisfies(timestamp__hour_equals=0))
        self.assertFalse(filterable_thing.satisfies(timestamp__hour_equals=1))
        self.assertTrue(filterable_thing.satisfies(timestamp__hour_in={0, 1, 2}))
        self.assertFalse(filterable_thing.satisfies(timestamp__hour_in={1, 2}))
        self.assertTrue(filterable_thing.satisfies(timestamp__minute_equals=0))
        self.assertFalse(filterable_thing.satisfies(timestamp__minute_equals=1))
        self.assertTrue(filterable_thing.satisfies(timestamp__minute_in={0, 1, 2}))
        self.assertFalse(filterable_thing.satisfies(timestamp__minute_in={1, 2}))
        self.assertTrue(filterable_thing.satisfies(timestamp__second_equals=0))
        self.assertFalse(filterable_thing.satisfies(timestamp__second_equals=1))
        self.assertTrue(filterable_thing.satisfies(timestamp__second_in={0, 1, 2}))
        self.assertFalse(filterable_thing.satisfies(timestamp__second_in={1, 2}))
        self.assertTrue(filterable_thing.satisfies(timestamp__in_date_range=(date(1000, 1, 4), date(3000, 7, 10))))
        self.assertFalse(filterable_thing.satisfies(timestamp__in_date_range=(date(2000, 1, 4), date(3000, 7, 10))))
        self.assertTrue(filterable_thing.satisfies(timestamp__in_time_range=(time(0, 0, 0), time(13, 2, 22))))
        self.assertFalse(filterable_thing.satisfies(timestamp__in_time_range=(time(0, 0, 1), time(13, 2, 22))))

    def test_filtering_different_attributes_on_same_instance(self):
        """ Ensure all filterable attributes on an instance can be checked for filter satisfaction. """
        filterable_thing = FilterableSubclass(name="Fred", is_alive=True, iterable={1, 2, 3}, age=5.2, owner=None)
        self.assertTrue(filterable_thing.satisfies(name__icontains="f"))
        self.assertTrue(filterable_thing.satisfies(name__not_icontains="j"))
        self.assertFalse(filterable_thing.satisfies(is_alive__is=False))
        self.assertTrue(filterable_thing.satisfies(iterable__contains=3))
        self.assertTrue(filterable_thing.satisfies(age__equals=5.2))
        self.assertTrue(filterable_thing.satisfies(age__not_equals=5))
        self.assertTrue(filterable_thing.satisfies(owner__is=None))

    def test_filtering_with_nested_attributes(self):
        """Test that Filterable subclasses can be checked for satisfaction of a filter of nested attributes."""
        inner_mock = Mock(b=3)
        outer_mock = Mock(a=inner_mock)
        filterable_thing = FilterableSubclass(name=outer_mock)
        self.assertTrue(filterable_thing.satisfies(name__a__b__equals=3))

    def test_filtering_with_nested_attributes_ending_in_dictionary_key(self):
        """Test that Filterable subclasses can be checked for satisfaction of a filter of nested attributes that ends
        with a dictionary key.
        """
        filterable_thing = FilterableSubclass(name={"first": "Joe", "last": "Bloggs"})
        self.assertTrue(filterable_thing.satisfies(name__first__equals="Joe"))
        self.assertTrue(filterable_thing.satisfies(name__last__equals="Bloggs"))

    def test_tag_dict_filters(self):
        """Test some filters that apply to a TagDict. These should behave just the same as for a dictionary."""
        filterable_thing = FilterableSubclass(tags=TagDict({"first": "Joe", "middle": "Horatio", "last": "Bloggs"}))
        self.assertTrue(filterable_thing.satisfies(tags__last__lt="Kevin"))
        self.assertFalse(filterable_thing.satisfies(tags__middle__is="Boratio"))
