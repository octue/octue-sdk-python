from octue import exceptions
from octue.mixins import Filterable
from octue.resources.filter_containers import FilterList, FilterSet
from tests.base import BaseTestCase


class Cat(Filterable):
    def __init__(self, name=None, previous_names=None, age=None):
        self.name = name
        self.previous_names = previous_names
        self.age = age


class TestFilterSet(BaseTestCase):
    def test_ordering_by_a_non_existent_attribute(self):
        """ Ensure an error is raised if ordering is attempted by a non-existent attribute. """
        filter_set = FilterSet([Cat(age=5), Cat(age=4), Cat(age=3)])
        with self.assertRaises(exceptions.InvalidInputException):
            filter_set.order_by("dog-likeness")

    def test_order_by_with_string_attribute(self):
        """ Test ordering a FilterSet by a string attribute returns an appropriately ordered FilterList. """
        cats = [Cat(name="Zorg"), Cat(name="James"), Cat(name="Princess Carolyn")]
        sorted_filter_set = FilterSet(cats).order_by("name")
        self.assertEqual(sorted_filter_set, FilterList([cats[1], cats[2], cats[0]]))

    def test_order_by_with_int_attribute(self):
        """ Test ordering a FilterSet by an integer attribute returns an appropriately ordered FilterList. """
        cats = [Cat(age=5), Cat(age=4), Cat(age=3)]
        sorted_filter_set = FilterSet(cats).order_by("age")
        self.assertEqual(sorted_filter_set, FilterList(reversed(cats)))

    def test_order_by_list_attribute(self):
        """ Test that ordering by list attributes orders by the size of the list. """
        cats = [Cat(previous_names=["Scatta", "Catta"]), Cat(previous_names=["Kitty"]), Cat(previous_names=[])]
        sorted_filter_set = FilterSet(cats).order_by("previous_names")
        self.assertEqual(sorted_filter_set, FilterList(reversed(cats)))

    def test_order_by_in_reverse(self):
        """ Test ordering in reverse works correctly. """
        cats = [Cat(age=5), Cat(age=3), Cat(age=4)]
        sorted_filter_set = FilterSet(cats).order_by("age", reverse=True)
        self.assertEqual(sorted_filter_set, FilterList([cats[0], cats[2], cats[1]]))
