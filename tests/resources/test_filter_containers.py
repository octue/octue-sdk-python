from tests.base import BaseTestCase

from octue import exceptions
from octue.mixins import Filterable
from octue.resources.filter_containers import FilterList, FilterSet


class Cat(Filterable):
    def __init__(self, name=None, is_alive=None, previous_names=None, age=None, owner=None):
        self.name = name
        self.is_alive = is_alive
        self.previous_names = previous_names
        self.age = age
        self.owner = owner


class TestFilterSet(BaseTestCase):
    def test_order_by_with_string_attribute(self):
        """ Test ordering a FilterSet by a string attribute. """
        cats = [Cat(name="Zorg"), Cat(name="James"), Cat(name="Princess Carolyne")]
        filter_set = FilterSet(cats)
        sorted_filter_set = filter_set.order_by("name")
        self.assertEqual(sorted_filter_set, FilterList([cats[1], cats[2], cats[0]]))

    def test_order_by_with_int_attribute(self):
        """ Test ordering a FilterSet by an integer attribute. """
        cats = [Cat(age=5), Cat(age=4), Cat(age=3)]
        filter_set = FilterSet(cats)
        sorted_filter_set = filter_set.order_by("age")
        self.assertEqual(sorted_filter_set, FilterList([cats[2], cats[1], cats[0]]))

    def test_ordering_by_a_non_existent_attribute(self):
        """ Ensure an error is raised if ordering is attempted by a non-existent attribute. """
        filter_set = FilterSet([Cat(age=5), Cat(age=4), Cat(age=3)])
        with self.assertRaises(exceptions.InvalidInputException):
            filter_set.order_by("dog-likeness")
