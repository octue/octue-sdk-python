from octue import exceptions
from octue.mixins import Filterable
from octue.resources.filter_containers import FilterDict, FilterList, FilterSet
from tests.base import BaseTestCase


class FilterableThing(Filterable):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestFilterSet(BaseTestCase):
    def test_ordering_by_a_non_existent_attribute(self):
        """ Ensure an error is raised if ordering is attempted by a non-existent attribute. """
        filter_set = FilterSet([FilterableThing(age=5), FilterableThing(age=4), FilterableThing(age=3)])
        with self.assertRaises(exceptions.InvalidInputException):
            filter_set.order_by("dog-likeness")

    def test_order_by_with_string_attribute(self):
        """ Test ordering a FilterSet by a string attribute returns an appropriately ordered FilterList. """
        cats = [FilterableThing(name="Zorg"), FilterableThing(name="James"), FilterableThing(name="Princess Carolyn")]
        sorted_filter_set = FilterSet(cats).order_by("name")
        self.assertEqual(sorted_filter_set, FilterList([cats[1], cats[2], cats[0]]))

    def test_order_by_with_int_attribute(self):
        """ Test ordering a FilterSet by an integer attribute returns an appropriately ordered FilterList. """
        cats = [FilterableThing(age=5), FilterableThing(age=4), FilterableThing(age=3)]
        sorted_filter_set = FilterSet(cats).order_by("age")
        self.assertEqual(sorted_filter_set, FilterList(reversed(cats)))

    def test_order_by_list_attribute(self):
        """ Test that ordering by list attributes orders by the size of the list. """
        cats = [
            FilterableThing(previous_names=["Scatta", "Catta"]),
            FilterableThing(previous_names=["Kitty"]),
            FilterableThing(previous_names=[]),
        ]
        sorted_filter_set = FilterSet(cats).order_by("previous_names")
        self.assertEqual(sorted_filter_set, FilterList(reversed(cats)))

    def test_order_by_in_reverse(self):
        """ Test ordering in reverse works correctly. """
        cats = [FilterableThing(age=5), FilterableThing(age=3), FilterableThing(age=4)]
        sorted_filter_set = FilterSet(cats).order_by("age", reverse=True)
        self.assertEqual(sorted_filter_set, FilterList([cats[0], cats[2], cats[1]]))


class TestFilterDict(BaseTestCase):
    def test_instantiate(self):
        """Test that a FilterDict can be instantiated like a dictionary."""
        filter_dict = FilterDict(a=1, b=3)
        self.assertEqual(filter_dict["a"], 1)
        self.assertEqual(filter_dict["b"], 3)

        filter_dict = FilterDict({"a": 1, "b": 3})
        self.assertEqual(filter_dict["a"], 1)
        self.assertEqual(filter_dict["b"], 3)

        filter_dict = FilterDict(**{"a": 1, "b": 3})
        self.assertEqual(filter_dict["a"], 1)
        self.assertEqual(filter_dict["b"], 3)

    def test_filter(self):
        """Test that a FilterDict can be filtered on its values when they are all filterables."""
        filterables = {
            "first-filterable": FilterableThing(my_value=3),
            "second-filterable": FilterableThing(my_value=90),
        }

        filter_dict = FilterDict(filterables)
        self.assertEqual(filter_dict.filter(my_value__equals=90).keys(), {"second-filterable"})
        self.assertEqual(filter_dict.filter(my_value__gt=2), filterables)

    def test_filter_chaining(self):
        """Test that filters can be chained to filter a FilterDict multiple times."""
        animals = FilterDict(
            {
                "cat": FilterableThing(age=3, size="small"),
                "dog": FilterableThing(age=90, size="big"),
                "another_dog": FilterableThing(age=90, size="small"),
            }
        )

        animals_with_age_90 = animals.filter(age__equals=90)
        self.assertEqual({"dog", "another_dog"}, animals_with_age_90.keys())

        animals_with_age_90_and_size_small = animals_with_age_90.filter(size__equals="small")
        self.assertEqual(animals_with_age_90_and_size_small.keys(), {"another_dog"})
