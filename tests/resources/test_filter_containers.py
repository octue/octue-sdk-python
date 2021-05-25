from octue import exceptions
from octue.mixins import Filterable
from octue.resources.filter_containers import FilterDict, FilterList, FilterSet
from tests.base import BaseTestCase


class FilterableThing(Filterable):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __eq__(self, other):
        return vars(self) == vars(other)

    def __hash__(self):
        return hash(str(vars(self).items()))


class TestFilterSet(BaseTestCase):
    def test_error_raised_if_any_items_are_not_filterable(self):
        """Test that an error is raised if any items in the FilterSet are not of type Filterable."""
        filter_set = FilterSet([1, 2, 3])

        with self.assertRaises(TypeError):
            filter_set.filter(a__equals=2)

    def test_filter_with_filterables_of_differing_attributes(self):
        """Test filtering with filterables of differing attributes ignores the filterables lacking the filtered-for
        attribute.
        """
        filterables = {FilterableThing(a=3), FilterableThing(b=90), FilterableThing(a=77)}
        filter_set = FilterSet(filterables)

        self.assertEqual(filter_set.filter(a__gt=2), {FilterableThing(a=3), FilterableThing(a=77)})
        self.assertEqual(filter_set.filter(b__equals=90), {FilterableThing(b=90)})
        self.assertEqual(filter_set.filter(b__equals=0), set())

    def test_filter_with_filterables_of_differing_attributes_fails_if_setting_enabled(self):
        """Test filtering with filterables of differing attributes raises an error if any filterables lack the
        filtered-for attribute when `ignore_items_without_attribute` is False.
        """
        filter_set = FilterSet({FilterableThing(a=3), FilterableThing(b=90), FilterableThing(a=77)})

        for kwarg in {"a__gt": 2}, {"b__equals": 90}, {"b__equals": 0}:
            with self.assertRaises(AttributeError):
                filter_set.filter(**kwarg, ignore_items_without_attribute=False)

    def test_filtering_with_multiple_filters(self):
        """Test that multiple filters can be specified in FilterSet.filter at once."""
        filterables = {FilterableThing(a=3, b=2), FilterableThing(a=3, b=99), FilterableThing(a=77)}
        self.assertEqual(FilterSet(filterables).filter(a__equals=3, b__gt=80), {FilterableThing(a=3, b=99)})

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
    ANIMALS = FilterDict(
        {
            "cat": FilterableThing(name="Princess Carolyn", age=3, size="small", previous_names=["scatta", "catta"]),
            "dog": FilterableThing(name="Spot", age=90, size="big", previous_names=[]),
            "another_dog": FilterableThing(
                name="Ranger", age=91, size="small", previous_names=["doggo", "oggo", "loggo"]
            ),
        }
    )

    def test_instantiate(self):
        """Test that a FilterDict can be instantiated like a dictionary."""
        filter_dict = FilterDict(a=1, b=3)
        self.assertEqual(filter_dict, {"a": 1, "b": 3})

        filter_dict = FilterDict({"a": 1, "b": 3})
        self.assertEqual(filter_dict, {"a": 1, "b": 3})

        filter_dict = FilterDict(**{"a": 1, "b": 3})
        self.assertEqual(filter_dict, {"a": 1, "b": 3})

    def test_error_raised_when_filtering_if_any_items_are_not_filterable(self):
        """Test that an error is raised if any values in the FilterDict are not of type Filterable."""
        filter_dict = FilterDict({"a": 1, "b": 2, "c": 3})

        with self.assertRaises(TypeError):
            filter_dict.filter(my_attribute__equals=2)

    def test_filter(self):
        """Test that a FilterDict can be filtered on its values when they are all Filterables."""
        filterables = {
            "first-filterable": FilterableThing(my_value=3),
            "second-filterable": FilterableThing(my_value=90),
        }

        filter_dict = FilterDict(filterables)
        self.assertEqual(filter_dict.filter(my_value__equals=90).keys(), {"second-filterable"})
        self.assertEqual(filter_dict.filter(my_value__gt=2), filterables)

    def test_filter_with_filterables_of_differing_attributes(self):
        """Test filtering with filterables of differing attributes ignores the filterables lacking the filtered-for
        attribute.
        """
        filterables = {
            "first-filterable": FilterableThing(a=3),
            "second-filterable": FilterableThing(b=90),
            "third-filterable": FilterableThing(a=77),
        }

        filter_dict = FilterDict(filterables)
        self.assertEqual(filter_dict.filter(a__gt=2).keys(), {"first-filterable", "third-filterable"})
        self.assertEqual(filter_dict.filter(b__equals=90).keys(), {"second-filterable"})
        self.assertEqual(filter_dict.filter(b__equals=0).keys(), set())

    def test_filter_with_filterables_of_differing_attributes_fails_if_setting_enabled(self):
        """Test filtering with filterables of differing attributes raises an error if any filterables lack the
        filtered-for attribute when `ignore_items_without_attribute` is False.
        """
        filterables = {
            "first-filterable": FilterableThing(a=3),
            "second-filterable": FilterableThing(b=90),
            "third-filterable": FilterableThing(a=77),
        }

        filter_dict = FilterDict(filterables)

        for kwarg in {"a__gt": 2}, {"b__equals": 90}, {"b__equals": 0}:
            with self.assertRaises(AttributeError):
                filter_dict.filter(**kwarg, ignore_items_without_attribute=False)

    def test_filter_chaining(self):
        """Test that filters can be chained to filter a FilterDict multiple times."""
        animals_with_age_of_at_least_90 = self.ANIMALS.filter(age__gte=90)
        self.assertEqual({"dog", "another_dog"}, animals_with_age_of_at_least_90.keys())

        animals_with_age_of_at_least_90_and_size_small = animals_with_age_of_at_least_90.filter(size__equals="small")
        self.assertEqual(animals_with_age_of_at_least_90_and_size_small.keys(), {"another_dog"})

    def test_filtering_with_multiple_filters(self):
        """Test that multiple filters can be specified in FilterDict.filter at once."""
        self.assertEqual(self.ANIMALS.filter(size__equals="small", age__lt=5), {"cat": self.ANIMALS["cat"]})

    def test_ordering_by_a_non_existent_attribute(self):
        """Ensure an error is raised if ordering is attempted by a non-existent attribute."""
        with self.assertRaises(exceptions.InvalidInputException):
            self.ANIMALS.order_by("dog-likeness")

    def test_order_by_with_string_attribute(self):
        """Test that ordering a FilterDict by a string attribute returns an appropriately ordered FilterList."""
        self.assertEqual(
            self.ANIMALS.order_by("name"),
            FilterList(
                (
                    ("cat", self.ANIMALS["cat"]),
                    ("another_dog", self.ANIMALS["another_dog"]),
                    ("dog", self.ANIMALS["dog"]),
                )
            ),
        )

    def test_order_by_with_int_attribute(self):
        """ Test ordering a FilterDict by an integer attribute returns an appropriately ordered FilterList. """
        self.assertEqual(
            self.ANIMALS.order_by("age"),
            FilterList(
                (
                    ("cat", self.ANIMALS["cat"]),
                    ("dog", self.ANIMALS["dog"]),
                    ("another_dog", self.ANIMALS["another_dog"]),
                )
            ),
        )

    def test_order_by_list_attribute(self):
        """Test that ordering by list attributes orders members alphabetically by the first element of each list."""
        self.assertEqual(
            self.ANIMALS.order_by("previous_names"),
            FilterList(
                (
                    ("dog", self.ANIMALS["dog"]),
                    ("another_dog", self.ANIMALS["another_dog"]),
                    ("cat", self.ANIMALS["cat"]),
                )
            ),
        )

    def test_order_by_in_reverse(self):
        """ Test ordering in reverse works correctly. """
        self.assertEqual(
            self.ANIMALS.order_by("age", reverse=True),
            FilterList(
                (
                    ("another_dog", self.ANIMALS["another_dog"]),
                    ("dog", self.ANIMALS["dog"]),
                    ("cat", self.ANIMALS["cat"]),
                )
            ),
        )
