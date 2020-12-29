from tests.base import BaseTestCase

from octue.mixins.filteree import Filteree


class TestFilteree(BaseTestCase):
    def test_error_raised_when_no_filterable_attributes(self):
        filteree = Filteree()

        with self.assertRaises(ValueError):
            filteree.check_attribute(None, None)

    def test_check_attribute(self):
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
