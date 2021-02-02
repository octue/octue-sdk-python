from octue.mixins import CoolNameable
from tests.base import BaseTestCase


class NamedThing(CoolNameable):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        super().__init__(*args, **kwargs)


class PotentiallyNamedThing(CoolNameable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = kwargs.pop("name", None)


class TestCoolNameable(BaseTestCase):
    def test_name_has_two_components(self):
        """ Test that the cool name is in the format of <word>-<word>. """
        cool_nameable = CoolNameable()
        self.assertEqual(len(cool_nameable.name.split("-")), 2)

    def test_names_are_different(self):
        """ Test that the cool names of two different instances are different. """
        cool_nameables = {CoolNameable().name for _ in range(5)}
        self.assertEqual(len(cool_nameables), 5)

    def test_cool_name_is_not_used_if_subclass_already_has_name_attribute(self):
        """ Ensure that CoolNameable does not override the existing name attribute of a subclass instance. """
        named_thing = NamedThing(name="I have a name")
        self.assertEqual(named_thing.name, "I have a name")

    def test_cool_name_is_not_used_if_subclass_already_has_name_in_kwargs(self):
        """ Ensure that CoolNameable does not override the existing name keyword argument of a subclass instance. """
        named_thing = PotentiallyNamedThing(name="I have a name")
        self.assertEqual(named_thing.name, "I have a name")

    def test_cool_name_is_applied_when_existing_name_is_none_in_subclass(self):
        """ Test that a cool name is applied if the existing name attribute of the subclass is set to None. """
        named_thing = NamedThing(name=None)
        self.assertTrue(named_thing is not None)

    def test_cool_name_is_applied_when_no_name_kwarg_given_to_subclass(self):
        """ Test that a cool name is applied if there is no name keyword argument is given to the subclass. """
        named_thing = PotentiallyNamedThing()
        self.assertTrue(named_thing is not None)
