from octue.exceptions import InvalidInputException
from octue.utils import gen_uuid


class Identifiable:
    """ Mixin to allow instantiation of a class with a given uuid, or generate one on instantiation

    Prevents setting an id after an object is instantiated.

    Provides a basic str() method which will be overloaded by most Resource classes

    ```
    class MyResource(Identifiable):
        pass

    MyResource().id  # Some generated uuid
    MyResource(id='not_a_uuid')  # Raises exception
    MyResource(id='a10603a0-194c-40d0-a7b7-fcf9952c3690').id  # That same uuid
    ```
    """

    def __init__(self, *args, id=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._id = id or gen_uuid()

    def __str__(self):
        return f"{self.__class__.__name__} {self._id}"

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        raise InvalidInputException("You cannot set the id of an already-instantiated %s", self.__class__.__name__)
