import uuid

from octue.exceptions import InvalidInputException
from octue.utils import gen_uuid


class Identifiable:
    """Mixin to allow instantiation of a class with a given uuid, or generate one on instantiation

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
        """Constructor for Identifiable class"""
        super().__init__(*args, **kwargs)

        # Store a boolean record of whether this object was created with a previously-existing uuid or was created new.
        self._created = True if id is None else False

        if isinstance(id, uuid.UUID):
            # If it's a uuid, stringify it
            id = str(id)

        elif isinstance(id, str):
            # If it's a string (or something similar which can be converted to UUID) check it's valid
            try:
                id = str(uuid.UUID(id))
            except ValueError:
                raise InvalidInputException(f"Value of id '{id}' is not a valid uuid string or instance of class UUID")

        elif id is not None:
            raise InvalidInputException(
                f"Value of id '{id}' must be a valid uuid string, an instance of class UUID or None"
            )

        self._id = id or gen_uuid()

    def __str__(self):
        return f"{self.__class__.__name__} {self._id}"

    def __repr__(self):
        return self.__str__()

    @property
    def id(self):
        return self._id
