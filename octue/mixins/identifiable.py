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

    def __init__(self, *args, id=None, name=None, **kwargs):
        self._name = name
        super().__init__(*args, **kwargs)
        self._set_id(id)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        if self.name:
            return f"<{type(self).__name__}({self.name!r})>"

        return f"<{type(self).__name__}({self.id})>"

    @property
    def id(self):
        """Get the ID of the identifiable instance.

        :return str:
        """
        return self._id

    @property
    def name(self):
        """Get the name of the identifiable instance.

        :return str:
        """
        return self._name

    @name.setter
    def name(self, name):
        """Set the name of the identifiable instance.

        :param str name:
        :return None:
        """
        self._name = name

    def _set_id(self, value):
        """Set the ID to the given value.

        :param str|uuid.UUID|None value:
        :return None:
        """
        # Store a boolean record of whether this object was created with a previously-existing uuid or was created new.
        self._created = True if value is None else False

        if isinstance(value, uuid.UUID):
            # If it's a uuid, stringify it
            value = str(value)

        elif isinstance(value, str):
            # If it's a string (or something similar which can be converted to UUID) check it's valid
            try:
                value = str(uuid.UUID(value))
            except ValueError:
                raise InvalidInputException(
                    f"Value of id '{value}' is not a valid uuid string or instance of class UUID"
                )

        elif value is not None:
            raise InvalidInputException(
                f"Value of id '{value}' must be a valid uuid string, an instance of class UUID or None"
            )

        self._id = value or gen_uuid()
