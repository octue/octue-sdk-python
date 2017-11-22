import uuid


def gen_uuid():
    """Generates a unique identifier for an object
    TODO - generate using an Octue api call, so we can register and find objects later using their UUID

    :return: uuid string
    """

    return str(uuid.uuid4())
