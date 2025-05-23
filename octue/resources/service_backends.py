"""The names of backend classes must end with "Backend" to be registered as valid backends for the `Child` class.
Beyond that, a backend class can store whatever data it needs to authenticate and specify resources for the specific
backend it represents. Credentials should not be stored directly in a backend instance, but storing the names of
environment variables that store credentials is fine. For a backend to be valid, it must have a corresponding entry in
the "oneOf" field of the "backend" key of the children schema in `Twined`, which is located at
`twined/schema/children_schema.json`.
"""

from abc import ABC

from octue import exceptions


def get_backend(backend_name=None):
    """Get the service backend with the given name.

    :param str|None backend_name: if `None`, return the `GCPPubSubBackend`
    :return ServiceBackend:
    """
    if not backend_name:
        return GCPPubSubBackend

    if backend_name not in AVAILABLE_BACKENDS:
        raise exceptions.BackendNotFound(
            f"Backend with name {backend_name} not found. Available backends are {list(AVAILABLE_BACKENDS.keys())}"
        )

    return AVAILABLE_BACKENDS[backend_name]


class ServiceBackend(ABC):
    """A dataclass specifying the backend for an Octue Service, including any credentials and other information it
    needs.

    :return None:
    """


class GCPPubSubBackend(ServiceBackend):
    """A dataclass containing the details needed to use Google Cloud Pub/Sub as a Service backend.

    :param str project_id: the ID of the project to use for Pub/Sub
    :return None:
    """

    def __init__(self, project_id):
        if project_id is None:
            raise exceptions.CloudLocationNotSpecified(
                "`project_id` must be specified for a service to connect to the correct service - received None."
            )

        self.project_id = project_id

    def __repr__(self):
        return f"<{type(self).__name__}(project_id={self.project_id!r})>"


AVAILABLE_BACKENDS = {
    key: value for key, value in locals().items() if key.endswith("Backend") and key != "ServiceBackend"
}
