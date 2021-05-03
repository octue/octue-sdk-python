""" The names of backend classes must end with "Backend" to be registered as valid backends for the `Child` class.
Beyond that, a backend class can store whatever data it needs to authenticate and specify resources for the specific
backend it represents. Credentials should not be stored directly in a backend instance, but storing the names of
environment variables that store credentials is fine. For a backend to be valid, it must have a corresponding entry in
the "oneOf" field of the "backend" key of the children schema in `Twined`, which is located at
`twined/schema/children_schema.json`.
"""
from abc import ABC

from octue import exceptions


def get_backend(backend_name):
    if backend_name not in AVAILABLE_BACKENDS:
        raise exceptions.BackendNotFound(
            f"Backend with name {backend_name} not found. Available backends are {list(AVAILABLE_BACKENDS.keys())}"
        )

    return AVAILABLE_BACKENDS[backend_name]


class ServiceBackend(ABC):
    """A dataclass specifying the backend for an Octue Service, including any credentials and other information it
    needs.

    :param str|None credentials_environment_variable:
    :return None:
    """

    def __init__(self, credentials_environment_variable):
        self.credentials_environment_variable = credentials_environment_variable


class GCPPubSubBackend(ServiceBackend):
    """A dataclass containing the details needed to use Google Cloud Platform Pub/Sub as a Service backend.

    :param str project_name:
    :param str|None credentials_environment_variable:
    :return None:
    """

    def __init__(self, project_name, credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS"):
        self.project_name = project_name
        super().__init__(credentials_environment_variable)

    def __repr__(self):
        return f"<{type(self).__name__}(project_name={self.project_name!r})>"


AVAILABLE_BACKENDS = {
    key: value for key, value in locals().items() if key.endswith("Backend") and key != "ServiceBackend"
}
