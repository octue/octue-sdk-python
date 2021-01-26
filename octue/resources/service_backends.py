""" The names of backend classes must end with "Backend" to be registered as valid backends for the `Child` class.
Beyond that, a backend class can store whatever data it needs to authenticate and specify resources for the specific
backend it represents. Credentials should not be stored directly in a backend instance, but storing the names of
environment variables that store credentials is fine. For a backend to be valid, it must have a corresponding entry in
the "oneOf" field of the "backend" key of the children schema in `Twined`, which is located at
`twined/schema/children_schema.json`.
"""


class GCPPubSubBackend:
    """ A dataclass containing the details needed to use Google Cloud Platform Pub/Sub as a Service backend. """

    def __init__(self, project_name, credentials_filename):
        self.project_name = project_name
        self.credentials_filename = credentials_filename
