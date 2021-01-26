class GCPPubSubBackend:
    """ A dataclass containing the details needed to use Google Cloud Platform Pub/Sub as a Service backend. """

    def __init__(self, project_name, credentials_filename):
        self.project_name = project_name
        self.credentials_filename = credentials_filename
