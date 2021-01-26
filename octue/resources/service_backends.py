class GCPPubSubBackend:
    def __init__(self, project_name, credentials_filename):
        self.project_name = project_name
        self.credentials_filename = credentials_filename
