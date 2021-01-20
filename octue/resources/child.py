from octue.resources.service import Service


class Child:
    def __init__(self, name, id, gcp_project_name):
        self.name = name
        self.id = id
        self.gcp_project_name = gcp_project_name
        self._service = Service(name=f"{self.name}-local", gcp_project_name=self.gcp_project_name)

    def ask(self, input_values, input_manifest=None, timeout=20):
        subscription = self._service.ask(self.id, input_values, input_manifest)
        return self._service.wait_for_answer(subscription, timeout)
