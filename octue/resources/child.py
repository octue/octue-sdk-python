from octue.resources.service import Service


class Child:
    def __init__(self, name, gcp_project_name):
        self.name = name
        self.gcp_project_name = gcp_project_name
        self._service = Service(name=f"{self.name}-local", gcp_project_name=self.gcp_project_name)

    def ask(self, input_values, input_manifest=None, timeout=20):
        question_uuid = self._service.ask(self.name, input_values, input_manifest)
        return self._service.wait_for_answer(question_uuid, self.name, timeout)
