from octue.resources.service import Service


class Child:
    """ A class representing a child service that can be asked questions. """

    def __init__(self, name, id, gcp_project_name):
        self.name = name
        self.id = id
        self.gcp_project_name = gcp_project_name
        self._service = Service(name=f"{self.name}-local", gcp_project_name=self.gcp_project_name)

    def ask(self, input_values, input_manifest=None, timeout=20):
        """ Ask the child a question (i.e. send it some input value and/or a manifest and wait for it to run an analysis
        on them and return the output values). The input values given must adhere to the Twine file of the child.
        """
        subscription = self._service.ask(self.id, input_values, input_manifest)
        return self._service.wait_for_answer(subscription, timeout)
