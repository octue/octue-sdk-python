import json
import uuid

from octue import exceptions
from octue.cloud.pub_sub.service import ANSWERS_NAMESPACE
from octue.resources import Manifest


class MockFuture:
    def __init__(self, subscription_path):
        self.subscription_path = subscription_path


class MockService:

    messages = {}

    def __init__(self, backend, id=None, run_function=None, responders=None):
        self.id = id or str(uuid.uuid4())
        self.backend = backend
        self.run_function = run_function
        self.responders = responders or []
        self._mock_answer_index = 0

    def serve(self):
        topic_name = f"projects/{self.backend.project_name}/topics/octue.{self.id}"
        self.messages[topic_name] = None

    def receive_question_then_answer(self, question):
        """Receive a question, acknowledge it, then answer it."""
        data = json.loads(question.data.decode())
        question_uuid = question.attributes["question_uuid"]
        question.ack()
        self.answer(data, question_uuid)

    def answer(self, data, question_uuid, timeout=30):
        """Answer a question (i.e. run the Service's app to analyse the given data, and return the output values to the
        asker). Answers are published to a topic whose name is generated from the UUID sent with the question, and are
        in the format specified in the Service's Twine file.
        """
        analysis = self.run_function(input_values=data["input_values"], input_manifest=data["input_manifest"])

        if analysis.output_manifest is None:
            serialised_output_manifest = None
        else:
            serialised_output_manifest = analysis.output_manifest.serialise(to_string=True)

        topic_name = f"projects/{self.backend.project_name}/topics/octue.{self.id}.{ANSWERS_NAMESPACE}.{question_uuid}"

        self.messages[topic_name] = {
            "data": json.dumps(
                {"output_values": analysis.output_values, "output_manifest": serialised_output_manifest}
            ).encode(),
            "attributes": None,
        }

    def ask(self, service_id, input_values, input_manifest=None):
        """Ask a serving Service a question (i.e. send it input values for it to run its app on). The input values must
        be in the format specified by the serving Service's Twine file. A single-use topic and subscription are created
        before sending the question to the serving Service - the topic is the expected publishing place for the answer
        from the serving Service when it comes, and the subscription is set up to subscribe to this.
        """
        if (input_manifest is not None) and (not input_manifest.all_datasets_are_in_cloud):
            raise exceptions.FileLocationError(
                "All datasets of the input manifest and all files of the datasets must be uploaded to the cloud before "
                "asking a service to perform an analysis upon them. The manifest must then be updated with the new "
                "cloud locations."
            )

        question_topic_name = f"projects/{self.backend.project_name}/topics/octue.{service_id}"

        if question_topic_name not in self.messages:
            raise exceptions.ServiceNotFound(f"Service with ID {service_id!r} cannot be found.")

        question_uuid = str(uuid.uuid4())
        response_topic_name = (
            f"projects/{self.backend.project_name}/topics/octue.{service_id}.{ANSWERS_NAMESPACE}.{question_uuid}"
        )
        # self.messages[response_topic_name] = self.mock_answers[self._mock_answer_index]
        # self._mock_answer_index = (self._mock_answer_index + 1) % 2

        if input_manifest is not None:
            input_manifest = input_manifest.serialise(to_string=True)

        self.messages[question_topic_name] = {
            "data": json.dumps({"input_values": input_values, "input_manifest": input_manifest}).encode(),
            "attributes": {"question_uuid": question_uuid},
        }

        self.messages[response_topic_name] = self._get_responder(service_id).run_function(input_values, input_manifest)
        return MockFuture(response_topic_name), question_uuid

    def _get_responder(self, service_id):
        for responder in self.responders:
            if responder.id == service_id:
                return responder

        return None

    def wait_for_answer(self, subscription, timeout=30):
        """Wait for an answer to a question on the given subscription, deleting the subscription and its topic once
        the answer is received.
        """
        answer = self.messages[subscription.subscription_path]

        if answer.output_manifest is not None:
            answer.output_manifest = answer.output_manifest.serialise()

        data = {"output_values": answer.output_values, "output_manifest": answer.output_manifest}

        if data["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(data["output_manifest"])

        return {"output_values": data["output_values"], "output_manifest": output_manifest}
