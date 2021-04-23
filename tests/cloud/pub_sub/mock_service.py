import json
import uuid

from octue import exceptions
from octue.cloud.pub_sub.service import ANSWERS_NAMESPACE


class MockFuture:
    """A mock future that contains the subscription path for the corresponding answer.

    :param str subscription_path:
    :return None
    """

    def __init__(self, subscription_path):
        self.subscription_path = subscription_path


class MockService:
    """A mock Google Pub/Sub Service that can send and receive messages synchronously to other instances.

    :param octue.resources.service_backends.GCPPubSubBackEnd backend:
    :param str id:
    :param callable run_function:
    :param list(MockService) children:
    """

    messages = {}

    def __init__(self, backend, id=None, run_function=None, children=None):
        self.id = id or str(uuid.uuid4())
        self.backend = backend
        self.run_function = run_function
        self.children = children or []
        self._mock_answer_index = 0

    def serve(self):
        """Register the mock service as available to receive questions.

        :return None:
        """
        topic_name = f"projects/{self.backend.project_name}/topics/octue.{self.id}"
        self.messages[topic_name] = None

    def answer(self, data, question_uuid, timeout=30):
        """Answer a question, putting it in the messages register under the relevant key. Serialise the manifest before
        putting the answer in the register.

        :param dict data:
        :param str question_uuid:
        :param float timeout:
        :return None:
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
        """Put the question into the messages register, register the existence of the corresponding response topic, add
        the response to the register, and return a MockFuture containing the answer subscription path.

        :param str service_id:
        :param dict input_values:
        :param octue.resources.manifest.Manifest input_manifest:
        :return MockFuture, str:
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

        if input_manifest is not None:
            input_manifest = input_manifest.serialise(to_string=True)

        self.messages[question_topic_name] = {
            "data": json.dumps({"input_values": input_values, "input_manifest": input_manifest}).encode(),
            "attributes": {"question_uuid": question_uuid},
        }

        response_topic_path = (
            f"projects/{self.backend.project_name}/topics/octue.{service_id}.{ANSWERS_NAMESPACE}.{question_uuid}"
        )

        self.messages[response_topic_path] = self._get_child(service_id).run_function(input_values, input_manifest)
        return MockFuture(subscription_path=response_topic_path), question_uuid

    def wait_for_answer(self, subscription, timeout=30):
        """Get the answer from the message register, change it into the correct format, and return it.

        :param subscription:
        :param timeout:
        :return:
        """
        answer = self.messages[subscription.subscription_path]
        data = {"output_values": answer.output_values, "output_manifest": answer.output_manifest}
        return {"output_values": data["output_values"], "output_manifest": data["output_manifest"]}

    def _get_child(self, service_id):
        """Get the correct responding MockService from the children.

        :param str service_id:
        :return MockService:
        """
        for child in self.children:
            if child.id == service_id:
                return child

        return None
