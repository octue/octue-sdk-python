import json
import logging
import google.api_core.exceptions

from octue.cloud.pub_sub import Subscription, Topic
from octue.cloud.pub_sub.service import Service
from octue.resources import Manifest


MESSAGES = {}


logger = logging.getLogger(__name__)


def get_service_id(path):
    """Get the service ID (e.g. octue.services.<uuid>) from a topic or subscription path (e.g.
    projects/<project-name>/topics/octue.services.<uuid>)

    :param str path:
    :return str:
    """
    return path.split("/")[-1]


class MockTopic(Topic):
    """A mock topic that registers in a global messages dictionary rather than Google Pub/Sub."""

    def create(self, allow_existing=False):
        """Register the topic in the global messages dictionary.

        :param bool allow_existing: if True, don't raise an error if the topic already exists
        :raise google.api_core.exceptions.AlreadyExists: if the topic already exists
        :return None:
        """
        if not allow_existing:
            if self.exists():
                raise google.api_core.exceptions.AlreadyExists(f"Topic {self.path!r} already exists.")

        if not self.exists():
            MESSAGES[get_service_id(self.path)] = []

    def delete(self):
        """Delete the topic from the global messages dictionary.

        :return None:
        """
        try:
            del MESSAGES[get_service_id(self.path)]
        except KeyError:
            pass

    def exists(self):
        """Check if the topic exists in the global messages dictionary.

        :return bool:
        """
        return get_service_id(self.path) in MESSAGES


class MockSubscription(Subscription):
    """A mock subscription that registers in a global dictionary rather than Google Pub/Sub."""

    def create(self, allow_existing=False):
        pass

    def delete(self):
        pass


class MockFuture:
    """A mock future that is returned when publishing or subscribing."""

    def result(self, timeout=None):
        pass

    def cancel(self):
        pass


class MockPublisher:
    """A mock publisher that puts messages in a global dictionary instead of Google Pub/Sub."""

    def publish(self, topic, data, retry=None, **attributes):
        """Put the data and attributes into a MockMessage and add it to the global messages dictionary before returning
        a MockFuture.

        :param str topic:
        :param bytes data:
        :param google.api_core.retry.Retry|None retry:
        :return MockFuture:
        """
        MESSAGES[get_service_id(topic)].append(MockMessage(data=data, **attributes))
        return MockFuture()

    @staticmethod
    def topic_path(project_name, topic_name):
        """Generate the full topic path from the project and topic names.

        :param str project_name:
        :param str topic_name:
        :return str:
        """
        return f"projects/{project_name}/topics/{topic_name}"


class MockSubscriber:
    """A mock subscriber that gets messages from a global dictionary instead of Google Pub/Sub."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def subscribe(self, subscription, callback):
        """Simulate subscribing to a topic by returning a mock future.

        :param MockSubscription subscription:
        :param callable callback:
        :return MockFuture:
        """
        return MockFuture()

    def pull(self, request, timeout=None, retry=None):
        """Return a MockPullResponse containing one MockMessage wrapped in a MockMessageWrapper. The MockMessage is
        retrieved from the global messages dictionary for the subscription included in the request under the
        "subscription" key.

        :param dict request:
        :param float|None timeout:
        :param google.api_core.retry.Retry|None retry:
        :return MockPullResponse:
        """
        return MockPullResponse(
            received_messages=[MockMessageWrapper(message=MESSAGES[get_service_id(request["subscription"])].pop(0))]
        )

    def acknowledge(self, request):
        pass

    @staticmethod
    def subscription_path(project_name, subscription_name):
        """Generate the full subscription path from the given project and subscription names.

        :param str project_name:
        :param str subscription_name:
        :return str:
        """
        return f"projects/{project_name}/subscriptions/{subscription_name}"


class MockPullResponse:
    """A mock PullResponse that is returned by `MockSubscriber.pull`.

    :param iter(MockMessageWrapper)|None received_messages: mock pub/sub messages wrapped in the MockMessageWrapper
    :return None:
    """

    def __init__(self, received_messages=None):
        self.received_messages = received_messages or []


class MockMessageWrapper:
    """A wrapper for MockMessage.

    :param MockMessage message:
    :return None:
    """

    def __init__(self, message):
        self.message = message
        self.ack_id = None


class MockMessage:
    """A mock Pub/Sub message containing serialised data and any number of attributes.

    :param bytes data:
    :return None:
    """

    def __init__(self, data, **attributes):
        self.data = data
        self.attributes = {}
        for key, value in attributes.items():
            self.attributes[key] = value

    def ack(self):
        pass


class MockService(Service):
    """A mock Google Pub/Sub Service that can send and receive messages synchronously to other instances.

    :param octue.resources.service_backends.GCPPubSubBackEnd backend:
    :param str service_id:
    :param callable run_function:
    :param dict(str, MockService)|None children:
    :return None:
    """

    def __init__(self, backend, service_id=None, run_function=None, children=None):
        super().__init__(backend, service_id, run_function)
        self.children = children or {}
        self.publisher = MockPublisher()
        self.subscriber = MockSubscriber()

    def ask(self, service_id, input_values, input_manifest=None, subscribe_to_logs=True):
        """Put the question into the messages register, register the existence of the corresponding response topic, add
        the response to the register, and return a MockFuture containing the answer subscription path.

        :param str service_id:
        :param dict|list input_values:
        :param octue.resources.manifest.Manifest|None input_manifest:
        :param bool subscribe_to_logs:
        :return MockFuture, str:
        """
        response_subscription, question_uuid = super().ask(service_id, input_values, input_manifest, subscribe_to_logs)

        # Ignore any errors from the answering service as they will be raised on the remote service in practice, not
        # locally as is done in this mock.
        if input_manifest is not None:
            input_manifest = input_manifest.serialise(to_string=True)

        try:
            self.children[service_id].answer(
                MockMessage(
                    data=json.dumps({"input_values": input_values, "input_manifest": input_manifest}).encode(),
                    question_uuid=question_uuid,
                    forward_logs=subscribe_to_logs,
                )
            )
        except Exception as e:  # noqa
            logger.exception(e)

        return response_subscription, question_uuid


class MockMessagePuller:
    """A mock message puller that returns the messages in the order they were provided on initialisation.

    :param iter(dict) messages:
    :return None:
    """

    def __init__(self, messages):
        self.messages = messages
        self.message_number = 0

    def pull(self, subscription, timeout):
        """Return the next message from the messages given at initialisation.

        :param any subscription: this isn't used in this mock but is required in the signature of a message pulling function
        :return dict:
        """
        message = self.messages[self.message_number]
        self.message_number += 1
        return message


class MockAnalysis:
    """A mock Analysis object with just the output strands.

    :param any output_values:
    :param octue.resources.manifest.Manifest|None output_manifest:
    :return None:
    """

    def __init__(self, output_values="Hello! It worked!", output_manifest=None):
        self.output_values = output_values
        self.output_manifest = output_manifest


class DifferentMockAnalysis:
    output_values = "This is another successful analysis."
    output_manifest = None


class MockAnalysisWithOutputManifest:
    output_values = "This is an analysis with an empty output manifest."
    output_manifest = Manifest()
