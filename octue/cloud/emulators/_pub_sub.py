from collections import defaultdict
import json
import logging

import google.api_core

from octue.cloud.events.attributes import QuestionAttributes
from octue.cloud.pub_sub import Subscription, Topic
from octue.cloud.pub_sub.service import Service
from octue.definitions import LOCAL_SDK_VERSION
from octue.resources import Manifest
from octue.utils.dictionaries import make_minimal_dictionary
from octue.utils.encoders import OctueJSONEncoder

logger = logging.getLogger(__name__)

TOPICS = set()
SUBSCRIPTIONS = set()
MESSAGES = defaultdict(list)


class MockTopic(Topic):
    """A mock topic that registers in a global topics dictionary rather than Google Pub/Sub."""

    def create(self, allow_existing=False):
        """Register the topic in the global topics dictionary.

        :param bool allow_existing: if True, don't raise an error if the topic already exists
        :raise google.api_core.exceptions.AlreadyExists: if the topic already exists
        :return None:
        """
        if not allow_existing:
            if self.exists():
                raise google.api_core.exceptions.AlreadyExists(f"Topic {self.path!r} already exists.")

        if not self.exists():
            TOPICS.add(self.name)
            self._created = True

    def delete(self):
        """Delete the topic from the global topics dictionary.

        :return None:
        """
        try:
            TOPICS.remove(self.name)
        except KeyError:
            pass

    def exists(self, timeout=10):
        """Check if the topic exists in the global topics dictionary.

        :param float timeout:
        :return bool:
        """
        return self.name in TOPICS


class MockSubscription(Subscription):
    """A mock subscription that registers in a global dictionary rather than Google Pub/Sub."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subscriber = MockSubscriber()

    @property
    def subscriber(self):
        return self._subscriber

    def create(self, allow_existing=False):
        """Register the subscription in the global subscriptions dictionary.

        :param bool allow_existing:
        :return None:
        """
        if not allow_existing:
            if self.exists():
                raise google.api_core.exceptions.AlreadyExists(f"Subscription {self.path!r} already exists.")

        if not self.exists():
            SUBSCRIPTIONS.add(self.name)
            self._created = True

    def delete(self):
        """Delete the subscription from the global subscriptions dictionary.

        :return None:
        """
        try:
            SUBSCRIPTIONS.remove(self.name)
        except KeyError:
            pass

    def exists(self, timeout=5):
        """Check if the subscription exists in the global subscriptions dictionary.

        :param float timeout:
        :return bool:
        """
        return self.name in SUBSCRIPTIONS


class MockFuture:
    """A mock future that is returned when publishing or subscribing."""

    def __init__(self):
        self._cancelled = False
        self._returned = False

    @property
    def returned(self):
        """Check if the future has had its `result` method called.

        :return bool:
        """
        return self._returned

    @property
    def cancelled(self):
        """Check if the future has been cancelled.

        :return bool:
        """
        return self._cancelled

    def result(self, timeout=None):
        """Mark the future as returned.

        :param float timeout:
        :return None:
        """
        self._returned = True

    def cancel(self):
        """Mark the future as cancelled.

        :return None:
        """
        self._cancelled = True


class MockPublisher:
    """A mock publisher that puts messages in a global dictionary instead of Google Pub/Sub."""

    def __init__(self, *args, **kwargs):
        pass

    def publish(self, topic, data, retry=None, **attributes):
        """Put the data and attributes into a MockMessage and add it to the global messages dictionary before returning
        a MockFuture.

        :param str topic:
        :param bytes data:
        :param google.api_core.retry.Retry|None retry:
        :return MockFuture:
        """
        MESSAGES[attributes["question_uuid"]].append(MockMessage(data=data, attributes=attributes))
        return MockFuture()


class MockSubscriber:
    """A mock subscriber that gets messages from a global dictionary instead of Google Pub/Sub."""

    def __init__(self, credentials=None):
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.closed = True

    def subscribe(self, subscription, callback):
        """Simulate subscribing to a topic by returning a mock future.

        :param MockSubscription subscription:
        :param callable callback:
        :return MockFuture:
        """
        if self.closed:
            raise ValueError("ValueError: Cannot invoke RPC: Channel closed!")

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
        if self.closed:
            raise ValueError("ValueError: Cannot invoke RPC: Channel closed!")

        question_uuid = request["subscription"].split(".")[-1]

        try:
            return MockPullResponse(
                received_messages=[
                    MockMessageWrapper(message=MESSAGES[question_uuid].pop(0)),
                ]
            )

        except IndexError:
            return MockPullResponse(received_messages=[])

    def acknowledge(self, request):
        """Do nothing.

        :param google.pubsub_v1.types.pubsub.Subscription request:
        :return None:
        """
        pass

    def create_subscription(self, request):
        """Do nothing.

        :param google.pubsub_v1.types.pubsub.Subscription request:
        :return None:
        """
        pass


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

    def __repr__(self):
        """Represent the mock message wrapper as a string.

        :return str:
        """
        return f"<{type(self).__name__}(message={self.message})>"


class MockMessage:
    """A mock Pub/Sub message containing serialised data and any number of attributes.

    :param bytes data:
    :param dict|None attributes:
    :return None:
    """

    def __init__(self, data, attributes=None):
        self.data = data
        self.attributes = attributes or {}

        # Encode the attributes as they would be in a real Pub/Sub message.
        for key, value in self.attributes.items():
            if isinstance(value, bool):
                value = str(int(value))
            elif isinstance(value, (int, float)):
                value = str(value)

            self.attributes[key] = value

    @classmethod
    def from_primitive(cls, data, attributes):
        """Instantiate a mock message from data in the form of a JSON-serialisable python primitive.

        :param any data:
        :param dict attributes:
        :return MockMessage:
        """
        return cls(data=json.dumps(data, cls=OctueJSONEncoder).encode(), attributes=attributes)

    def __repr__(self):
        """Represent a mock message as a string.

        :return str: the mock message as a string
        """
        return f"<{type(self).__name__}(data={self.data!r})>"

    def ack(self):
        """Do nothing.

        :return None:
        """
        pass


class MockService(Service):
    """A mock Google Pub/Sub Service that can send and receive messages synchronously to other instances.

    :param octue.resources.service_backends.GCPPubSubBackEnd backend:
    :param str service_id:
    :param callable run_function:
    :param dict(str, MockService)|None children:
    :return None:
    """

    def __init__(self, backend, service_id=None, run_function=None, children=None, *args, **kwargs):
        super().__init__(backend, service_id, run_function, *args, **kwargs)
        self.children = children or {}
        self._publisher = MockPublisher()

    @property
    def publisher(self):
        return self._publisher

    def ask(
        self,
        service_id,
        input_values=None,
        input_manifest=None,
        children=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        save_diagnostics="SAVE_DIAGNOSTICS_ON_CRASH",
        question_uuid=None,
        parent_question_uuid=None,
        originator_question_uuid=None,
        originator=None,
        push_endpoint=None,
        asynchronous=False,
        retry_count=0,
        cpus=None,
        memory=None,
        ephemeral_storage=None,
        timeout=86400,
        parent_sdk_version=LOCAL_SDK_VERSION,
    ):
        """Put the question into the messages register, register the existence of the corresponding response topic, add
        the response to the register, and return a MockFuture containing the answer subscription path.

        :param str service_id:
        :param dict|list|None input_values:
        :param octue.resources.manifest.Manifest|None input_manifest:
        :param list(dict)|None children:
        :param bool subscribe_to_logs:
        :param bool allow_local_files:
        :param str save_diagnostics:
        :param str|None question_uuid:
        :param str|None parent_question_uuid:
        :param str|None originator_question_uuid:
        :param str|None originator:
        :param str|None push_endpoint:
        :param bool asynchronous:
        :param int retry_count: the retry count of the question (this is zero if it's the first attempt at the question)
        :param int|None cpus:
        :param str|None memory:
        :param str|None ephemeral_storage:
        :param float|None timeout:
        :param str parent_sdk_version:
        :return MockFuture, str:
        """
        response_subscription, question_uuid = super().ask(
            service_id=service_id,
            input_values=input_values,
            input_manifest=input_manifest,
            children=children,
            subscribe_to_logs=subscribe_to_logs,
            allow_local_files=allow_local_files,
            save_diagnostics=save_diagnostics,
            question_uuid=question_uuid,
            parent_question_uuid=parent_question_uuid,
            originator_question_uuid=originator_question_uuid,
            originator=originator,
            push_endpoint=push_endpoint,
            asynchronous=asynchronous,
            retry_count=retry_count,
            cpus=cpus,
            memory=memory,
            ephemeral_storage=ephemeral_storage,
            timeout=timeout,
        )

        # Delete question from messages sent to subscription so the parent doesn't pick it up as a response message. We
        # do this as subscription filtering isn't implemented in this set of mocks.
        MESSAGES[question_uuid].pop(0)

        question = make_minimal_dictionary(kind="question", input_values=input_values, children=children)

        # Ignore any errors from the answering service as they will be raised on the remote service in practice, not
        # locally as is done in this mock.
        if input_manifest is not None:
            question["input_manifest"] = input_manifest.to_primitive()

        # If the originator question UUID isn't provided, assume that this question is the originator.
        originator_question_uuid = originator_question_uuid or question_uuid

        # If the originator isn't provided, assume that this service revision is the originator.
        originator = originator or self.id

        attributes = QuestionAttributes(
            question_uuid=question_uuid,
            parent_question_uuid=parent_question_uuid,
            originator_question_uuid=originator_question_uuid,
            forward_logs=subscribe_to_logs,
            save_diagnostics=save_diagnostics,
            parent=self.id,
            originator=originator,
            sender=self.id,
            sender_sdk_version=parent_sdk_version,
            recipient=service_id,
            retry_count=retry_count,
            cpus=cpus,
            memory=memory,
            ephemeral_storage=ephemeral_storage,
        )

        try:
            self.children[service_id].answer(
                MockMessage.from_primitive(data=question, attributes=attributes.to_serialised_attributes())
            )
        except Exception as e:  # noqa
            logger.exception(e)

        return response_subscription, question_uuid


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


class MockSubscriptionCreationResponse:
    """A mock response to creating a Google Cloud Pub/Sub subscription.

    :param google.pubsub_v1.types.pubsub.Subscription request:
    :return None:
    """

    def __init__(self, request):
        self.__dict__ = vars(request)


def get_pub_sub_resource_name(path):
    """Get the Pub/Sub resource name of the topic or subscription (e.g. "octue.services.<uuid>") from its path (e.g.
    "projects/<project-name>/topics/octue.services.<uuid>").

    :param str path:
    :return str:
    """
    return path.split("/")[-1]
