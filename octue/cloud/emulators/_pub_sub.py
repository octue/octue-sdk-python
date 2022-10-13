import json
import logging

import google.api_core
import pkg_resources

from octue.cloud.pub_sub import Subscription, Topic
from octue.cloud.pub_sub.service import Service
from octue.resources import Manifest


logger = logging.getLogger(__name__)

MESSAGES = {}
SUBSCRIPTIONS = {}


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
            MESSAGES[self.name] = []
            self._created = True

    def delete(self):
        """Delete the topic from the global messages dictionary.

        :return None:
        """
        try:
            del MESSAGES[self.name]
        except KeyError:
            pass

    def exists(self, timeout=10):
        """Check if the topic exists in the global messages dictionary.

        :param float timeout:
        :return bool:
        """
        return self.name in MESSAGES


class MockSubscription(Subscription):
    """A mock subscription that registers in a global dictionary rather than Google Pub/Sub."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subscriber = MockSubscriber()

    def create(self, allow_existing=False):
        """Register the subscription in the global subscriptions dictionary.

        :param bool allow_existing:
        :return None:
        """
        if not allow_existing:
            if self.exists():
                raise google.api_core.exceptions.AlreadyExists(f"Subscription {self.path!r} already exists.")

        if not self.exists():
            SUBSCRIPTIONS[self.name] = []
            self._created = True

    def delete(self):
        """Do nothing.

        :return None:
        """
        pass

    def exists(self, timeout=5):
        """Check if the subscription exists in the global subscriptions dictionary.

        :param float timeout:
        :return bool:
        """
        return self.name in SUBSCRIPTIONS


class MockFuture:
    """A mock future that is returned when publishing or subscribing."""

    def result(self, timeout=None):
        """Do nothing.

        :param float timeout:
        :return None:
        """
        pass

    def cancel(self):
        """Do nothing.

        :return None:
        """
        pass


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
        MESSAGES[get_service_id(topic)].append(MockMessage(data=data, **attributes))
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

        try:
            return MockPullResponse(
                received_messages=[MockMessageWrapper(message=MESSAGES[get_service_id(request["subscription"])].pop(0))]
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
        self.subscriber = MockSubscriber()

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
        allow_save_diagnostics_data_on_crash=True,
        question_uuid=None,
        push_endpoint=None,
        timeout=86400,
        parent_sdk_version=pkg_resources.get_distribution("octue").version,
    ):
        """Put the question into the messages register, register the existence of the corresponding response topic, add
        the response to the register, and return a MockFuture containing the answer subscription path.

        :param str service_id:
        :param dict|list|None input_values:
        :param octue.resources.manifest.Manifest|None input_manifest:
        :param list(dict)|None children:
        :param bool subscribe_to_logs:
        :param bool allow_local_files:
        :param bool allow_save_diagnostics_data_on_crash:
        :param str|None question_uuid:
        :param str|None push_endpoint:
        :param float|None timeout:
        :return MockFuture, str:
        """
        response_subscription, question_uuid = super().ask(
            service_id=service_id,
            input_values=input_values,
            input_manifest=input_manifest,
            children=children,
            subscribe_to_logs=subscribe_to_logs,
            allow_local_files=allow_local_files,
            allow_save_diagnostics_data_on_crash=allow_save_diagnostics_data_on_crash,
            question_uuid=question_uuid,
            push_endpoint=push_endpoint,
            timeout=timeout,
        )

        # Ignore any errors from the answering service as they will be raised on the remote service in practice, not
        # locally as is done in this mock.
        if input_manifest is not None:
            input_manifest = input_manifest.serialise()

        try:
            self.children[service_id].answer(
                MockMessage(
                    data=json.dumps(
                        {"input_values": input_values, "input_manifest": input_manifest, "children": children}
                    ).encode(),
                    question_uuid=question_uuid,
                    forward_logs=subscribe_to_logs,
                    octue_sdk_version=parent_sdk_version,
                    allow_save_diagnostics_data_on_crash=allow_save_diagnostics_data_on_crash,
                )
            )
        except Exception as e:  # noqa
            logger.exception(e)

        return response_subscription, question_uuid


class MockMessagePuller:
    """A mock message puller that enqueues messages in the message handler in the order they're provided on
    initialisation. This is meant for patching
    `octue.cloud.pub_sub.message_handler.OrderedMessageHandler._pull_and_enqueue_message` in tests.

    :param iter(dict) messages:
    :param octue.cloud.pub_sub.message_handler.OrderedMessageHandler message_handler:
    :return None:
    """

    def __init__(self, messages, message_handler):
        self.messages = messages
        self.message_handler = message_handler
        self.message_number = 0

    def pull(self, timeout, delivery_acknowledgement_timeout):
        """Get the next message from the messages given at instantiation and enqueue it for handling in the message
        handler.

        :return None:
        """
        message = self.messages[self.message_number]
        self.message_handler._waiting_messages[int(message["message_number"])] = message
        self.message_number += 1


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


def get_service_id(path):
    """Get the service ID (e.g. octue.services.<uuid>) from a topic or subscription path (e.g.
    projects/<project-name>/topics/octue.services.<uuid>)

    :param str path:
    :return str:
    """
    return path.split("/")[-1]
