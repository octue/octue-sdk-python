import json

from octue.cloud.pub_sub.service import Service


MESSAGES = {}


class MockTopic:
    def __init__(self, name, namespace, service):
        if name.startswith(namespace):
            self.name = name
        else:
            self.name = f"{namespace}.{name}"

        self.service = service
        self.path = f"projects/{service.backend.project_name}/topics/{self.name}"

    def create(self, allow_existing=False):
        if not allow_existing:
            if self.exists():
                return

        if not self.exists():
            MESSAGES[self.path] = None

    def delete(self):
        del MESSAGES[self.path]

    def exists(self):
        return self.path in MESSAGES


class MockSubscription:
    def __init__(self, name, topic, namespace, service):
        if name.startswith(namespace):
            self.name = name
        else:
            self.name = f"{namespace}.{name}"

        self.topic = topic
        self.service = service
        self.path = f"projects/{service.backend.project_name}/subscriptions/{self.name}"

    def create(self, allow_existing=False):
        pass

    def delete(self):
        pass

    def exists(self):
        return self.path in MESSAGES


class MockFuture:
    """A mock future that contains the subscription path for the corresponding answer.

    :param str subscription_path:
    :return None
    """

    def __init__(self, subscription_path):
        self.subscription_path = subscription_path

    def result(self, timeout=None):
        return self.subscription_path

    def cancel(self):
        pass


class MockSubscriber:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def subscribe(self, subscription, callback):
        return MockFuture(subscription_path=subscription)

    def pull(self, request, timeout=None, retry=None):
        return MockPullResponse(
            received_messages=[MockMessageWrapper(message=MockMessage(data=MESSAGES[request["subscription"]]))]
        )

    def acknowledge(self, request):
        pass


class MockPullResponse:
    def __init__(self, received_messages=None):
        self.received_messages = received_messages or []


class MockMessageWrapper:
    def __init__(self, message):
        self.message = message
        self.ack_id = None


class MockMessage:
    def __init__(self, data):
        self.data = data


class MockPublisher:
    def publish(self, topic, data, retry=None, **attributes):
        MESSAGES[topic] = {
            "data": data,
            "attributes": attributes,
        }
        return MockFuture(None)


class MockService(Service):
    """A mock Google Pub/Sub Service that can send and receive messages synchronously to other instances.

    :param octue.resources.service_backends.GCPPubSubBackEnd backend:
    :param str id:
    :param callable run_function:
    :param list(MockService) children:
    """

    def __init__(self, backend, id=None, run_function=None, children=None):
        super().__init__(backend, id, run_function)
        self.children = children or []
        self.publisher = MockPublisher()
        self.subscriber = MockSubscriber()

    def ask(self, service_id, input_values, input_manifest=None):
        """Put the question into the messages register, register the existence of the corresponding response topic, add
        the response to the register, and return a MockFuture containing the answer subscription path.

        :param str service_id:
        :param dict input_values:
        :param octue.resources.manifest.Manifest input_manifest:
        :return MockFuture, str:
        """
        response_subscription, question_uuid = super().ask(service_id, input_values, input_manifest)
        analysis = self._get_child(service_id).run_function(input_values, input_manifest)

        if analysis.output_manifest is None:
            serialised_output_manifest = None
        else:
            serialised_output_manifest = analysis.output_manifest.serialise(to_string=True)

        MESSAGES[response_subscription.path] = json.dumps(
            {"output_values": analysis.output_values, "output_manifest": serialised_output_manifest}
        ).encode()

        return response_subscription, question_uuid

    def _get_child(self, service_id):
        """Get the correct responding MockService from the children.

        :param str service_id:
        :return MockService:
        """
        for child in self.children:
            if child.id == service_id:
                return child

        return None
