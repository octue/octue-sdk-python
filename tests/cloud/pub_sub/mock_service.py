from octue.cloud.pub_sub.service import Service


MESSAGES = {}


class MockTopic:
    """A mock topic that registers in a global dictionary rather than Google Pub/Sub.

    :param str name:
    :param str namespace:
    :param MockService service:
    :return None:
    """

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
    """A mock subscription that registers in a global dictionary rather than Google Pub/Sub.

    :param str name:
    :param MockTopic topic:
    :param str namespace:
    :param MockService service:
    :return None:
    """

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


class MockFuture:
    """A mock future that is returned when publishing or subscribing.

    :return None:
    """

    def result(self, timeout=None):
        pass

    def cancel(self):
        pass


class MockSubscriber:
    """A mock subscriber that gets messages from a global dictionary instead of Google Pub/Sub.

    :return None:
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def subscribe(self, subscription, callback):
        return MockFuture()

    def pull(self, request, timeout=None, retry=None):
        return MockPullResponse(
            received_messages=[MockMessageWrapper(message=MockMessage(**MESSAGES[request["subscription"]]))]
        )

    def acknowledge(self, request):
        pass


class MockPullResponse:
    """A mock PullResponse that is returned by `MockSubscriber.pull`.

    :param iter|None received_messages:
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
        for key, value in attributes.items():
            setattr(self, key, value)


class MockPublisher:
    """A mock publisher that puts messages in a global dictionary instead of Google Pub/Sub.

    :return None:
    """

    def publish(self, topic, data, retry=None, **attributes):
        subscription = topic.replace("topics", "subscriptions")
        MESSAGES[subscription] = {"data": data, "attributes": attributes}
        return MockFuture()


class MockService(Service):
    """A mock Google Pub/Sub Service that can send and receive messages synchronously to other instances.

    :param octue.resources.service_backends.GCPPubSubBackEnd backend:
    :param str id:
    :param callable run_function:
    :param dict(str, MockService)|None children:
    :return None:
    """

    def __init__(self, backend, id=None, run_function=None, children=None):
        super().__init__(backend, id, run_function)
        self.children = children or {}
        self.publisher = MockPublisher()
        self.subscriber = MockSubscriber()

    def ask(self, service_id, input_values, input_manifest=None):
        """Put the question into the messages register, register the existence of the corresponding response topic, add
        the response to the register, and return a MockFuture containing the answer subscription path.

        :param str service_id:
        :param dict|list input_values:
        :param octue.resources.manifest.Manifest|None input_manifest:
        :return MockFuture, str:
        """
        response_subscription, question_uuid = super().ask(service_id, input_values, input_manifest)

        self.children[service_id].answer(
            data={"input_values": input_values, "input_manifest": input_manifest}, question_uuid=question_uuid
        )

        return response_subscription, question_uuid
