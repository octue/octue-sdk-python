import copy
import logging
from unittest.mock import patch

from octue.resources import Analysis, service_backends
from tests.cloud.pub_sub.mocks import MockService, MockSubscriber, MockSubscription, MockTopic


logger = logging.getLogger(__name__)


class ChildEmulator:
    def __init__(self, id, backend, internal_service_name=None, messages=None):
        self.id = id
        self.messages = messages or []

        backend = copy.deepcopy(backend)
        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)

        self._child = MockService(service_id=self.id, backend=backend, run_function=self.emulate_messages)

        self._parent = MockService(
            backend=backend,
            service_id=internal_service_name or f"{self.id}-parent",
            children={self._child.id: self._child},
        )

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        handle_monitor_message=None,
        question_uuid=None,
        timeout=86400,
    ):
        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                    with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                        self._child.serve()

                        subscription, _ = self._parent.ask(
                            service_id=self._child.id,
                            input_values=input_values,
                            input_manifest=input_manifest,
                            subscribe_to_logs=subscribe_to_logs,
                            allow_local_files=allow_local_files,
                            question_uuid=question_uuid,
                        )

                        return self._parent.wait_for_answer(
                            subscription,
                            handle_monitor_message=handle_monitor_message,
                            service_name=self.id,
                            timeout=timeout,
                        )

    def emulate_messages(self, analysis_id, input_values, input_manifest, analysis_log_handler, handle_monitor_message):
        for message in self.messages:
            if message["type"] == "log_record":
                logger.handle(message["content"])

            elif message["type"] == "monitor_message":
                handle_monitor_message(message["content"])

            elif message["type"] == "exception":
                raise message["content"]

            elif message["type"] == "result":
                return Analysis(
                    id=analysis_id,
                    twine={},
                    handle_monitor_message=handle_monitor_message,
                    input_values=input_values,
                    input_manifest=input_manifest,
                    output_values=message["content"]["output_values"],
                    output_manifest=message["content"]["output_manifest"],
                )
