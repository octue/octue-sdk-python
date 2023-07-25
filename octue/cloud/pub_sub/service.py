import base64
import concurrent.futures
import datetime
import functools
import importlib.metadata
import json
import logging
import threading
import uuid

import google.api_core.exceptions
from google.api_core import retry
from google.cloud import pubsub_v1

import octue.exceptions
from octue.cloud.pub_sub import Subscription, Topic
from octue.cloud.pub_sub.logging import GooglePubSubHandler
from octue.cloud.pub_sub.message_handler import OrderedMessageHandler
from octue.cloud.service_id import (
    convert_service_id_to_pub_sub_form,
    create_sruid,
    get_default_sruid,
    split_service_id,
    validate_sruid,
)
from octue.compatibility import warn_if_incompatible
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.exceptions import convert_exception_to_primitives
from octue.utils.objects import get_nested_attribute
from octue.utils.threads import RepeatingTimer


logger = logging.getLogger(__name__)
send_message_lock = threading.Lock()

DEFAULT_NAMESPACE = "default"
ANSWERS_NAMESPACE = "answers"
OCTUE_SERVICE_REGISTRY_ENDPOINT = "services.registry.octue.com"

# Switch message batching off by setting `max_messages` to 1. This minimises latency and is recommended for
# microservices publishing single messages in a request-response sequence.
BATCH_SETTINGS = pubsub_v1.types.BatchSettings(max_bytes=10 * 1000 * 1000, max_latency=0.01, max_messages=1)


class Service:
    """An Octue service that can be used as a data service or digital twin in one of two modes:

    - As a child accepting questions (input values and manifests) from parents, running them through its app, and
      responding with an answer
    - As a parent asking questions to children in the above mode

    Services communicate entirely via Google Pub/Sub and can ask and/or respond to questions from any other service that
    has a corresponding topic on Google Pub/Sub.

    :param octue.resources.service_backends.ServiceBackend backend: the object representing the type of backend the service uses
    :param str|None service_id: a unique ID to give to the service (any string); a UUID is generated if none is given
    :param callable|None run_function: the function the service should run when it is called
    :param str|None name: an optional name to use for the service to override its ID in its string representation
    :param iter(dict)|None service_registries: the names and endpoints of the registries used to resolve service revisions when asking questions; these should be in priority order (highest priority first)
    :return None:
    """

    def __init__(self, backend, service_id=None, run_function=None, name=None, service_registries=None):
        if service_id is None:
            self.id = create_sruid(namespace=DEFAULT_NAMESPACE, name=str(uuid.uuid4()))

        # Raise an error if the service ID is some kind of falsey object that isn't `None`.
        elif not service_id:
            raise ValueError(f"`service_id` should be `None` or a non-falsey value; received {service_id!r} instead.")

        else:
            validate_sruid(service_id)
            self.id = service_id

        self.backend = backend
        self.run_function = run_function
        self.name = name

        self.service_registries = service_registries or [
            {
                "name": "Octue Registry",
                "endpoint": OCTUE_SERVICE_REGISTRY_ENDPOINT,
            },
        ]

        self._pub_sub_id = convert_service_id_to_pub_sub_form(self.id)
        self._local_sdk_version = importlib.metadata.version("octue")
        self._topic = None
        self._publisher = None
        self._message_handler = None

    def __repr__(self):
        return f"<{type(self).__name__}({self.name or self.id!r})>"

    @property
    def publisher(self):
        """Get or instantiate the publisher client for the service. No publisher is instantiated until this property is
        called for the first time. This allows checking for the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to
        be put off until it's needed.

        :return google.cloud.pubsub_v1.PublisherClient:
        """
        if not self._publisher:
            self._publisher = pubsub_v1.PublisherClient(batch_settings=BATCH_SETTINGS)

        return self._publisher

    @property
    def received_messages(self):
        """Get the messages received by the service from a child service while running the `wait_for_answer` method. If
        the `wait_for_answer` method hasn't been run, `None` is returned. If an empty list is returned, no messages have
        been received.

        :return list(dict)|None:
        """
        if self._message_handler:
            return self._message_handler.handled_messages
        return None

    def serve(self, timeout=None, delete_topic_and_subscription_on_exit=False, allow_existing=False, detach=False):
        """Start the service as a child, waiting to accept questions from any other Octue service using Google Pub/Sub
        on the same Google Cloud project. Questions are accepted, processed, and answered asynchronously.

        :param float|None timeout: time in seconds after which to shut down the child
        :param bool delete_topic_and_subscription_on_exit: if `True`, delete the child's topic and subscription on exiting serving mode
        :param bool allow_existing: if `True`, allow starting a service for which the topic and/or subscription already exists (indicating an existing service) - this connects this service to the existing service's topic and subscription
        :param bool detach: if `True`, detach from the subscription future. The future and subscriber are returned so can still be cancelled and closed manually. Note that the topic and subscription are not automatically deleted on exit if this option is chosen.
        :return (google.cloud.pubsub_v1.subscriber.futures.StreamingPullFuture, google.cloud.pubsub_v1.SubscriberClient):
        """
        logger.info("Starting %r.", self)

        self._topic = Topic(name=self._pub_sub_id, project_name=self.backend.project_name)

        subscription = Subscription(
            name=self._pub_sub_id,
            topic=self._topic,
            project_name=self.backend.project_name,
            expiration_time=None,
        )

        try:
            self._topic.create(allow_existing=allow_existing)
            subscription.create(allow_existing=allow_existing)
        except google.api_core.exceptions.AlreadyExists:
            raise octue.exceptions.ServiceAlreadyExists(f"A service with the ID {self.id!r} already exists.")

        subscriber = pubsub_v1.SubscriberClient()

        try:
            future = subscriber.subscribe(subscription=subscription.path, callback=self.answer)

            logger.info(
                "You can now ask this service questions at %r using the `octue.resources.Child` class.",
                self.id,
            )

            # If not detaching, keep answering questions until the subscriber times out (or forever if there's no
            # timeout).
            if not detach:
                try:
                    future.result(timeout=timeout)
                except (TimeoutError, concurrent.futures.TimeoutError, KeyboardInterrupt):
                    future.cancel()

        finally:
            # If not detaching, delete the topic and subscription deletion if required and close the subscriber.
            if not detach:
                if delete_topic_and_subscription_on_exit:
                    try:
                        if subscription.creation_triggered_locally:
                            subscription.delete()

                        if self._topic.creation_triggered_locally:
                            self._topic.delete()

                    except Exception:
                        logger.error("Deletion of topic and/or subscription %r failed.", self._topic.name)

                subscriber.close()

        return future, subscriber

    def answer(self, question, heartbeat_interval=120, timeout=30):
        """Answer a question from a parent - i.e. run the child's app on the given data and return the output values.
        Answers conform to the output values and output manifest schemas specified in the child's Twine file.

        :param dict|Message question:
        :param int|float heartbeat_interval: the time interval, in seconds, at which to send heartbeats
        :param float|None timeout: time in seconds to keep retrying sending of the answer once it has been calculated
        :raise Exception: if any exception arises during running analysis and sending its results
        :return None:
        """
        (
            data,
            question_uuid,
            forward_logs,
            parent_sdk_version,
            allow_save_diagnostics_data_on_crash,
        ) = self._parse_question(question)

        self._send_delivery_acknowledgment(self._topic)

        heartbeater = RepeatingTimer(
            interval=heartbeat_interval,
            function=self._send_heartbeat,
            kwargs={"topic": self._topic},
        )

        heartbeater.daemon = True
        heartbeater.start()

        try:
            if forward_logs:
                analysis_log_handler = GooglePubSubHandler(
                    message_sender=self._send_message,
                    topic=self._topic,
                    analysis_id=question_uuid,
                )
            else:
                analysis_log_handler = None

            analysis = self.run_function(
                analysis_id=question_uuid,
                input_values=data["input_values"],
                input_manifest=data["input_manifest"],
                children=data.get("children"),
                analysis_log_handler=analysis_log_handler,
                handle_monitor_message=functools.partial(self._send_monitor_message, topic=self._topic),
                allow_save_diagnostics_data_on_crash=allow_save_diagnostics_data_on_crash,
            )

            if analysis.output_manifest is None:
                serialised_output_manifest = None
            else:
                serialised_output_manifest = analysis.output_manifest.serialise()

            self._send_message(
                {
                    "type": "result",
                    "output_values": analysis.output_values,
                    "output_manifest": serialised_output_manifest,
                },
                topic=self._topic,
                timeout=timeout,
            )

            heartbeater.cancel()
            logger.info("%r answered question %r.", self, question_uuid)

        except BaseException as error:  # noqa
            heartbeater.cancel()
            warn_if_incompatible(child_sdk_version=self._local_sdk_version, parent_sdk_version=parent_sdk_version)
            self.send_exception(self._topic, timeout)
            raise error

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
    ):
        """Ask a child a question (i.e. send it input values for it to analyse and produce output values for) and return
        a subscription to receive its answer on. The input values and manifest must conform to the schemas in the
        child's Twine file.

        :param str service_id: the ID of the child to ask the question to
        :param any|None input_values: any input values for the question
        :param octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question
        :param list(dict)|None children: a list of children for the child to use instead of its default children (if it uses children). These should be in the same format as in an app's app configuration file and have the same keys.
        :param bool subscribe_to_logs: if `True`, subscribe to the child's logs and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the child will be able to access these local files
        :param bool allow_save_diagnostics_data_on_crash: if `True`, allow the input values and manifest (and its datasets) to be saved by the child if it fails while processing them
        :param str|None question_uuid: the UUID to use for the question if a specific one is needed; a UUID is generated if not
        :param str|None push_endpoint: if answers to the question should be pushed to an endpoint, provide its URL here; if they should be pulled, leave this as `None`
        :param float|None timeout: time in seconds to keep retrying sending the question
        :return (octue.cloud.pub_sub.subscription.Subscription, str): the answer subscription and question UUID
        """
        service_namespace, service_name, service_revision_tag = split_service_id(service_id)

        if not service_revision_tag:
            service_id = get_default_sruid(
                namespace=service_namespace,
                name=service_name,
                service_registries=self.service_registries,
            )

        if not allow_local_files:
            if (input_manifest is not None) and (not input_manifest.all_datasets_are_in_cloud):
                raise octue.exceptions.FileLocationError(
                    "All datasets of the input manifest and all files of the datasets must be uploaded to the cloud "
                    "before asking a service to perform an analysis upon them. The manifest must then be updated with "
                    "the new cloud locations."
                )

        pub_sub_service_id = convert_service_id_to_pub_sub_form(service_id)
        topic = Topic(name=pub_sub_service_id, project_name=self.backend.project_name)

        if not topic.exists(timeout=timeout):
            raise octue.exceptions.ServiceNotFound(f"Service with ID {service_id!r} cannot be found.")

        question_uuid = question_uuid or str(uuid.uuid4())

        answer_subscription = Subscription(
            name=topic.name,
            topic=topic,
            project_name=self.backend.project_name,
            push_endpoint=push_endpoint,
        )
        answer_subscription.create(allow_existing=False)

        if input_manifest is not None:
            input_manifest.use_signed_urls_for_datasets()
            input_manifest = input_manifest.to_primitive()

        self._send_message(
            {"input_values": input_values, "input_manifest": input_manifest, "children": children},
            topic=topic,
            question_uuid=question_uuid,
            forward_logs=subscribe_to_logs,
            allow_save_diagnostics_data_on_crash=allow_save_diagnostics_data_on_crash,
        )

        logger.info("%r asked a question %r to service %r.", self, question_uuid, service_id)
        return answer_subscription, question_uuid

    def wait_for_answer(
        self,
        subscription,
        handle_monitor_message=None,
        record_messages=True,
        service_name="REMOTE",
        timeout=60,
        maximum_heartbeat_interval=300,
    ):
        """Wait for an answer to a question on the given subscription, deleting the subscription and its topic once
        the answer is received.

        :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription for the question's answer
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param bool record_messages: if `True`, record messages received from the child in the `received_messages` attribute
        :param str service_name: a name by which to refer to the child subscribed to (used for labelling its log messages if subscribed to)
        :param float|None timeout: how long in seconds to wait for an answer before raising a `TimeoutError`
        :param float|int delivery_acknowledgement_timeout: how long in seconds to wait for a delivery acknowledgement before aborting
        :param float|int maximum_heartbeat_interval: the maximum amount of time (in seconds) allowed between child heartbeats before an error is raised
        :raise TimeoutError: if the timeout is exceeded
        :return dict: dictionary containing the keys "output_values" and "output_manifest"
        """
        if subscription.is_push_subscription:
            raise octue.exceptions.PushSubscriptionCannotBePulled(
                f"{subscription.path!r} is a push subscription so it cannot be waited on for an answer. Please check "
                f"its push endpoint at {subscription.push_endpoint!r}."
            )

        self._message_handler = OrderedMessageHandler(
            subscription=subscription,
            receiving_service=self,
            handle_monitor_message=handle_monitor_message,
            service_name=service_name,
            record_messages=record_messages,
        )

        try:
            return self._message_handler.handle_messages(
                timeout=timeout,
                maximum_heartbeat_interval=maximum_heartbeat_interval,
            )

        finally:
            subscription.delete()

    def send_exception(self, topic, timeout=30):
        """Serialise and send the exception being handled to the parent.

        :param octue.cloud.pub_sub.topic.Topic topic:
        :param float|None timeout: time in seconds to keep retrying sending of the exception
        :return None:
        """
        exception = convert_exception_to_primitives()
        exception_message = f"Error in {self!r}: {exception['message']}"

        self._send_message(
            {
                "type": "exception",
                "exception_type": exception["type"],
                "exception_message": exception_message,
                "traceback": exception["traceback"],
            },
            topic=topic,
            timeout=timeout,
        )

    def _send_message(self, message, topic, timeout=30, **attributes):
        """Send a JSON-serialised message to the given topic with optional message attributes.

        :param dict message: JSON-serialisable data to send as a message
        :param octue.cloud.pub_sub.topic.Topic topic: the Pub/Sub topic to send the message to
        :param int|float timeout: the timeout for sending the message in seconds
        :param attributes: key-value pairs to attach to the message - the values must be strings or bytes
        :return None:
        """
        with send_message_lock:
            attributes["octue_sdk_version"] = self._local_sdk_version

            # This would be better placed in the Pub/Sub message's attributes but has been left in `message` for
            # inter-service backwards compatibility.
            message["message_number"] = topic.messages_published
            converted_attributes = {}

            for key, value in attributes.items():
                if isinstance(value, bool):
                    value = str(int(value))
                elif isinstance(value, (int, float)):
                    value = str(value)

                converted_attributes[key] = value

            self.publisher.publish(
                topic=topic.path,
                data=json.dumps(message, cls=OctueJSONEncoder).encode(),
                retry=retry.Retry(deadline=timeout),
                **converted_attributes,
            )

            topic.messages_published += 1

    def _send_delivery_acknowledgment(self, topic, timeout=30):
        """Send an acknowledgement of question receipt to the parent.

        :param octue.cloud.pub_sub.topic.Topic topic: topic to send the acknowledgement to
        :param float timeout: time in seconds after which to give up sending
        :return None:
        """
        self._send_message(
            {
                "type": "delivery_acknowledgement",
                "delivery_time": str(datetime.datetime.now()),
            },
            topic=topic,
            timeout=timeout,
        )

        logger.info("%r acknowledged receipt of question.", self)

    def _send_heartbeat(self, topic, timeout=30):
        """Send a heartbeat to the parent, indicating that the service is alive.

        :param octue.cloud.pub_sub.topic.Topic topic: topic to send the heartbeat to
        :param float timeout: time in seconds after which to give up sending
        :return None:
        """
        self._send_message(
            {
                "type": "heartbeat",
                "time": str(datetime.datetime.now()),
            },
            topic=topic,
            timeout=timeout,
        )

        logger.debug("Heartbeat sent by %r.", self)

    def _send_monitor_message(self, data, topic, timeout=30):
        """Send a monitor message to the parent.

        :param any data: the data to send as a monitor message
        :param octue.cloud.pub_sub.topic.Topic topic: the topic to send the message to
        :param float timeout: time in seconds to retry sending the message
        :return None:
        """
        self._send_message(
            {
                "type": "monitor_message",
                "data": json.dumps(data, cls=OctueJSONEncoder),
            },
            topic=topic,
            timeout=timeout,
        )

        logger.debug("Monitor message sent by %r.", self)

    def _parse_question(self, question):
        """Parse a question in the Google Cloud Pub/Sub or Google Cloud Run format.

        :param dict|Message question:
        :return (dict, str, bool, str|None, bool):
        """
        logger.info("%r received a question.", self)

        try:
            # Parse question directly from Pub/Sub or Dataflow.
            data = json.loads(question.data.decode(), cls=OctueJSONDecoder)

            # Acknowledge it if it's directly from Pub/Sub
            if hasattr(question, "ack"):
                question.ack()

        except Exception:
            # Parse question from Google Cloud Run.
            data = json.loads(base64.b64decode(question["data"]).decode("utf-8").strip(), cls=OctueJSONDecoder)

        # Keep backwards compatibility with questions from Octue services running `octue<0.41.1`.
        if isinstance(data["input_manifest"], str):
            data["input_manifest"] = json.loads(data["input_manifest"], cls=OctueJSONDecoder)

        question_uuid = get_nested_attribute(question, "attributes.question_uuid")
        forward_logs = bool(int(get_nested_attribute(question, "attributes.forward_logs")))

        try:
            parent_sdk_version = get_nested_attribute(question, "attributes.octue_sdk_version")
        except AttributeError:
            parent_sdk_version = None

        try:
            allow_save_diagnostics_data_on_crash = get_nested_attribute(
                question,
                "attributes.allow_save_diagnostics_data_on_crash",
            )
        except AttributeError:
            allow_save_diagnostics_data_on_crash = False

        logger.info("%r parsed the question successfully.", self)
        return data, question_uuid, forward_logs, parent_sdk_version, allow_save_diagnostics_data_on_crash
