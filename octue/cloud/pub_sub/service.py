import concurrent.futures
import copy
import functools
import json
import logging
import uuid

from google.api_core import retry
import google.api_core.exceptions
from google.cloud import pubsub_v1
import jsonschema

from octue.cloud.events import OCTUE_SERVICES_TOPIC_NAME
from octue.cloud.events.attributes import CHILD_SENDER_TYPE, PARENT_SENDER_TYPE, QuestionAttributes, ResponseAttributes
from octue.cloud.events.validation import raise_if_event_is_invalid
from octue.cloud.pub_sub import Subscription, Topic
from octue.cloud.pub_sub.events import GoogleCloudPubSubEventHandler, extract_event
from octue.cloud.pub_sub.logging import GoogleCloudPubSubHandler
from octue.cloud.registry import get_default_sruid, raise_if_revision_not_registered
from octue.cloud.service_id import (
    convert_service_id_to_pub_sub_form,
    create_sruid,
    split_service_id,
    validate_sruid,
)
from octue.compatibility import warn_if_incompatible
from octue.definitions import DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL, LOCAL_SDK_VERSION
import octue.exceptions
from octue.utils.dictionaries import make_minimal_dictionary
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.exceptions import convert_exception_to_primitives
from octue.utils.objects import get_nested_attribute
from octue.utils.threads import RepeatingTimer

logger = logging.getLogger(__name__)


ANSWERS_NAMESPACE = "answers"

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
    :param iter(dict)|None service_registries: the names and endpoints of the registries used to resolve service revisions when asking questions; these should be in priority order (highest priority first)
    :return None:
    """

    def __init__(self, backend, service_id=None, run_function=None, service_registries=None):
        if service_id is None:
            self.id = create_sruid()

        # Raise an error if the service ID is some kind of falsey object that isn't `None`.
        elif not service_id:
            raise ValueError(f"`service_id` should be `None` or a non-falsey value; received {service_id!r} instead.")

        else:
            validate_sruid(service_id)
            self.id = service_id

        self.backend = backend
        self.run_function = run_function
        self.service_registries = service_registries

        self._pub_sub_id = convert_service_id_to_pub_sub_form(self.id)
        self._event_handler = None

    def __repr__(self):
        """Represent the service as a string.

        :return str: the service represented as a string
        """
        return f"<{type(self).__name__}({self.id!r})>"

    @functools.cached_property
    def publisher(self):
        """Get or instantiate the publisher client for the service. No publisher is instantiated until this property is
        called for the first time. This allows checking for the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to
        be put off until it's needed.

        :return google.cloud.pubsub_v1.PublisherClient:
        """
        return pubsub_v1.PublisherClient(
            batch_settings=BATCH_SETTINGS,
            publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True),
        )

    @functools.cached_property
    def services_topic(self):
        """Get the Octue services topic that all events in the project are published to. No topic is instantiated until
        this property is called for the first time. This allows checking for the `GOOGLE_APPLICATION_CREDENTIALS`
        environment variable to be put off until it's needed.

        :raise octue.exceptions.ServiceNotFound: if the topic doesn't exist in the project
        :return octue.cloud.pub_sub.topic.Topic: the Octue services topic for the project
        """
        topic = Topic(name=OCTUE_SERVICES_TOPIC_NAME, project_name=self.backend.project_name)

        if not topic.exists():
            raise octue.exceptions.ServiceNotFound(
                f"The {topic!r} topic cannot be found. Check that it's been created for this service network."
            )

        return topic

    @property
    def received_events(self):
        """Get the events received from a child service while running the `wait_for_answer` method. If the
        `wait_for_answer` method hasn't been run, `None` is returned. If an empty list is returned, no events have been
        received.

        :return list(dict)|None:
        """
        if self._event_handler:
            return self._event_handler.handled_events
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

        subscription = Subscription(
            name=self._pub_sub_id,
            topic=self.services_topic,
            filter=f'attributes.recipient = "{self.id}" AND attributes.sender_type = "{PARENT_SENDER_TYPE}"',
            expiration_time=None,
        )

        try:
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

                    except Exception:
                        logger.error("Deletion of %r failed.", subscription)

                subscriber.close()

        return future, subscriber

    def answer(self, question, heartbeat_interval=120, timeout=30):
        """Answer a question from a parent - i.e. run the child's app on the given data and return the output values.
        Answers conform to the output values and output manifest schemas specified in the child's Twine file.

        :param dict|google.cloud.pubsub_v1.subscriber.message.Message question:
        :param int|float heartbeat_interval: the time interval, in seconds, at which to send heartbeats
        :param float|None timeout: time in seconds to keep retrying sending of the answer once it has been calculated
        :raise Exception: if any exception arises during running analysis and sending its results
        :return dict: the result event
        """
        try:
            question, question_attributes = self._parse_question(question)
        except jsonschema.ValidationError:
            return

        heartbeater = None
        response_attributes = ResponseAttributes.from_question_attributes(question_attributes)

        try:
            self._send_delivery_acknowledgment(response_attributes)

            heartbeater = RepeatingTimer(
                interval=heartbeat_interval,
                function=self._send_heartbeat,
                kwargs={"attributes": response_attributes},
            )

            heartbeater.daemon = True
            heartbeater.start()

            if question_attributes.forward_logs:
                analysis_log_handler = GoogleCloudPubSubHandler(
                    event_emitter=self._emit_event,
                    attributes=response_attributes,
                )
            else:
                analysis_log_handler = None

            handle_monitor_message = functools.partial(self._send_monitor_message, attributes=response_attributes)

            analysis = self.run_function(
                analysis_id=question_attributes.question_uuid,
                input_values=question.get("input_values"),
                input_manifest=question.get("input_manifest"),
                children=question.get("children"),
                analysis_log_handler=analysis_log_handler,
                handle_monitor_message=handle_monitor_message,
                save_diagnostics=question_attributes.save_diagnostics,
                originator_question_uuid=question_attributes.originator_question_uuid,
                originator=question_attributes.originator,
            )

            result = self._send_result(analysis, response_attributes)
            heartbeater.cancel()
            logger.info("%r answered question %r.", self, question_attributes.question_uuid)
            return result

        except BaseException as error:  # noqa
            if heartbeater is not None:
                heartbeater.cancel()

            warn_if_incompatible(
                recipient_sdk_version=LOCAL_SDK_VERSION,
                sender_sdk_version=question_attributes.sender_sdk_version,
            )

            self.send_exception(attributes=response_attributes, timeout=timeout)
            raise error

    def ask(
        self,
        service_id,
        input_values=None,
        input_manifest=None,
        children=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        save_diagnostics="SAVE_DIAGNOSTICS_ON_CRASH",  # This is repeated as a string here to avoid a circular import.
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
    ):
        """Ask a child a question (i.e. send it input values for it to analyse and produce output values for) and return
        a subscription to receive its answer on. The input values and manifest must conform to the schemas in the
        child's Twine file.

        :param str service_id: the ID of the child to ask the question to
        :param any|None input_values: any input values for the question
        :param dict|octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question
        :param list(dict)|None children: a list of children for the child to use instead of its default children (if it uses children). These should be in the same format as in an app's app configuration file and have the same keys.
        :param bool subscribe_to_logs: if `True`, subscribe to the child's logs and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the child will be able to access these local files
        :param str save_diagnostics: must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}; if turned on, allow the input values and manifest (and its datasets) to be saved by the child either all the time or just if it fails while processing them
        :param str|None question_uuid: the UUID to use for the question; if `None`, a UUID is generated
        :param str|None parent_question_uuid: the UUID of the question that triggered this question; this should be `None` if this is the first question in a question tree
        :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question; if `None`, this question is assumed to be the originator question
        :param str|None originator: the SRUID of the service revision that triggered all ancestor questions of this question; if `None`, this service revision is assumed to the the originator
        :param str|None push_endpoint: if answers to the question should be pushed to an endpoint, provide its URL here (the returned subscription will be a push subscription); if not, leave this as `None`
        :param bool asynchronous: if `True` and not using a push endpoint, don't create an answer subscription
        :param int retry_count: the retry count of the question (this is zero if it's the first attempt at the question)
        :param int|None cpus: the number of CPUs to request for the question; defaults to the number set by the child service
        :param str|None memory: the amount of memory to request for the question e.g. "256Mi" or "1Gi"; defaults to the amount set by the child service
        :param str|None ephemeral_storage: the amount of ephemeral storage to request for the question e.g. "256Mi" or "1Gi"; defaults to the amount set by the child service
        :param float|None timeout: time in seconds to keep retrying sending the question
        :return (octue.cloud.pub_sub.subscription.Subscription|None, str): the answer subscription (if the question is synchronous or a push endpoint was used) and question UUID
        """
        service_namespace, service_name, service_revision_tag = split_service_id(service_id)

        # If using a service registry, check that the service revision is registered, or get the default service
        # revision if no revision tag is provided.
        if self.service_registries:
            if service_revision_tag:
                raise_if_revision_not_registered(sruid=service_id, service_registries=self.service_registries)
            else:
                service_id = get_default_sruid(
                    namespace=service_namespace,
                    name=service_name,
                    service_registries=self.service_registries,
                )

        elif not service_revision_tag:
            raise octue.exceptions.InvalidServiceID(
                f"A service revision tag for {service_id!r} must be provided if service registries aren't being used."
            )

        if not allow_local_files:
            if (input_manifest is not None) and (not input_manifest.all_datasets_are_in_cloud):
                raise octue.exceptions.FileLocationError(
                    "All datasets of the input manifest and all files of the datasets must be uploaded to the cloud "
                    "before asking a service to perform an analysis upon them. The manifest must then be updated with "
                    "the new cloud locations."
                )

        question_uuid = question_uuid or str(uuid.uuid4())

        # If the originator question UUID isn't provided, assume that this question is the originator question.
        originator_question_uuid = originator_question_uuid or question_uuid

        # If the originator isn't provided, assume that this service revision is the originator.
        originator = originator or self.id

        if asynchronous and not push_endpoint:
            answer_subscription = None
        else:
            answer_subscription = Subscription(
                name=".".join((self._pub_sub_id, ANSWERS_NAMESPACE, question_uuid)),
                topic=self.services_topic,
                filter=(
                    f'attributes.recipient = "{self.id}" '
                    f'AND attributes.question_uuid = "{question_uuid}" '
                    f'AND attributes.sender_type = "{CHILD_SENDER_TYPE}"'
                ),
                push_endpoint=push_endpoint,
            )

            if retry_count > 0:
                allow_existing = True
            else:
                allow_existing = False

            answer_subscription.create(allow_existing=allow_existing)

        self._send_question(
            input_values=input_values,
            input_manifest=input_manifest,
            children=children,
            forward_logs=subscribe_to_logs,
            save_diagnostics=save_diagnostics,
            question_uuid=question_uuid,
            parent_question_uuid=parent_question_uuid,
            originator_question_uuid=originator_question_uuid,
            originator=originator,
            recipient=service_id,
            cpus=cpus,
            memory=memory,
            ephemeral_storage=ephemeral_storage,
            retry_count=retry_count,
        )

        return answer_subscription, question_uuid

    def wait_for_answer(
        self,
        subscription,
        handle_monitor_message=None,
        record_events=True,
        timeout=60,
        maximum_heartbeat_interval=DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL,
    ):
        """Wait for an answer to a question on the given subscription, deleting the subscription and its topic once
        the answer is received.

        :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription for the question's answer
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param bool record_events: if `True`, record messages received from the child in the `received_events` attribute
        :param float|None timeout: how long in seconds to wait for an answer before raising a `TimeoutError`
        :param float|int maximum_heartbeat_interval: the maximum amount of time (in seconds) allowed between child heartbeats before an error is raised
        :raise TimeoutError: if the timeout is exceeded
        :return dict: dictionary containing the keys "output_values" and "output_manifest"
        """
        if subscription.is_push_subscription:
            raise octue.exceptions.NotAPullSubscription(
                f"{subscription.path!r} is a push subscription so it cannot be waited on for an answer. Please check "
                f"its push endpoint at {subscription.push_endpoint!r}."
            )

        self._event_handler = GoogleCloudPubSubEventHandler(
            subscription=subscription,
            handle_monitor_message=handle_monitor_message,
            record_events=record_events,
        )

        try:
            return self._event_handler.handle_events(
                timeout=timeout,
                maximum_heartbeat_interval=maximum_heartbeat_interval,
            )

        finally:
            subscription.delete()

    # def cancel(self, question_uuid, event_store_table_id, timeout=30):
    #     """Request cancellation of a running question.
    #
    #     :param str question_uuid: the question UUID of the question to cancel
    #     :param str event_store_table_id: the full ID of the Google BigQuery table used as the event store e.g. "your-project.your-dataset.your-table"
    #     :param float timeout: time to wait for the cancellation to send before raising a timeout error [s]
    #     :raise ValueError: if no question or more than one question is found for the given question UUID
    #     :return None:
    #     """
    #     questions = get_events(table_id=event_store_table_id, question_uuid=question_uuid, kinds=["question"])
    #
    #     if len(questions) == 0:
    #         raise ValueError(f"No question found with question UUID {question_uuid!r}.")
    #
    #     if len(questions) > 1:
    #         raise ValueError(f"Multiple questions found with same question UUID {question_uuid!r}.")
    #
    #     question_finished = get_events(
    #         table_id=event_store_table_id,
    #         question_uuid=question_uuid,
    #         kinds=["result", "exception"],
    #     )
    #
    #     if question_finished:
    #         logger.warning("Cannot cancel question %r - it has already finished.", question_uuid)
    #
    #     question_attributes = EventAttributes(**questions[0]["attributes"])
    #     self._emit_event({"kind": "cancellation"}, attributes=question_attributes, timeout=timeout)
    #     logger.info("Cancellation of question %r requested.", question_uuid)

    def send_exception(self, attributes, timeout=30):
        """Serialise and send the exception being handled to the parent.

        :param octue.cloud.events.attributes.ResponseAttributes attributes: the attributes to use for the exception event
        :param float|None timeout: time in seconds to keep retrying sending of the exception
        :return None:
        """
        exception = convert_exception_to_primitives()
        exception_message = f"Error in {self!r}: {exception['message']}"

        self._emit_event(
            {
                "kind": "exception",
                "exception_type": exception["type"],
                "exception_message": exception_message,
                "exception_traceback": exception["traceback"],
            },
            attributes=attributes,
            timeout=timeout,
        )

    def _emit_event(self, event, attributes, wait=True, timeout=30):
        """Emit a JSON-serialised event as a Pub/Sub message to the services topic with optional message attributes.
        Extra attributes can be added to an event via the `attributes` argument but the following attributes are always
        included:
        - `uuid` (event UUID)
        - `question_uuid`
        - `parent_question_uuid`
        - `originator_question_uuid`
        - `parent`
        - `originator`
        - `sender`
        - `sender_sdk_version`
        - `recipient`
        - `retry_count`
        - `datetime`

        :param dict event: JSON-serialisable data to emit as an event
        :param octue.cloud.events.attributes.ResponseAttributes attributes: the attributes to use for the event
        :param bool wait: if `True`, wait for the result of the publishing future before continuing execution (this is important if the python process ends promptly after the event is emitted instead of being part of a prolonged stream as the publishing may not complete and the event won't actually be emitted)
        :param int|float timeout: the timeout for sending the event in seconds
        :return google.cloud.pubsub_v1.publisher.futures.Future:
        """
        attributes.reset_uuid_and_datetime()

        future = self.publisher.publish(
            topic=self.services_topic.path,
            data=json.dumps(event, cls=OctueJSONEncoder).encode(),
            ordering_key=attributes.question_uuid,
            retry=retry.Retry(deadline=timeout),
            **attributes.to_serialised_attributes(),
        )

        if wait:
            future.result()

        return future

    def _send_question(
        self,
        input_values,
        input_manifest,
        children,
        forward_logs,
        save_diagnostics,
        question_uuid,
        parent_question_uuid,
        originator_question_uuid,
        originator,
        recipient,
        retry_count,
        cpus,
        memory,
        ephemeral_storage,
        timeout=30,
    ):
        """Send a question to a child service.

        :param any|None input_values: any input values for the question
        :param octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question
        :param list(dict)|None children: a list of children for the child to use instead of its default children (if it uses children). These should be in the same format as in an app's app configuration file and have the same keys.
        :param bool forward_logs: whether to request the child to forward its logs
        :param str save_diagnostics: must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}; if turned on, allow the input values and manifest (and its datasets) to be saved by the child either all the time or just if it fails while processing them
        :param str question_uuid: the UUID of the question being sent
        :param str|None parent_question_uuid: the UUID of the question that triggered this question
        :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question
        :param str originator: the SRUID of the service revision that triggered all ancestor questions of this question
        :param str recipient: the SRUID of the child the question is intended for
        :param int retry_count: the retry count of the question (this is zero if it's the first attempt at the question)
        :param float timeout: time in seconds after which to give up sending
        :return None:
        """
        question = make_minimal_dictionary(kind="question", input_values=input_values, children=children)

        if input_manifest is not None:
            input_manifest.use_signed_urls_for_datasets()
            question["input_manifest"] = input_manifest.to_primitive()

        question_attributes = QuestionAttributes(
            question_uuid=question_uuid,
            parent_question_uuid=parent_question_uuid,
            originator_question_uuid=originator_question_uuid,
            parent=self.id,
            originator=originator,
            sender=self.id,
            recipient=recipient,
            retry_count=retry_count,
            forward_logs=forward_logs,
            save_diagnostics=save_diagnostics,
            cpus=cpus,
            memory=memory,
            ephemeral_storage=ephemeral_storage,
        )

        self._emit_event(event=question, attributes=question_attributes, timeout=timeout)
        logger.info("%r asked a question %r to service %r.", self, question_uuid, recipient)

    def _send_delivery_acknowledgment(self, attributes, timeout=30):
        """Send an acknowledgement of question receipt to the parent.

        :param octue.cloud.events.attributes.ResponseAttributes attributes: the attributes to use for the delivery acknowledgement event
        :param float timeout: time in seconds after which to give up sending
        :return None:
        """
        self._emit_event({"kind": "delivery_acknowledgement"}, attributes=attributes, timeout=timeout, wait=False)
        logger.info("%r acknowledged receipt of question %r.", self, attributes.question_uuid)

    def _send_heartbeat(self, attributes, timeout=30):
        """Send a heartbeat to the parent, indicating that the service is alive.

        :param octue.cloud.events.attributes.ResponseAttributes attributes: the attributes to use for the heartbeat event
        :param float timeout: time in seconds after which to give up sending
        :return None:
        """
        self._emit_event({"kind": "heartbeat"}, attributes=attributes, timeout=timeout, wait=False)
        logger.debug("Heartbeat sent by %r.", self)

    def _send_monitor_message(self, data, attributes, timeout=30):
        """Send a monitor message to the parent.

        :param any data: the data to send as a monitor message
        :param octue.cloud.events.attributes.ResponseAttributes attributes: the attributes to use for the monitor message event
        :param float timeout: time in seconds to retry sending the message
        :return None:
        """
        self._emit_event({"kind": "monitor_message", "data": data}, attributes=attributes, timeout=timeout, wait=False)
        logger.debug("Monitor message sent by %r.", self)

    def _send_result(self, analysis, attributes, timeout=30):
        """Send the result to the parent.

        :param octue.resources.analysis.Analysis analysis: the analysis object containing the output values and/or output manifest
        :param octue.cloud.events.attributes.ResponseAttributes attributes: the attributes to use for the result event
        :param float timeout: time in seconds to retry sending the message
        :return dict: the result
        """
        result = make_minimal_dictionary(kind="result", output_values=analysis.output_values)

        if analysis.output_manifest is not None:
            result["output_manifest"] = analysis.output_manifest.to_primitive()

        self._emit_event(event=result, attributes=attributes, timeout=timeout)
        return result

    def _parse_question(self, question):
        """Parse a question in dictionary format or direct Google Pub/Sub format.

        :param dict|google.cloud.pubsub_v1.subscriber.message.Message question: the question to parse in dictionary format or direct Google Pub/Sub format
        :return (dict, octue.cloud.events.attributes.QuestionAttributes): the question's event and its attributes
        """
        logger.info("%r received a question.", self)

        # Acknowledge the question if it's directly from Pub/Sub.
        if hasattr(question, "ack"):
            question.ack()
            logger.info("Question acknowledged on Pub/Sub.")

        event = extract_event(question)
        attributes = QuestionAttributes.from_serialised_attributes(get_nested_attribute(question, "attributes"))
        logger.info("Extracted question event and attributes.")

        raise_if_event_is_invalid(event=copy.deepcopy(event), attributes=attributes, recipient=self.id)
        logger.info("%r parsed question %r successfully.", self, attributes.question_uuid)

        if attributes.retry_count > 0:
            logger.warning("This is retry %d for question %r.", attributes.retry_count, attributes.question_uuid)

        return event, attributes
