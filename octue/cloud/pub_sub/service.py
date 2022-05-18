import base64
import concurrent.futures
import datetime
import functools
import json
import logging
import uuid

from google import auth
from google.api_core import retry
from google.cloud import pubsub_v1

import octue.exceptions
from octue.cloud.pub_sub import Subscription, Topic
from octue.cloud.pub_sub.logging import GooglePubSubHandler
from octue.cloud.pub_sub.message_handler import OrderedMessageHandler
from octue.mixins import CoolNameable
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.exceptions import convert_exception_to_primitives
from octue.utils.objects import get_nested_attribute


logger = logging.getLogger(__name__)

OCTUE_NAMESPACE = "octue.services"
ANSWERS_NAMESPACE = "answers"

# Switch message batching off by setting max_messages to 1. This minimises latency and is recommended for
# microservices publishing single messages in a request-response sequence.
BATCH_SETTINGS = pubsub_v1.types.BatchSettings(max_bytes=10 * 1000 * 1000, max_latency=0.01, max_messages=1)


class Service(CoolNameable):
    """A Twined service that can be used in two modes:
    - As a child accepting questions (input values and manifests) from parents, running them through its app, and
      responding with the results of the analysis
    - As a parent asking questions to children in the above mode

    Services communicate entirely via Google Pub/Sub and can ask and/or respond to questions from any other Service that
    has a corresponding topic on Google Pub/Sub.

    :param octue.resources.service_backends.ServiceBackend backend: the object representing the type of backend the service uses
    :param str|None service_id: a unique ID to give to the service (any string); a UUID is generated if none is given
    :param callable|None run_function: the function the service should run when it is called
    :return None:
    """

    def __init__(self, backend, service_id=None, run_function=None, *args, **kwargs):
        if service_id is None:
            self.id = f"{OCTUE_NAMESPACE}.{str(uuid.uuid4())}"
        elif not service_id:
            raise ValueError(f"service_id should be None or a non-falsey value; received {service_id!r} instead.")
        else:
            if service_id.startswith(OCTUE_NAMESPACE):
                self.id = service_id
            else:
                self.id = f"{OCTUE_NAMESPACE}.{service_id}"

            self.id = self._clean_service_id(self.id)

        self.backend = backend
        self.run_function = run_function
        self._credentials = auth.default()[0]
        self.publisher = pubsub_v1.PublisherClient(credentials=self._credentials, batch_settings=BATCH_SETTINGS)
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def serve(self, timeout=None, delete_topic_and_subscription_on_exit=False):
        """Start the Service as a server, waiting to accept questions from any other Service using Google Pub/Sub on
        the same Google Cloud Platform project. Questions are responded to asynchronously.

        :param float|None timeout: time in seconds after which to shut down the service
        :param bool delete_topic_and_subscription_on_exit: if `True`, delete the service's topic and subscription on exit
        :return None:
        """
        logger.info("Starting %r.", self)

        topic = Topic(name=self.id, namespace=OCTUE_NAMESPACE, service=self)
        subscriber = pubsub_v1.SubscriberClient(credentials=self._credentials)

        subscription = Subscription(
            name=self.id,
            topic=topic,
            namespace=OCTUE_NAMESPACE,
            project_name=self.backend.project_name,
            subscriber=subscriber,
            expiration_time=None,
        )

        try:
            topic.create(allow_existing=True)
            subscription.create(allow_existing=True)

            future = subscriber.subscribe(subscription=subscription.path, callback=self.answer)
            logger.debug("%r is waiting for questions.", self)

            try:
                future.result(timeout=timeout)
            except (TimeoutError, concurrent.futures.TimeoutError, KeyboardInterrupt):
                future.cancel()

        finally:
            if delete_topic_and_subscription_on_exit:
                try:
                    if subscription.exists():
                        subscription.delete()
                        logger.info("Subscription deleted.")

                    if topic.exists():
                        topic.delete()
                        logger.info("Topic deleted.")

                except Exception:
                    logger.error("Deletion of topic and/or subscription %r failed.", topic.name)

            subscriber.close()

    def answer(self, question, answer_topic=None, timeout=30):
        """Answer a question (i.e. run the Service's app to analyse the given data, and return the output values to the
        parent). Answers are published to a topic whose name is generated from the UUID sent with the question, and are
        in the format specified in the Service's Twine file.

        :param dict|Message question:
        :param octue.cloud.pub_sub.topic.Topic|None answer_topic: provide if messages need to be sent to the parent from outside the service (e.g. in octue.cloud.deployment.google.cloud_run.flask_app)
        :param float|None timeout: time in seconds to keep retrying sending of the answer once it has been calculated
        :raise Exception: if any exception arises during running analysis and sending its results
        :return None:
        """
        data, question_uuid, forward_logs = self._parse_question(question)
        topic = answer_topic or self.instantiate_answer_topic(question_uuid)
        self._send_delivery_acknowledgment(topic)

        if forward_logs:
            analysis_log_handler = GooglePubSubHandler(publisher=self.publisher, topic=topic, analysis_id=question_uuid)
        else:
            analysis_log_handler = None

        try:
            analysis = self.run_function(
                analysis_id=question_uuid,
                input_values=data["input_values"],
                input_manifest=data["input_manifest"],
                analysis_log_handler=analysis_log_handler,
                handle_monitor_message=functools.partial(self._send_monitor_message, topic=topic),
            )

            if analysis.output_manifest is None:
                serialised_output_manifest = None
            else:
                serialised_output_manifest = analysis.output_manifest.serialise()

            self.publisher.publish(
                topic=topic.path,
                data=json.dumps(
                    {
                        "type": "result",
                        "output_values": analysis.output_values,
                        "output_manifest": serialised_output_manifest,
                        "message_number": topic.messages_published,
                    },
                    cls=OctueJSONEncoder,
                ).encode(),
                retry=retry.Retry(deadline=timeout),
            )
            topic.messages_published += 1
            logger.info("%r answered question %r.", self, question_uuid)

        except BaseException as error:  # noqa
            self.send_exception(topic, timeout)
            raise error

    def instantiate_answer_topic(self, question_uuid, service_id=None):
        """Instantiate the answer topic for the given question UUID for the given service ID.

        :param str question_uuid:
        :param str|None service_id: the ID of the service to ask the question to
        :return octue.cloud.pub_sub.topic.Topic:
        """
        return Topic(
            name=".".join((service_id or self.id, ANSWERS_NAMESPACE, question_uuid)),
            namespace=OCTUE_NAMESPACE,
            service=self,
        )

    def ask(
        self,
        service_id,
        input_values=None,
        input_manifest=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        question_uuid=None,
        push_endpoint=None,
        timeout=30,
    ):
        """Ask a serving Service a question (i.e. send it input values for it to run its app on). The input values must
        be in the format specified by the serving Service's Twine file. A single-use topic and subscription are created
        before sending the question to the serving Service - the topic is the expected publishing place for the answer
        from the serving Service when it comes, and the subscription is set up to subscribe to this.

        :param str service_id: the UUID of the service to ask the question to
        :param any input_values: the input values of the question
        :param octue.resources.manifest.Manifest|None input_manifest: the input manifest of the question
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the remote service and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the serving service will have access to these local files
        :param str|None question_uuid: the UUID to use for the question if a specific one is needed; a UUID is generated if not
        :param str|None push_endpoint: if answers to the question should be pushed to an endpoint, provide its URL here; if they should be pulled, leave this as `None`
        :param float|None timeout: time in seconds to keep retrying sending the question
        :return (octue.cloud.pub_sub.subscription.Subscription, str): the answer subscription and question UUID
        """
        if not allow_local_files:
            if (input_manifest is not None) and (not input_manifest.all_datasets_are_in_cloud):
                raise octue.exceptions.FileLocationError(
                    "All datasets of the input manifest and all files of the datasets must be uploaded to the cloud "
                    "before asking a service to perform an analysis upon them. The manifest must then be updated with "
                    "the new cloud locations."
                )

        unlinted_service_id = service_id
        service_id = self._clean_service_id(service_id)
        question_topic = Topic(name=service_id, namespace=OCTUE_NAMESPACE, service=self)

        if not question_topic.exists(timeout=timeout):
            raise octue.exceptions.ServiceNotFound(f"Service with ID {unlinted_service_id!r} cannot be found.")

        question_uuid = question_uuid or str(uuid.uuid4())

        answer_topic = self.instantiate_answer_topic(question_uuid, service_id)
        answer_topic.create(allow_existing=False)

        answer_subscription = Subscription(
            name=answer_topic.name,
            topic=answer_topic,
            namespace=OCTUE_NAMESPACE,
            project_name=self.backend.project_name,
            subscriber=pubsub_v1.SubscriberClient(credentials=self._credentials),
            push_endpoint=push_endpoint,
        )
        answer_subscription.create(allow_existing=True)

        serialised_input_manifest = None
        if input_manifest is not None:
            serialised_input_manifest = input_manifest.serialise()

        self.publisher.publish(
            topic=question_topic.path,
            data=json.dumps({"input_values": input_values, "input_manifest": serialised_input_manifest}).encode(),
            question_uuid=question_uuid,
            forward_logs=str(int(subscribe_to_logs)),
            retry=retry.Retry(deadline=timeout),
        )

        logger.info("%r asked a question %r to service %r.", self, question_uuid, unlinted_service_id)
        return answer_subscription, question_uuid

    def wait_for_answer(
        self,
        subscription,
        handle_monitor_message=None,
        service_name="REMOTE",
        timeout=60,
        delivery_acknowledgement_timeout=120,
    ):
        """Wait for an answer to a question on the given subscription, deleting the subscription and its topic once
        the answer is received.

        :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription for the question's answer
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param str service_name: an arbitrary name to refer to the service subscribed to by (used for labelling its remote log messages)
        :param float|None timeout: how long in seconds to wait for an answer before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long in seconds to wait for a delivery acknowledgement before aborting
        :raise TimeoutError: if the timeout is exceeded
        :raise octue.exceptions.QuestionNotDelivered: if a delivery acknowledgement is not received in time
        :return dict: dictionary containing the keys "output_values" and "output_manifest"
        """
        if subscription.is_push_subscription:
            raise octue.exceptions.PushSubscriptionCannotBePulled(
                f"Cannot pull from {subscription.path!r} subscription as it is a push subscription."
            )

        subscriber = pubsub_v1.SubscriberClient(credentials=self._credentials)

        message_handler = OrderedMessageHandler(
            subscriber=subscriber,
            subscription=subscription,
            handle_monitor_message=handle_monitor_message,
            service_name=service_name,
        )

        try:
            return message_handler.handle_messages(
                timeout=timeout,
                delivery_acknowledgement_timeout=delivery_acknowledgement_timeout,
            )
        finally:
            subscription.delete()
            subscriber.close()

    def send_exception(self, topic, timeout=30):
        """Serialise and send the exception being handled to the parent.

        :param octue.cloud.pub_sub.topic.Topic topic:
        :param float|None timeout: time in seconds to keep retrying sending of the exception
        :return None:
        """
        exception = convert_exception_to_primitives()
        exception_message = f"Error in {self!r}: {exception['message']}"

        self.publisher.publish(
            topic=topic.path,
            data=json.dumps(
                {
                    "type": "exception",
                    "exception_type": exception["type"],
                    "exception_message": exception_message,
                    "traceback": exception["traceback"],
                    "message_number": topic.messages_published,
                }
            ).encode(),
            retry=retry.Retry(deadline=timeout),
        )

        topic.messages_published += 1

    def _clean_service_id(self, service_id):
        """Replace forward slashes in the given service ID with dots.

        :param str service_id: the raw service ID
        :return str: the cleaned service ID.
        """
        return service_id.replace("/", ".")

    def _send_delivery_acknowledgment(self, topic, timeout=30):
        """Send an acknowledgement of question delivery to the parent.

        :param octue.cloud.pub_sub.topic.Topic topic: topic to send acknowledgement to
        :param float timeout: time in seconds after which to give up sending
        :return None:
        """
        logger.info("%r acknowledged receipt of question.", self)

        self.publisher.publish(
            topic=topic.path,
            data=json.dumps(
                {
                    "type": "delivery_acknowledgement",
                    "delivery_time": str(datetime.datetime.now()),
                    "message_number": topic.messages_published,
                }
            ).encode(),
            retry=retry.Retry(deadline=timeout),
        )

        topic.messages_published += 1

    def _send_monitor_message(self, data, topic, timeout=30):
        """Send a monitor message to the parent.

        :param any data: the data to send as a monitor message
        :param octue.cloud.pub_sub.topic.Topic topic: the topic to send the message to
        :param float timeout: time in seconds to retry sending the message
        :return None:
        """
        logger.debug("%r sending monitor message.", self)

        self.publisher.publish(
            topic=topic.path,
            data=json.dumps(
                {
                    "type": "monitor_message",
                    "data": json.dumps(data),
                    "message_number": topic.messages_published,
                }
            ).encode(),
            retry=retry.Retry(deadline=timeout),
        )

        topic.messages_published += 1

    def _parse_question(self, question):
        """Parse a question in the Google Cloud Pub/Sub or Google Cloud Run format.

        :param dict|Message question:
        :return (dict, str, bool):
        """
        try:
            # Parse question directly from Pub/Sub or Dataflow.
            data = json.loads(question.data.decode())

            # Acknowledge it if it's directly from Pub/Sub
            if hasattr(question, "ack"):
                question.ack()

        except Exception:
            # Parse question from Google Cloud Run.
            data = json.loads(base64.b64decode(question["data"]).decode("utf-8").strip())

        logger.info("%r received a question.", self)

        question_uuid = get_nested_attribute(question, "attributes.question_uuid")
        forward_logs = bool(int(get_nested_attribute(question, "attributes.forward_logs")))
        return data, question_uuid, forward_logs
