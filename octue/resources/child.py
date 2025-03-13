import concurrent.futures
import copy
import logging
import os

from octue.cloud.pub_sub.service import Service
from octue.definitions import DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL
from octue.resources import service_backends

logger = logging.getLogger(__name__)

BACKEND_TO_SERVICE_MAPPING = {"GCPPubSubBackend": Service}


class Child:
    """A class representing an Octue child service that can be asked questions. This is a convenience wrapper for
    `Service` that makes asking questions more intuitive and allows easier selection of backends.

    :param str id: the ID of the child
    :param dict backend: must include the key "name" with a value of the name of the type of backend e.g. "GCPPubSubBackend" and key-value pairs for any other parameters the chosen backend expects
    :param str internal_sruid: the SRUID to give to the internal service used to ask questions to the child
    :param iter(dict)|None service_registries: the names and endpoints of the registries used to resolve the child's service revision when asking it questions; these should be in priority order (highest priority first)
    :return None:
    """

    def __init__(self, id, backend, internal_sruid="local/local:local", service_registries=None):
        self.id = id

        backend = copy.deepcopy(backend)
        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)

        self._service = BACKEND_TO_SERVICE_MAPPING[backend_type_name](
            service_id=internal_sruid,
            backend=backend,
            service_registries=service_registries,
        )

    def __repr__(self):
        """Represent the child as a string.

        :return str:
        """
        return f"<{type(self).__name__}({self.id!r})>"

    @property
    def received_events(self):
        """Get the events received from the child if it has been asked a question. If it hasn't, `None` is returned.
        If an empty list is returned, no messages have been received.

        :return list(dict)|None:
        """
        return self._service.received_events

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        children=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        handle_monitor_message=None,
        record_events=True,
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
        raise_errors=True,
        max_retries=0,
        prevent_retries_when=None,
        log_errors=True,
        timeout=86400,
        maximum_heartbeat_interval=DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL,
    ):
        """Ask the child either:
        - A synchronous (ask-and-wait) question and wait for it to return an output. Questions are synchronous if
          the `push_endpoint` isn't provided and `asynchronous=False`.
        - An asynchronous (fire-and-forget) question and return immediately. To make a question asynchronous, provide
          the `push_endpoint` argument or set `asynchronous=True`.

        :param any|None input_values: any input values for the question, conforming with the schema in the child's twine
        :param octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question, conforming with the schema in the child's twine
        :param list(dict)|None children: a list of children for the child to use instead of its default children (if it uses children). These should be in the same format as in an app's app configuration file and have the same keys.
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the child and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the child will have access to these local files
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param bool record_events: if `True`, record messages received from the child in the `received_events` property
        :param str save_diagnostics: must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}; if turned on, allow the input values and manifest (and its datasets) to be saved by the child either all the time or just if it fails while processing them
        :param str|None question_uuid: the UUID to use for the question if a specific one is needed; a UUID is generated if not
        :param str|None parent_question_uuid: the UUID of the question that triggered this question
        :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question; if `None`, this question is assumed to be the originator question
        :param str|None originator: the SRUID of the service revision that triggered all ancestor questions of this question;  if `None`, this service revision is assumed to be the originator
        :param str|None push_endpoint: if answers to the question should be pushed to an endpoint, provide its URL here (the returned subscription will be a push subscription); if not, leave this as `None`
        :param bool asynchronous: if `True`, don't wait for an answer or create an answer subscription (the result and other events can be retrieved from the event store later)
        :param int retry_count: the retry count of the question (this is zero if it's the first attempt at the question)
        :param int|None cpus: the number of CPUs to request for the question; defaults to the number set by the child service
        :param str|None memory: the amount of memory to request for the question e.g. "256Mi" or "1Gi"; defaults to the amount set by the child service
        :param str|None ephemeral_storage: the amount of ephemeral storage to request for the question e.g. "256Mi" or "1Gi"; defaults to the amount set by the child service
        :param bool raise_errors: if `True` and the question fails, raise the error; if False, return the error in place of the answer
        :param int max_retries: if `raise_errors=False` and the question fails, retry the question up to this number of times
        :param list(type)|None prevent_retries_when: if `raise_errors=False` and the question fails, prevent retrying the question if it fails with an exception type in this list
        :param bool log_errors: if `True`, `raise_errors=False`, and the question fails after its final retry, log the error
        :param float timeout: time to wait for an answer before raising a timeout error [s]
        :param float|int maximum_heartbeat_interval: the maximum amount of time (in seconds) allowed between child heartbeats before an error is raised
        :raise TimeoutError: if the timeout is exceeded while waiting for an answer
        :raise Exception: if the question raises an error and `raise_errors=True`
        :return dict|octue.cloud.pub_sub.subscription.Subscription|Exception|None, str: for a synchronous question, a dictionary containing the keys "output_values" and "output_manifest" from the result (or just an exception if the question fails), and the question UUID; for a question with a push endpoint, the push subscription and the question UUID; for an asynchronous question, `None` and the question UUID
        """
        prevent_retries_when = prevent_retries_when or []

        inputs = {
            "input_values": input_values,
            "input_manifest": input_manifest,
            "children": children,
            "subscribe_to_logs": subscribe_to_logs,
            "allow_local_files": allow_local_files,
            "save_diagnostics": save_diagnostics,
            "question_uuid": question_uuid,
            "parent_question_uuid": parent_question_uuid,
            "originator_question_uuid": originator_question_uuid,
            "originator": originator,
            "push_endpoint": push_endpoint,
            "asynchronous": asynchronous,
            "retry_count": retry_count,
            "cpus": cpus,
            "memory": memory,
            "ephemeral_storage": ephemeral_storage,
            "timeout": timeout,
        }

        subscription, question_uuid = self._service.ask(service_id=self.id, **inputs)

        if push_endpoint or asynchronous:
            return subscription, question_uuid

        logger.info("Waiting for question to be accepted...")

        try:
            answer = self._service.wait_for_answer(
                subscription=subscription,
                handle_monitor_message=handle_monitor_message,
                record_events=record_events,
                timeout=timeout,
                maximum_heartbeat_interval=maximum_heartbeat_interval,
            )

            return answer, question_uuid

        except Exception as e:
            logger.error(
                "Question %r failed. Run 'octue question diagnostics gs://<diagnostics-cloud-path>/%s "
                "--download-datasets' to get the crash diagnostics.",
                question_uuid,
                question_uuid,
            )

            if raise_errors:
                raise e

            if type(e) in prevent_retries_when:
                logger.info("Skipping retries for exceptions of type %r.", type(e))
                return e, question_uuid

            for retry in range(max_retries):
                logger.info("Retrying question %r %d of %d times.", question_uuid, retry + 1, max_retries)

                inputs["retry_count"] += 1
                answer, question_uuid = self.ask(**inputs, raise_errors=False, log_errors=False)

                if not isinstance(answer, Exception) or type(answer) in prevent_retries_when:
                    return answer, question_uuid

                e = answer

            if log_errors:
                logger.error(
                    "Question %r failed after %d retries (see below for error).",
                    question_uuid,
                    max_retries,
                    exc_info=e,
                )

            return e, question_uuid

    def ask_multiple(
        self,
        *questions,
        raise_errors=True,
        max_retries=0,
        prevent_retries_when=None,
        max_workers=None,
        log_errors=True,
    ):
        """Ask the child multiple questions in parallel and wait for the answers. Each question should be provided as a
        dictionary of `Child.ask` keyword arguments. The `raise_errors`, `max_retries`, `prevent_retries_when`, and
        `log_errors` arguments have the same effect as in `Child.ask`, applied to all questions. These values may be
        overridden on a per-question basis by specifying them in the question dictionary.

        :param questions: any number of questions provided as dictionaries of arguments to the `Child.ask` method
        :param bool raise_errors: if `True`, an error is raised and no answers are returned if any of the individual questions raise an error; if `False`, answers are returned for all successful questions while errors are returned unraised for any failed ones
        :param int max_retries: retry any questions that failed up to this number of times (note: this will have no effect unless `raise_errors=False`)
        :param list(type)|None prevent_retries_when: prevent retrying any questions that fail with an exception type in this list (note: this will have no effect unless `raise_errors=False`)
        :param int|None max_workers: the maximum number of questions that can be asked at once; defaults to the lowest of {32, no. of CPUs + 4, and no. of questions} (see `concurrent.futures.ThreadPoolExecutor`)
        :param bool log_errors: if `True` and `raise_errors=False`, log any errors remaining once retries are exhausted
        :return list((dict|octue.cloud.pub_sub.subscription.Subscription|Exception|None, str)): the answers to the questions and the question UUIDs (in the same order as asked)
        """
        # Answers will come out of order, so use a dictionary to store them against their questions' original index.
        answers = {}
        n_questions = len(questions)
        max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4, n_questions)
        logger.info("Asking %d questions with maximum %d threads.", n_questions, max_workers)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_question_index_mapping = {}

            for i, question in enumerate(questions):
                # Set retry/error parameters if they're not given in the individual questions.
                question["raise_errors"] = question.get("raise_errors", raise_errors)
                question["max_retries"] = question.get("max_retries", max_retries)
                question["prevent_retries_when"] = question.get("prevent_retries_when", prevent_retries_when)
                question["log_errors"] = question.get("log_errors", log_errors)

                future = executor.submit(self.ask, **question)
                future_to_question_index_mapping[future] = i

            for i, future in enumerate(concurrent.futures.as_completed(future_to_question_index_mapping)):
                logger.info("%d of %d answers received.", i + 1, n_questions)
                question_index = future_to_question_index_mapping[future]
                answers[question_index] = future.result()

        # Convert dictionary to list in asking order.
        return [answer[1] for answer in sorted(answers.items(), key=lambda item: item[0])]

    # def cancel(self, question_uuid, event_store_table_id, timeout=30):
    #     """Request cancellation of a running question.
    #
    #     :param str question_uuid: the question UUID of the question to cancel
    #     :param str event_store_table_id: the full ID of the Google BigQuery table used as the event store e.g. "your-project.your-dataset.your-table"
    #     :param float timeout: time to wait for the cancellation to send before raising a timeout error [s]
    #     :return None:
    #     """
    #     self._service.cancel(question_uuid=question_uuid, event_store_table_id=event_store_table_id, timeout=timeout)
