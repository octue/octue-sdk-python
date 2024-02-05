import concurrent.futures
import copy
import logging
import os

from octue.cloud.pub_sub.service import Service
from octue.resources import service_backends


logger = logging.getLogger(__name__)

BACKEND_TO_SERVICE_MAPPING = {"GCPPubSubBackend": Service}


class Child:
    """A class representing an Octue child service that can be asked questions. This is a convenience wrapper for
    `Service` that makes asking questions more intuitive and allows easier selection of backends.

    :param str id: the ID of the child
    :param dict backend: must include the key "name" with a value of the name of the type of backend e.g. "GCPPubSubBackend" and key-value pairs for any other parameters the chosen backend expects
    :param str internal_service_name: the name to give to the internal service used to ask questions to the child
    :param iter(dict)|None service_registries: the names and endpoints of the registries used to resolve the child's service revision when asking it questions; these should be in priority order (highest priority first)
    :return None:
    """

    def __init__(self, id, backend, internal_service_name="local/local:local", service_registries=None):
        self.id = id

        backend = copy.deepcopy(backend)
        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)

        self._service = BACKEND_TO_SERVICE_MAPPING[backend_type_name](
            name=internal_service_name,
            backend=backend,
            service_registries=service_registries,
        )

    def __repr__(self):
        """Represent the child as a string.

        :return str:
        """
        return f"<{type(self).__name__}({self.id!r})>"

    @property
    def received_messages(self):
        """Get the messages received from the child if it has been asked a question. If it hasn't, `None` is returned.
        If an empty list is returned, no messages have been received.

        :return list(dict)|None:
        """
        return self._service.received_messages

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        children=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        handle_monitor_message=None,
        record_messages=True,
        save_diagnostics="SAVE_DIAGNOSTICS_ON_CRASH",  # This is repeated as a string here to avoid a circular import.
        question_uuid=None,
        timeout=86400,
        maximum_heartbeat_interval=300,
    ):
        """Ask the child a question and wait for its answer - i.e. send it input values and/or an input manifest and
        wait for it to analyse them and return output values and/or an output manifest. The input values and manifest
        must conform to the schema in the child's twine.

        :param any|None input_values: any input values for the question
        :param octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question
        :param list(dict)|None children: a list of children for the child to use instead of its default children (if it uses children). These should be in the same format as in an app's app configuration file and have the same keys.
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the child and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the child will have access to these local files
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param bool record_messages: if `True`, record messages received from the child in the `received_messages` property
        :param str save_diagnostics: must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}; if turned on, allow the input values and manifest (and its datasets) to be saved by the child either all the time or just if it fails while processing them
        :param str|None question_uuid: the UUID to use for the question if a specific one is needed; a UUID is generated if not
        :param float timeout: time in seconds to wait for an answer before raising a timeout error
        :param float|int maximum_heartbeat_interval: the maximum amount of time (in seconds) allowed between child heartbeats before an error is raised
        :raise TimeoutError: if the timeout is exceeded while waiting for an answer
        :return dict: a dictionary containing the keys "output_values" and "output_manifest"
        """
        subscription, _ = self._service.ask(
            service_id=self.id,
            input_values=input_values,
            input_manifest=input_manifest,
            children=children,
            subscribe_to_logs=subscribe_to_logs,
            allow_local_files=allow_local_files,
            save_diagnostics=save_diagnostics,
            question_uuid=question_uuid,
            timeout=timeout,
        )

        return self._service.wait_for_answer(
            subscription=subscription,
            handle_monitor_message=handle_monitor_message,
            record_messages=record_messages,
            timeout=timeout,
            maximum_heartbeat_interval=maximum_heartbeat_interval,
        )

    def ask_multiple(self, *questions, raise_errors=True, max_retries=0, prevent_retries_when=None, max_workers=None):
        """Ask the child multiple questions in parallel and wait for the answers. Each question should be provided as a
        dictionary of `Child.ask` keyword arguments. If `raise_errors` is `True`, an error is raised and no answers are
        returned if any of the individual questions raise an error; if it's `False`, answers are returned for all
        successful questions while errors are returned unraised for any failed ones.

        :param questions: any number of questions provided as dictionaries of arguments to the `Child.ask` method
        :param bool raise_errors: if `True`, an error is raised and no answers are returned if any of the individual questions raise an error; if `False`, answers are returned for all successful questions while errors are returned unraised for any failed ones
        :param int max_retries: retry any questions that failed up to this number of times (note: this will have no effect unless `raise_errors=False`)
        :param list(type)|None prevent_retries_when: prevent retrying any questions that fail with an exception type in this list (note: this will have no effect unless `raise_errors=False`)
        :param int|None max_workers: the maximum number of questions that can be asked at once; defaults to `min(32, os.cpu_count() + 4, len(questions))` (see `concurrent.futures.ThreadPoolExecutor`)
        :raise ValueError: if the maximum number of parallel questions is set too high
        :raise Exception: if any question raises an error if `raise_errors` is `True`
        :return list: the answers or caught errors of the questions in the same order as asked
        """
        prevent_retries_when = prevent_retries_when or []

        # Answers will come out of order, so use a dictionary to store them against their questions' original index.
        answers = {}
        max_workers = max_workers or min(32, os.cpu_count() + 4, len(questions))
        logger.info("Asking %d questions.", len(questions))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_question_index_mapping = {
                executor.submit(self.ask, **question): i for i, question in enumerate(questions)
            }

            for i, future in enumerate(concurrent.futures.as_completed(future_to_question_index_mapping)):
                logger.info("%d of %d answers received.", i + 1, len(questions))
                question_index = future_to_question_index_mapping[future]

                try:
                    answers[question_index] = future.result()
                except Exception as e:
                    if raise_errors:
                        raise e

                    answers[question_index] = e
                    logger.exception("Question %d failed.", question_index)

        for retry in range(max_retries):
            failed_questions = {}

            for question_index, answer in answers.items():
                if isinstance(answer, Exception) and type(answer) not in prevent_retries_when:
                    failed_questions[question_index] = questions[question_index]

            if not failed_questions:
                break

            logger.info("%d questions failed - retrying.", len(failed_questions))
            retried_answers = self.ask_multiple(*failed_questions.values(), raise_errors=False)

            for question_index, answer in zip(failed_questions.keys(), retried_answers):
                answers[question_index] = answer

        # Convert dictionary to list in asking order.
        return [answer[1] for answer in sorted(answers.items(), key=lambda item: item[0])]
