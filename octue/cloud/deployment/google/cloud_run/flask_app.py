import logging

import google.api_core.exceptions
from flask import Flask, request

from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from octue.cloud.pub_sub.bigquery import get_events
from octue.configuration import DEFAULT_SERVICE_CONFIGURATION_PATH, ServiceConfiguration


logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """Receive questions from Google Cloud Run in the form of Google Pub/Sub messages.

    :return (str, int):
    """
    envelope = request.get_json()

    if not envelope:
        return _log_bad_request_and_return_400_response("No Pub/Sub message received.")

    if not isinstance(envelope, dict) or "message" not in envelope:
        return _log_bad_request_and_return_400_response(f"Invalid Pub/Sub message format - received {envelope!r}.")

    question = envelope["message"]

    if "data" not in question or "attributes" not in question or "question_uuid" not in question["attributes"]:
        return _log_bad_request_and_return_400_response(f"Invalid Pub/Sub message format - received {envelope!r}.")

    drop_question_response = _acknowledge_and_drop_redelivered_questions(
        question_uuid=question["attributes"].get("question_uuid"),
        retry_count=question["attributes"].get("retry_count"),
    )

    if drop_question_response:
        return drop_question_response

    project_name = envelope["subscription"].split("/")[1]
    answer_question(question=question, project_name=project_name)
    return ("", 204)


def _log_bad_request_and_return_400_response(message):
    """Log an error return a bad request (400) response.

    :param str message:
    :return (str, int):
    """
    logger.error(message)
    return (f"Bad Request: {message}", 400)


def _acknowledge_and_drop_redelivered_questions(question_uuid, retry_count):
    """Check if the question has been delivered before and, if it has, acknowledge and drop it. If it's a new question
    or if the question is an explicit retry (i.e. it's been received before but the retry count is unique), don't drop
    it. Non-dropped questions are added to the list of delivered questions.

    :param str question_uuid: the UUID of the question to check
    :param int retry_count: the retry count of the question to check
    :return (str, int)|None: an empty response with a 204 HTTP code if the question should be dropped
    """
    service_configuration = ServiceConfiguration.from_file(DEFAULT_SERVICE_CONFIGURATION_PATH)

    if not service_configuration.event_store_table_id:
        logger.warning(
            "Cannot check if question has been redelivered as the 'event_store_table_id' key hasn't been set in the "
            "service configuration (`octue.yaml` file)."
        )
        return

    try:
        previous_question_attempts = get_events(
            table_id=service_configuration.event_store_table_id,
            question_uuid=question_uuid,
            kind="delivery_acknowledgement",
        )

    except google.api_core.exceptions.NotFound:
        logger.warning(
            "Cannot check if question has been redelivered as no event store table was found with the ID %r; check "
            "that the 'event_store_table_id' key in the service configuration (`octue.yaml` file) is correct.",
            service_configuration.event_store_table_id,
        )
        return

    # If there are no events for this question UUID, assume this is the first attempt for the question.
    if not previous_question_attempts:
        logger.info("Question %r (retry count %s) is a new question.", question_uuid, retry_count)
        return

    # Acknowledge redelivered questions to stop further redundant redelivery and processing.
    for event in previous_question_attempts:
        if event["attributes"]["retry_count"] == retry_count:
            logger.warning(
                "Question %r (retry count %s) has already been received by the service. It will now be acknowledged to "
                "prevent further redundant redelivery and dropped.",
                question_uuid,
                retry_count,
            )
            return ("", 204)
