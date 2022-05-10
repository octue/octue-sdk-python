import logging

from flask import Flask, request

from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from octue.utils.metadata import load_local_metadata_file, overwrite_local_metadata_file


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
        return _log_bad_request_and_return_400_response("Invalid Pub/Sub message format.")

    question = envelope["message"]

    if "data" not in question or "attributes" not in question or "question_uuid" not in question["attributes"]:
        return _log_bad_request_and_return_400_response("Invalid Pub/Sub message format.")

    question_uuid = question["attributes"]["question_uuid"]

    local_metadata = load_local_metadata_file()
    delivered_questions = local_metadata.get("delivered_questions", set())

    # Acknowledge questions that are redelivered to stop further redelivery and redundant processing.
    if question_uuid in delivered_questions:
        logger.info(
            "Question %r has already been received by the service. It will now be acknowledged to prevent further "
            "redundant redelivery.",
            question_uuid,
        )
        return ("", 204)

    # Otherwise add the question UUID to the set.
    delivered_questions.add(question_uuid)
    overwrite_local_metadata_file(local_metadata)

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
