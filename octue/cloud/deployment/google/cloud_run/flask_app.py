import json
import logging

from flask import Flask, request

from octue.cloud.deployment.google.answer_pub_sub_question import answer_question


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
    delivered_questions = _load_metadata_file()

    # Acknowledge questions that are redelivered to stop further redelivery and redundant processing.
    if question_uuid in delivered_questions:
        return ("", 204)

    # Otherwise add the question UUID to the set.
    delivered_questions.add(question_uuid)
    _save_metadata_file(delivered_questions)

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


def _load_metadata_file():
    try:
        with open(".octue") as f:
            return set(json.load(f))

    except Exception as e:
        logger.exception(e)
        return []


def _save_metadata_file(data):
    with open(".octue", "w") as f:
        json.dump(data, f)
