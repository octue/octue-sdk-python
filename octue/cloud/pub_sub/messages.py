import base64
import json

from octue.utils.decoders import OctueJSONDecoder
from octue.utils.objects import getattr_or_subscribe


def extract_event_and_attributes_from_pub_sub(message):
    # Cast attributes to dict to avoid defaultdict behaviour.
    attributes = dict(getattr_or_subscribe(message, "attributes"))
    is_question = bool(int(attributes["is_question"]))
    question_uuid = attributes["question_uuid"]
    message_number = int(attributes["message_number"])
    octue_sdk_version = attributes["octue_sdk_version"]

    try:
        forward_logs = {"forward_logs": bool(int(attributes["forward_logs"]))}
    except KeyError:
        forward_logs = {}

    try:
        allow_save_diagnostics_data_on_crash = {
            "allow_save_diagnostics_data_on_crash": bool(int(attributes["allow_save_diagnostics_data_on_crash"]))
        }
    except KeyError:
        allow_save_diagnostics_data_on_crash = {}

    try:
        # Parse event directly from Pub/Sub or Dataflow.
        event = json.loads(message.data.decode(), cls=OctueJSONDecoder)
    except Exception:
        # Parse event from Google Cloud Run.
        event = json.loads(base64.b64decode(message["data"]).decode("utf-8").strip(), cls=OctueJSONDecoder)

    return (
        event,
        {
            "is_question": is_question,
            "question_uuid": question_uuid,
            "octue_sdk_version": octue_sdk_version,
            "message_number": message_number,
            **forward_logs,
            **allow_save_diagnostics_data_on_crash,
        },
    )
