import base64
import json

from octue.utils.decoders import OctueJSONDecoder
from octue.utils.objects import getattr_or_subscribe


def extract_event_and_attributes_from_pub_sub(message):
    """Extract an Octue service event and its attributes from a Google Pub/Sub message in either direct Pub/Sub format
    or in the Google Cloud Run format.

    :param dict|google.cloud.pubsub_v1.subscriber.message.Message message: the message in Google Cloud Run format or Google Pub/Sub format
    :return (any, dict): the extracted event and its attributes
    """
    # Cast attributes to a dictionary to avoid defaultdict-like behaviour from Pub/Sub message attributes container.
    attributes = dict(getattr_or_subscribe(message, "attributes"))

    converted_attributes = {
        "sender_type": attributes["sender_type"],
        "question_uuid": attributes["question_uuid"],
        "message_number": int(attributes["message_number"]),
        "version": attributes["version"],
    }

    if "forward_logs" in attributes:
        converted_attributes["forward_logs"] = bool(int(attributes["forward_logs"]))

    if "save_diagnostics" in attributes:
        converted_attributes["save_diagnostics"] = attributes["save_diagnostics"]

    try:
        # Parse event directly from Pub/Sub or Dataflow.
        event = json.loads(message.data.decode(), cls=OctueJSONDecoder)
    except Exception:
        # Parse event from Google Cloud Run.
        event = json.loads(base64.b64decode(message["data"]).decode("utf-8").strip(), cls=OctueJSONDecoder)

    return event, converted_attributes
