from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter

from octue.cloud.events.validation import VALID_EVENT_KINDS
from octue.exceptions import ServiceNotFound
from octue.resources import Manifest


def get_events(table_id, sender, question_uuid, kind=None, include_backend_metadata=False, limit=1000):
    """Get Octue service events for a question from a sender from a Google BigQuery event store.

    :param str table_id: the full ID of the table e.g. "your-project.your-dataset.your-table"
    :param str sender: the SRUID of the sender of the events
    :param str question_uuid: the UUID of the question to get the events for
    :param str|None kind: the kind of event to get; if `None`, all event kinds are returned
    :param bool include_backend_metadata: if `True`, include the service backend metadata
    :param int limit: the maximum number of events to return
    :raise ValueError: if the `kind` parameter is invalid
    :raise octue.exceptions.ServiceNotFound: if the sender hasn't emitted any events related to the question UUID (or any events at all)
    :return list(dict): the events for the question
    """
    if kind:
        if kind not in VALID_EVENT_KINDS:
            raise ValueError(f"`kind` must be one of {VALID_EVENT_KINDS!r}; received {kind!r}.")

        event_kind_condition = [f"AND kind={kind!r}"]
    else:
        event_kind_condition = []

    client = Client()

    fields = [
        "`event`",
        "`kind`",
        "`datetime`",
        "`uuid`",
        "`originator`",
        "`sender`",
        "`sender_type`",
        "`sender_sdk_version`",
        "`recipient`",
        "`order`",
        "`other_attributes`",
    ]

    if include_backend_metadata:
        fields.extend(("`backend`", "`backend_metadata`"))

    query = "\n".join(
        [
            f"SELECT {', '.join(fields)} FROM `{table_id}`",
            "WHERE sender=@sender",
            "AND question_uuid=@question_uuid",
            *event_kind_condition,
            "ORDER BY `order`",
            "LIMIT @limit",
        ]
    )

    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("sender", "STRING", sender),
            ScalarQueryParameter("question_uuid", "STRING", question_uuid),
            ScalarQueryParameter("limit", "INTEGER", limit),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    if result.total_rows == 0:
        raise ServiceNotFound(
            f"No events found. The requested sender {sender!r} may not exist or it hasn't emitted any events for "
            f"question {question_uuid!r} (or any events at all)."
        )

    df = result.to_dataframe()
    df["event"].apply(_deserialise_manifest_if_present)

    events = df.to_dict(orient="records")
    return _unflatten_events(events)


def _deserialise_manifest_if_present(event):
    """If the event is a "question" or "result" event and a manifest is present, deserialise the manifest and replace
    the serialised manifest with it.

    :param dict event: an Octue service event
    :return None:
    """
    manifest_keys = {"input_manifest", "output_manifest"}

    for key in manifest_keys:
        if key in event:
            event[key] = Manifest.deserialise(event[key])
            # Only one of the manifest types will be in the event, so return if one is found.
            return


def _unflatten_events(events):
    """Convert the events and attributes from the flat structure of the BigQuery table into the nested structure of the
    service communication schema.

    :param list(dict) events: flattened events
    :return list(dict): unflattened events
    """
    for event in events:
        event["event"]["kind"] = event.pop("kind")

        event["attributes"] = {
            "datetime": event.pop("datetime").isoformat(),
            "uuid": event.pop("uuid"),
            "originator": event.pop("originator"),
            "sender": event.pop("sender"),
            "sender_type": event.pop("sender_type"),
            "sender_sdk_version": event.pop("sender_sdk_version"),
            "recipient": event.pop("recipient"),
            "order": event.pop("order"),
            **event.pop("other_attributes"),
        }

    return events
