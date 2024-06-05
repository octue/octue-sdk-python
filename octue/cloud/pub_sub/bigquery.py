from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter

from octue.cloud.events.validation import VALID_EVENT_KINDS
from octue.resources import Manifest


DEFAULT_FIELDS = (
    "`originator_question_uuid`",
    "`parent_question_uuid`",
    "`question_uuid`",
    "`kind`",
    "`event`",
    "`datetime`",
    "`uuid`",
    "`originator`",
    "`parent`",
    "`sender`",
    "`sender_type`",
    "`sender_sdk_version`",
    "`recipient`",
    "`order`",
    "`other_attributes`",
)

BACKEND_METADATA_FIELDS = ("`backend`", "`backend_metadata`")


def get_events(
    table_id,
    question_uuid=None,
    parent_question_uuid=None,
    originator_question_uuid=None,
    kind=None,
    include_backend_metadata=False,
    limit=1000,
):
    """Get Octue service events for a question from a Google BigQuery event store. Exactly one of the question UUID,
    parent question UUID, or originator question UUID must be provided:
    - When a question UUID is specified, only events from that question are retrieved
    - When a parent question UUID is specified, events from questions triggered by the same parent question are retrieved, not including the parent question's events
    - When an originator question UUID is specified, events for the entire tree of questions triggered by the originator question are retrieved, including the originator question's events

    :param str table_id: the full ID of the Google BigQuery table e.g. "your-project.your-dataset.your-table"
    :param str|None question_uuid: the UUID of a question to get events for
    :param str|None parent_question_uuid: the UUID of a parent question to get the sub-question events for
    :param str|None originator_question_uuid: the UUID of an originator question get the full tree of events for
    :param str|None kind: the kind of event to get; if `None`, all event kinds are returned
    :param bool include_backend_metadata: if `True`, include the service backend metadata
    :param int limit: the maximum number of events to return
    :raise ValueError: if no events are found
    :return list(dict): the events for the question
    """
    _validate_inputs(question_uuid, parent_question_uuid, originator_question_uuid, kind)

    if question_uuid:
        question_uuid_condition = "WHERE question_uuid=@question_uuid"
    elif parent_question_uuid:
        question_uuid_condition = "WHERE parent_question_uuid=@question_uuid"
    elif originator_question_uuid:
        question_uuid_condition = "WHERE originator_question_uuid=@question_uuid"

    if kind:
        event_kind_condition = [f"AND kind={kind!r}"]
    else:
        event_kind_condition = []

    # Make a shallow copy of the fields to query.
    fields = list(DEFAULT_FIELDS)

    if include_backend_metadata:
        fields.extend(BACKEND_METADATA_FIELDS)

    query = "\n".join(
        [
            f"SELECT {', '.join(fields)} FROM `{table_id}`",
            question_uuid_condition,
            *event_kind_condition,
            "ORDER BY `datetime`",
            "LIMIT @limit",
        ]
    )

    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("question_uuid", "STRING", question_uuid),
            ScalarQueryParameter("limit", "INTEGER", limit),
        ]
    )

    client = Client()
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    if result.total_rows == 0:
        raise ValueError(f"No events found for question {question_uuid!r}. Check back later.")

    df = result.to_dataframe()
    df["event"].apply(_deserialise_manifest_if_present)

    events = df.to_dict(orient="records")
    return _unflatten_events(events)


def _validate_inputs(question_uuid, parent_question_uuid, originator_question_uuid, kind):
    """Check that only one of `question_uuid`, `parent_question_uuid`, or `originator_question_uuid` are provided and
    that the `kind` parameter is a valid event kind.

    :param str|None question_uuid: the UUID of a question to get events for
    :param str|None parent_question_uuid: the UUID of a parent question to get the sub-question events for
    :param str|None originator_question_uuid: the UUID of an originator question get the full tree of events for
    :param str|None kind: the kind of event to get; if `None`, all event kinds are returned
    :raise ValueError: if more than one of `question_uuid`, `parent_question_uuid`, or `originator_question_uuid` are provided or the `kind` parameter is invalid
    :return None:
    """
    question_uuid_inputs = (bool(question_uuid), bool(parent_question_uuid), bool(originator_question_uuid))

    if sum(question_uuid_inputs) != 1:
        raise ValueError(
            "One and only one of `question_uuid`, `parent_question_uuid`, or `originator_question_uuid` must be "
            "provided."
        )

    if kind and kind not in VALID_EVENT_KINDS:
        raise ValueError(f"`kind` must be one of {VALID_EVENT_KINDS!r}; received {kind!r}.")


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
            "originator_question_uuid": event.pop("originator_question_uuid"),
            "parent_question_uuid": event.pop("parent_question_uuid"),
            "question_uuid": event.pop("question_uuid"),
            "datetime": event.pop("datetime").isoformat(),
            "uuid": event.pop("uuid"),
            "originator": event.pop("originator"),
            "parent": event.pop("parent"),
            "sender": event.pop("sender"),
            "sender_type": event.pop("sender_type"),
            "sender_sdk_version": event.pop("sender_sdk_version"),
            "recipient": event.pop("recipient"),
            "order": event.pop("order"),
            **event.pop("other_attributes"),
        }

    return events
