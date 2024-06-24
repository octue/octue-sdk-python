import json

import pandas as pd
from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter

from octue.cloud.events.validation import VALID_EVENT_KINDS
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


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
    tail=True,
    limit=1000,
):
    """Get Octue service events for a question from a Google BigQuery event store. Exactly one of the question UUID,
    parent question UUID, or originator question UUID must be provided:

    - When a question UUID is specified, only events from that question are retrieved
    - When a parent question UUID is specified, events from questions triggered by the same parent question are retrieved, not including the parent question's events
    - When an originator question UUID is specified, events for the entire tree of questions triggered by the originator question are retrieved, including the originator question's events

    When the limit is smaller than the total number of events, the default behaviour is to return the "tail" of the
    event stream for the question (the most recent n events for the question).

    :param str table_id: the full ID of the Google BigQuery table e.g. "your-project.your-dataset.your-table"
    :param str|None question_uuid: the UUID of a question to get events for
    :param str|None parent_question_uuid: the UUID of a parent question to get the sub-question events for
    :param str|None originator_question_uuid: the UUID of an originator question get the full tree of events for
    :param str|None kind: the kind of event to get; if `None`, all event kinds are returned
    :param bool include_backend_metadata: if `True`, include the service backend metadata
    :param bool tail: if `True`, return the most recent events (where a limit applies); e.g. return the most recent 100 log records
    :param int limit: the maximum number of events to return
    :raise ValueError: if no events are found
    :return list(dict): the events for the question
    """
    _validate_inputs(question_uuid, parent_question_uuid, originator_question_uuid, kind)

    if question_uuid:
        question_uuid_condition = "WHERE question_uuid=@relevant_question_uuid"
    elif parent_question_uuid:
        question_uuid_condition = "WHERE parent_question_uuid=@relevant_question_uuid"
    elif originator_question_uuid:
        question_uuid_condition = "WHERE originator_question_uuid=@relevant_question_uuid"

    if kind:
        event_kind_condition = [f"AND kind={kind!r}"]
    else:
        event_kind_condition = []

    # Make a shallow copy of the fields to query.
    fields = list(DEFAULT_FIELDS)

    if include_backend_metadata:
        fields.extend(BACKEND_METADATA_FIELDS)

    base_query = "\n".join(
        [
            f"SELECT {', '.join(fields)} FROM `{table_id}`",
            question_uuid_condition,
            *event_kind_condition,
        ]
    )

    if tail:
        # Order the inner query to get the most recent events at the top, then reorder the outer query to get the events
        # in natural order.
        query = "\n".join(
            [
                "SELECT * FROM (",
                base_query,
                "ORDER BY `datetime` DESC",
                "LIMIT @limit",
                ") ORDER BY `datetime` ASC",
            ]
        )
    else:
        query = "\n".join([base_query, "ORDER BY `datetime` ASC", "LIMIT @limit"])

    relevant_question_uuid = question_uuid or parent_question_uuid or originator_question_uuid

    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("relevant_question_uuid", "STRING", relevant_question_uuid),
            ScalarQueryParameter("limit", "INTEGER", limit),
        ]
    )

    client = Client()
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    if result.total_rows == 0:
        raise ValueError(
            f"No events found for question {relevant_question_uuid!r}. Try loosening the query parameters and/or check "
            f"back later."
        )

    df = result.to_dataframe()
    df = df.apply(_deserialise_row, axis=1)

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


def _deserialise_row(row):
    """Deserialise a row from the event store:

    - Convert "null" to `None` in the `parent_question_uuid` field
    - Convert string-cast booleans and integers to `bool` and `int` types

    :param dict row: a row from the event store
    :return None:
    """
    if row["parent_question_uuid"] == "null":
        row["parent_question_uuid"] = None

    # Convert string-cast attributes back to `bool` or `int`.
    row["order"] = int(row["order"])

    other_attributes = row["other_attributes"]
    other_attributes["retry_count"] = int(other_attributes.pop("retry_count"))

    if other_attributes.get("forward_logs"):
        other_attributes["forward_logs"] = bool(int(other_attributes.pop("forward_logs")))

    # Use JSON serialisation round trip to convert all nested numpy types to python primitives.
    row = pd.Series(json.loads(json.dumps(row.to_dict(), cls=OctueJSONEncoder), cls=OctueJSONDecoder))
    return row


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
