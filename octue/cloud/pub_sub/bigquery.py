import json

from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter

from octue.cloud.events.validation import VALID_EVENT_KINDS


def get_events(
    table_id,
    sender,
    question_uuid,
    kind=None,
    include_attributes=False,
    include_backend_metadata=False,
    limit=1000,
):
    """Get Octue service events for a question from a sender from a Google BigQuery event store.

    :param str table_id: the full ID of the table e.g. "your-project.your-dataset.your-table"
    :param str sender: the SRUID of the sender of the events
    :param str question_uuid: the UUID of the question to get the events for
    :param str|None kind: the kind of event to get; if `None`, all event kinds are returned
    :param bool include_attributes: if `True`, include events' attributes (excluding question UUID)
    :param bool include_backend_metadata: if `True`, include the service backend metadata
    :param int limit: the maximum number of events to return
    :return list(dict): the events for the question
    """
    if kind:
        if kind not in VALID_EVENT_KINDS:
            raise ValueError(f"`kind` must be one of {VALID_EVENT_KINDS!r}; received {kind!r}.")

        event_kind_condition = [f'AND JSON_EXTRACT_SCALAR(event, "$.kind") = "{kind}"']
    else:
        event_kind_condition = []

    client = Client()
    fields = ["`event`"]

    if include_attributes:
        fields.extend(
            (
                "`datetime`",
                "`uuid`",
                "`originator`",
                "`sender`",
                "`sender_type`",
                "`sender_sdk_version`",
                "`recipient`",
                "`order`",
                "`other_attributes`",
            )
        )

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
    rows = query_job.result()
    df = rows.to_dataframe()

    # Convert JSON strings to python primitives.
    df["event"] = df["event"].map(json.loads)

    if "other_attributes" in df:
        df["other_attributes"] = df["other_attributes"].map(json.loads)

    if "backend_metadata" in df:
        df["backend_metadata"] = df["backend_metadata"].map(json.loads)

    return df.to_dict(orient="records")
