import json

from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter


VALID_EVENT_KINDS = {"delivery_acknowledgement", "heartbeat", "log_record", "monitor_message", "exception", "result"}


def get_events(table_id, question_uuid, kind=None, limit=1000, include_pub_sub_metadata=False):
    """Get Octue service events for a question from a Google BigQuery table.

    :param str table_id: the full ID of the table e.g. "your-project.your-dataset.your-table"
    :param str question_uuid: the UUID of the question to get the events for
    :param str|None kind: the kind of event to get; if `None`, all event kinds are returned
    :param int limit: the maximum number of events to return
    :param bool include_pub_sub_metadata: if `True`, include Pub/Sub metadata
    :return list(dict): the events for the question
    """
    if kind:
        if kind not in VALID_EVENT_KINDS:
            raise ValueError(f"`kind` must be one of {VALID_EVENT_KINDS!r}; received {kind!r}.")

        event_kind_condition = [f'AND JSON_EXTRACT_SCALAR(data, "$.kind") = "{kind}"']
    else:
        event_kind_condition = []

    client = Client()
    fields = ["data", "attributes"]

    if include_pub_sub_metadata:
        fields.extend(("subscription_name", "message_id", "publish_time"))

    query = "\n".join(
        [
            f"SELECT {', '.join(fields)} FROM `{table_id}`",
            "WHERE CONTAINS_SUBSTR(subscription_name, @question_uuid)",
            *event_kind_condition,
            "ORDER BY publish_time",
            "LIMIT @limit",
        ]
    )

    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("question_uuid", "STRING", question_uuid),
            ScalarQueryParameter("limit", "INTEGER", limit),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    rows = query_job.result()
    messages = rows.to_dataframe()

    # Convert JSON to python primitives.
    if isinstance(messages.at[0, "data"], str):
        messages["data"] = messages["data"].map(json.loads)

    if isinstance(messages.at[0, "attributes"], str):
        messages["attributes"] = messages["attributes"].map(json.loads)

    # Order messages by the message number.
    messages = messages.iloc[messages["attributes"].str.get("message_number").astype(str).argsort()]
    messages.rename(columns={"data": "event"}, inplace=True)
    return messages.to_dict(orient="records")
