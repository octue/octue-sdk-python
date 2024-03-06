from google.cloud import bigquery


VALID_EVENT_KINDS = {"delivery_acknowledgement", "heartbeat", "log_record", "monitor_message", "exception", "result"}


def get_events(
    table_id,
    question_uuid,
    kind=None,
    limit=1000,
    include_attributes=False,
    include_pub_sub_metadata=False,
):
    """Get Octue service events for a question from a Google BigQuery table.

    :param str table_id: the full ID of the table e.g. "your-project.your-dataset.your-table"
    :param str question_uuid: the UUID of the question to get the events for
    :param str|None kind: the kind of event to get; if `None`, all event kinds are returned
    :param int limit: the maximum number of events to return
    :param bool include_attributes: if `True`, include the event attributes
    :param bool include_pub_sub_metadata: if `True`, include Pub/Sub metadata
    :return list(dict): the events for the question
    """
    if kind:
        if kind not in VALID_EVENT_KINDS:
            raise ValueError(f"`kind` must be one of {VALID_EVENT_KINDS!r}; received {kind!r}.")

        kind_condition = f'AND JSON_EXTRACT_SCALAR(data, "$.kind") = "{kind}"'
    else:
        kind_condition = ""

    client = bigquery.Client()
    fields = ["data"]

    if include_attributes:
        fields.append("attributes")

    if include_pub_sub_metadata:
        fields.extend(("subscription_name", "message_id", "publish_time"))

    query = f"""
    SELECT {", ".join(fields)} FROM `{table_id}`
    WHERE  CONTAINS_SUBSTR(subscription_name, @question_uuid)
    {kind_condition}
    ORDER BY `publish_time`
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("question_uuid", "STRING", question_uuid),
            bigquery.ScalarQueryParameter("limit", "INTEGER", limit),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    rows = query_job.result()
    messages = rows.to_dataframe()

    return messages.to_dict(orient="records")
