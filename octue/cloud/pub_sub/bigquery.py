from google.cloud import bigquery


def get_events(table_id, question_uuid, limit=1000):
    """Get Octue service events for a question from a Google BigQuery table.

    :param str table_id: the full ID of the table e.g. "your-project.your-dataset.your-table"
    :param str question_uuid: the UUID of the question to get the events for
    :param int limit: the maximum number of events to return.
    :return list(dict): the events for the question
    """
    client = bigquery.Client()

    query = f"""
    SELECT * FROM `{table_id}`
    WHERE  CONTAINS_SUBSTR(subscription_name, @question_uuid)
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
