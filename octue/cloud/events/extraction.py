from octue.utils.objects import get_nested_attribute


def extract_and_convert_attributes(container):
    """Extract a Twined service event's attributes and convert them to the expected form.

    :param dict|google.cloud.pubsub_v1.subscriber.message.Message container: the event container in dictionary format or direct Google Pub/Sub format
    :return dict: the extracted and converted attributes
    """
    # Cast attributes to a dictionary to avoid defaultdict-like behaviour from Pub/Sub message attributes container.
    attributes = dict(get_nested_attribute(container, "attributes"))

    # Deserialise the `parent_question_uuid`, `forward_logs`, and `retry_count`, fields if they're present
    # (don't assume they are before validation).
    if attributes.get("parent_question_uuid") == "null":
        attributes["parent_question_uuid"] = None

    retry_count = attributes.get("retry_count")

    if retry_count:
        attributes["retry_count"] = int(retry_count)
    else:
        attributes["retry_count"] = None

    # Question events have some extra optional attributes.
    if attributes.get("sender_type") == "PARENT":
        forward_logs = attributes.get("forward_logs")

        if forward_logs:
            attributes["forward_logs"] = bool(int(forward_logs))
        else:
            attributes["forward_logs"] = None

        cpus = attributes.get("cpus")

        if cpus:
            attributes["cpus"] = int(cpus)

    return attributes
