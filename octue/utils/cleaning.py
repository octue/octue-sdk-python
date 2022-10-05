def clean_service_id(service_id):
    """Replace forward slashes and colons with dots in the service ID and, if a service revision is included in the
    service ID, replace any dots in it with dashes.

    :param str service_id: the raw service ID
    :return str: the cleaned service ID.
    """
    if ":" in service_id:
        service_id, service_revision = service_id.split(":")
    else:
        service_revision = None

    service_id = service_id.replace("/", ".")

    if service_revision:
        service_id = service_id + "." + service_revision.replace(".", "-")

    return service_id
