import google.auth
import google.oauth2
import requests

from octue.cloud.service_id import create_sruid, logger, split_service_id
import octue.exceptions


def get_default_sruid(namespace, name, service_registries):
    """Get the SRUID of the default revision of the service `<namespace>/<name>` if it exists in one of the specified
    service registries. The registries should be provided in priority order so that, if more than one registry contains
    a matching service, the revision that's returned is taken from the highest priority (first) registry.

    :param str namespace: the namespace of the service
    :param str name: the name of the service
    :param iter(dict) service_registries: the registries to look for the service in; the registries should be in priority order in case more than one has a service with the given namespace and name
    :raise octue.exceptions.ServiceNotFound: if a revision can't be found for the service in the service registries
    :return str: the SRUID of the default revision of the service
    """
    service_id = f"{namespace}/{name}"

    for registry in service_registries:
        response = _make_service_registry_request(registry, namespace, name)

        if response.status_code == 200:
            revision_tag = response.json()["revision_tag"]

            logger.info(
                "Found default service revision '%s:%s' in %r registry.",
                service_id,
                revision_tag,
                registry["name"],
            )

            return create_sruid(namespace=namespace, name=name, revision_tag=revision_tag)

    raise octue.exceptions.ServiceNotFound(
        f"No revisions for the service {service_id!r} were found in any of the specified service registries: "
        f"{service_registries!r}"
    )


def raise_if_revision_not_registered(sruid, service_registries):
    """Raise an error if the service revision isn't registered in the given service registries.

    :param str sruid: the SRUID of the service revision
    :param iter(dict) service_registries: the registries to look for the service revision in
    :raise octue.exceptions.ServiceNotFound: if the service revision isn't registered in any of the service registries
    :return None:
    """
    namespace, name, revision_tag = split_service_id(sruid, require_revision_tag=True)

    for registry in service_registries:
        response = _make_service_registry_request(registry, namespace, name, revision_tag)

        if response.status_code == 200:
            logger.info("Found service revision %r in %r registry.", sruid, registry["name"])
            return

    raise octue.exceptions.ServiceNotFound(
        f"Service revision {sruid!r} was not found in any of the specified service registries: {service_registries!r}"
    )


def _make_service_registry_request(registry, namespace, name, revision_tag=None):
    """Make an authenticated request to a service registry about a service.

    :param dict registry: a dictionary with the keys "endpoint" and "name"
    :param str namespace: the namespace of the service
    :param str name: the name of the service
    :param str|None revision_tag: the revision tag for a revision of the service
    :raise requests.exceptions.HTTPError: if the request fails with a status code other than 404
    :return requests.Response: the response from the service registry
    """
    id_token = _get_google_cloud_id_token(registry)

    response = requests.get(
        f"{registry['endpoint']}/{namespace}/{name}",
        params={"revision_tag": revision_tag},
        headers={"Authorization": f"Bearer {id_token}"},
    )

    if response.status_code != 404:
        response.raise_for_status()

    return response


def _get_google_cloud_id_token(registry):
    """Get an ID token for Google Cloud.

    :param dict registry: a dictionary with the keys "endpoint" and "name"
    :return str: an ID token for Google Cloud
    """
    return google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), registry["endpoint"])
