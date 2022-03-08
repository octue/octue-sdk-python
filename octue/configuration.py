import logging
import os

import yaml


logger = logging.getLogger(__name__)


def load_service_and_app_configuration(service_configuration_path):
    """Load the service configuration from the given YAML file or return an empty one.

    :param str service_configuration_path: path to service configuration file
    :return dict:
    """
    raw_service_configuration = {}

    try:
        with open(service_configuration_path) as f:
            raw_service_configuration = yaml.load(f, Loader=yaml.SafeLoader)

        logger.info("Service configuration loaded from %r.", os.path.abspath(service_configuration_path))

    except FileNotFoundError:
        logger.info("Default service configuration used.")

    service_configuration = {
        "name": raw_service_configuration["name"],
        "app_source_path": raw_service_configuration.get("app_source_path", "."),
        "twine_path": raw_service_configuration.get("twine_path", "twine.json"),
        "app_configuration": raw_service_configuration.get("app_configuration"),
    }

    if service_configuration.get("app_configuration"):
        app_configuration = load_app_configuration(service_configuration["app_configuration"])
    else:
        app_configuration = {}

    return service_configuration, app_configuration


def load_app_configuration(app_configuration_path):
    """Load the app configuration from the given YAML file or return an empty one.

    :param str app_configuration_path: path to app configuration file
    :return dict:
    """
    raw_app_configuration = {}

    try:
        with open(app_configuration_path) as f:
            raw_app_configuration = yaml.load(f, Loader=yaml.SafeLoader)

    except FileNotFoundError:
        pass

    if not raw_app_configuration:
        logger.info("No app configuration found.")

    return {
        "configuration_values": raw_app_configuration.get("configuration_values"),
        "configuration_manifest": raw_app_configuration.get("configuration_manifest"),
        "output_manifest_path": raw_app_configuration.get("output_manifest"),
        "children": raw_app_configuration.get("children"),
    }
