import logging
import os

import yaml


logger = logging.getLogger(__name__)


def load_service_and_app_configuration(service_configuration_path):
    """Load the service configuration from the given YAML file or return an empty one.

    :param str service_configuration_path: path to deployment configuration file
    :return dict:
    """
    try:
        with open(service_configuration_path) as f:
            service_configuration = yaml.load(f, Loader=yaml.SafeLoader)

        logger.info("Service configuration loaded from %r.", os.path.abspath(service_configuration_path))

    except FileNotFoundError:
        service_configuration = {}
        logger.info("Default service configuration used.")

    app_configuration = {}

    if service_configuration.get("app_configuration"):
        try:
            with open(service_configuration["app_configuration"]) as f:
                app_configuration = yaml.load(f, Loader=yaml.SafeLoader)

        except FileNotFoundError:
            pass

    if not app_configuration:
        logger.info("No app configuration found.")

    return service_configuration, app_configuration
