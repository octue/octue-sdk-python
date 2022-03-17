import json
import logging
import os

import yaml


logger = logging.getLogger(__name__)


class ServiceConfiguration:
    def __init__(self, name, app_source_path=".", twine_path="twine.json", app_configuration_path=None):
        self.name = name
        self.app_source_path = app_source_path
        self.twine_path = twine_path
        self.app_configuration_path = app_configuration_path

    @classmethod
    def from_file(cls, path):
        with open(path) as f:
            raw_service_configuration = yaml.load(f, Loader=yaml.SafeLoader)

        logger.info("Service configuration loaded from %r.", os.path.abspath(path))
        return cls(**raw_service_configuration)


class AppConfiguration:
    def __init__(
        self,
        configuration_values=None,
        configuration_manifest=None,
        output_manifest_path=None,
        children=None,
    ):
        self.configuration_values = configuration_values
        self.configuration_manifest = configuration_manifest
        self.output_manifest_path = output_manifest_path
        self.children = children

    @classmethod
    def from_file(cls, path):
        with open(path) as f:
            raw_app_configuration = json.load(f)

        logger.info("Service configuration loaded from %r.", os.path.abspath(path))
        return cls(**raw_app_configuration)


def load_service_and_app_configuration(service_configuration_path):
    """Load the service configuration from the given YAML file or return an empty one.

    :param str service_configuration_path: path to service configuration file
    :return dict:
    """
    service_configuration = ServiceConfiguration.from_file(service_configuration_path)
    app_configuration = AppConfiguration()

    if service_configuration.app_configuration_path:
        try:
            app_configuration = AppConfiguration.from_file(service_configuration.app_configuration_path)
        except FileNotFoundError:
            logger.info("No app configuration found.")

    return service_configuration, app_configuration
